// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"bufio"
	"github.com/bitly/go-nsq"
	"github.com/ikkeps/lz4"
	"github.com/propellerads/nsq/util"
	"github.com/propellerads/nsq/util/lookupd"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	channel     = flag.String("channel", "nsq_to_file", "nsq channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	outputDir      = flag.String("output-dir", "/tmp", "directory to write output files to")
	tmpAndRename   = flag.Bool("tmp-and-rename", false, "When set, files will be written to <output-dir>/tmp and moved on rotate to <output-dir>. If file with same name exists, \".<number>\" suffix will be added.")
	writeDir       string // dir to write files.
	datetimeFormat = flag.String("datetime-format", "%Y-%m-%d_%H", "strftime compatible format for <DATETIME> in filename format")
	filenameFormat = flag.String("filename-format", "<TOPIC>.<HOST><REV>.<DATETIME>.log", "output filename format (<TOPIC>, <HOST>, <PID>, <DATETIME>, <REV> are replaced. <REV> is increased when file already exists)")
	hostIdentifier = flag.String("host-identifier", "", "value to output in log filename in place of hostname. <SHORT_HOST> and <HOSTNAME> are valid replacement tokens")
	compressLevel  = flag.Int("compress-level", 6, "compression level for gzip and lz4 (1-9, 1=BestSpeed, 9=BestCompression). For lz4, 9 is best compression, 1-8 is best speed.")
	gzipEnabled    = flag.Bool("gzip", false, "gzip output files.")
	lz4Enabled     = flag.Bool("lz4", false, "use lz4 to compress output files.")
	skipEmptyFiles = flag.Bool("skip-empty-files", false, "Skip writting empty files")
	topicPollRate  = flag.Duration("topic-refresh", time.Minute, "how frequently the topic list should be refreshed")
	topicPattern   = flag.String("topic-pattern", ".*", "Only log topics matching the following pattern")

	rotateSize     = flag.Int64("rotate-size", 0, "rotate the file when it grows bigger than `rotate-size` bytes")
	rotateInterval = flag.Duration("rotate-interval", 0*time.Second, "rotate the file every duration")

	consumerOpts     = util.StringArray{}
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
	topics           = util.StringArray{}
)

type CompressMethod int

const (
	NO_COMPRESS CompressMethod = 0
	GZIP                       = iota
	LZ4                        = iota

	LZ4_BUF_SIZE = 65536
)

func init() {
	// TODO: remove, deprecated
	flag.Var(&consumerOpts, "reader-opt", "(deprecated) use --consumer-opt")
	flag.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/bitly/go-nsq#Config)")

	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&topics, "topic", "nsq topic (may be given multiple times)")
}

type FileLogger struct {
	out            *os.File
	writer         io.Writer
	compressWriter io.WriteCloser
	logChan        chan *nsq.Message
	compressLevel  int
	compressMethod CompressMethod
	filenameFormat string

	ExitChan chan int
	termChan chan bool
	hupChan  chan bool

	// for rotation
	lastFilename string
	lastOpenTime time.Time
	filesize     int64
	rev          uint
}

type ConsumerFileLogger struct {
	F *FileLogger
	C *nsq.Consumer
}

type TopicDiscoverer struct {
	topics   map[string]*ConsumerFileLogger
	termChan chan os.Signal
	hupChan  chan os.Signal
	wg       sync.WaitGroup
}

func newTopicDiscoverer() *TopicDiscoverer {
	return &TopicDiscoverer{
		topics:   make(map[string]*ConsumerFileLogger),
		termChan: make(chan os.Signal),
		hupChan:  make(chan os.Signal),
	}
}

func (l *FileLogger) HandleMessage(m *nsq.Message) error {
	m.DisableAutoResponse()
	l.logChan <- m
	return nil
}

func (f *FileLogger) router(r *nsq.Consumer) {
	pos := 0
	output := make([]*nsq.Message, *maxInFlight)
	sync := false
	ticker := time.NewTicker(time.Duration(30) * time.Second)
	closing := false
	closeFile := false
	exit := false

	for {
		select {
		case <-r.StopChan:
			sync = true
			closeFile = true
			exit = true
		case <-f.termChan:
			ticker.Stop()
			r.Stop()
			sync = true
			closing = true
		case <-f.hupChan:
			sync = true
			closeFile = true
		case <-ticker.C:
			if f.needsFileRotate() {
				if *skipEmptyFiles {
					closeFile = true
				} else {
					f.updateFile()
				}
			}
			sync = true
		case m := <-f.logChan:
			if f.needsFileRotate() {
				f.updateFile()
				sync = true
			}
			_, err := f.writer.Write(m.Body)
			if err != nil {
				log.Fatalf("ERROR: writing message to disk - %s", err)
			}
			_, err = f.writer.Write([]byte("\n"))
			if err != nil {
				log.Fatalf("ERROR: writing newline to disk - %s", err)
			}
			output[pos] = m
			pos++
			if pos == cap(output) {
				sync = true
			}
		}

		if closing || sync || r.IsStarved() {
			if pos > 0 {
				log.Printf("syncing %d records to disk", pos)
				err := f.Sync()
				if err != nil {
					log.Fatalf("ERROR: failed syncing messages - %s", err)
				}
				for pos > 0 {
					pos--
					m := output[pos]
					m.Finish()
					output[pos] = nil
				}
			}
			sync = false
		}

		if closeFile {
			f.Close()
			closeFile = false
		}
		if exit {
			close(f.ExitChan)
			break
		}
	}
}

func (f *FileLogger) Close() error {
	if f.out != nil {
		if f.compressWriter != nil {
			f.compressWriter.Close()
		}
		err := f.out.Sync()
		if err != nil {
			return err
		}
		filename := f.out.Name()
		err = f.out.Close()
		if err != nil {
			return err
		}
		f.out = nil
		if *tmpAndRename {
			moveFileToOutputDir(filename)
		}
	}
	return nil
}

func moveFileToOutputDir(srcFilename string) {
	relative, err := filepath.Rel(writeDir, srcFilename)
	if err != nil {
		log.Fatalf("Could not determine full destination path to file (looks like this is internal error): %v", err)
	}
	destFilename := filepath.Join(*outputDir, relative)

	// Check that we will not overwrite existing file
	// XXX: There is tiny race here if someone creates file with same name befor os.Rename
	// XXX: doing this really atomically is kinda complicated (wrapping another syscall)
	if _, err := os.Stat(destFilename); !os.IsNotExist(err) {
		suffix := 0
		for {
			checkDst := fmt.Sprintf("%s.%v", destFilename, suffix)
			if _, err := os.Stat(checkDst); os.IsNotExist(err) {
				destFilename = checkDst
				break
			}
			suffix++
		}
	}
	log.Printf("INFO: renaming %s -> %s", srcFilename, destFilename)
	destDir := filepath.Dir(destFilename)
	err = os.MkdirAll(destDir, 0770)
	if err != nil {
		log.Fatalf("Could not create directories %s: %v", err)
	}
	err = os.Rename(srcFilename, destFilename)
	if err != nil {
		log.Fatalf("Could not rename %s -> %s: %v", srcFilename, destFilename, err)
	}
}

func moveAllFilesFromTempDir() {
	err := filepath.Walk(writeDir, moveToOutputWalkFunc)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		log.Fatalf("Could not move files from temp folder: %v", err)
	}
}

func moveToOutputWalkFunc(name string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if !info.IsDir() {
		moveFileToOutputDir(name)
	}
	return nil
}

func (f *FileLogger) Write(p []byte) (n int, err error) {
	f.filesize += int64(len(p))
	return f.out.Write(p)
}

func (f *FileLogger) Sync() error {
	if f.compressWriter != nil {
		f.compressWriter.Close()
		f.compressWriter = f.newCompressWriter()
		f.writer = f.compressWriter
	}
	return f.out.Sync()
}

func (f *FileLogger) newCompressWriter() io.WriteCloser {
	switch f.compressMethod {
	case LZ4:
		return newLz4Writer(f, f.compressLevel)
	case GZIP:
		cw, _ := gzip.NewWriterLevel(f, f.compressLevel)
		return cw
	}
	return f
}

func (f *FileLogger) calculateCurrentFilename() string {
	t := time.Now()
	datetime := strftime(*datetimeFormat, t)
	return strings.Replace(f.filenameFormat, "<DATETIME>", datetime, -1)
}

func (f *FileLogger) needsFileRotate() bool {
	if f.out == nil {
		return true
	}

	filename := f.calculateCurrentFilename()
	if filename != f.lastFilename {
		log.Printf("INFO: new filename %s, need rotate", filename)
		return true // rotate by filename
	}

	if *rotateInterval > 0 {
		if s := time.Since(f.lastOpenTime); s > *rotateInterval {
			log.Printf("INFO: %s since last open, need rotate", s)
			return true // rotate by interval
		}
	}

	if *rotateSize > 0 && f.filesize > *rotateSize {
		log.Printf("INFO: %s current %d bytes, need rotate", f.out.Name(), f.filesize)
		return true // rotate by size
	}
	return false
}

func (f *FileLogger) updateFile() {
	filename := f.calculateCurrentFilename()
	if filename != f.lastFilename {
		f.rev = 0 // reset revsion to 0 if it is a new filename
	} else {
		f.rev += 1
	}
	f.lastFilename = filename
	f.lastOpenTime = time.Now()

	fullPath := path.Join(writeDir, filename)
	dir, _ := filepath.Split(fullPath)
	if dir != "" {
		err := os.MkdirAll(dir, 0770)
		if err != nil {
			log.Fatalf("ERROR: %s Unable to create %s", err, dir)
		}
	}

	f.Close()

	var err error
	var fi os.FileInfo
	for ; ; f.rev += 1 {
		absFilename := strings.Replace(fullPath, "<REV>", fmt.Sprintf("-%06d", f.rev), -1)
		openFlag := os.O_WRONLY | os.O_CREATE
		if f.compressMethod != NO_COMPRESS {
			openFlag |= os.O_EXCL
		} else {
			openFlag |= os.O_APPEND
		}
		f.out, err = os.OpenFile(absFilename, openFlag, 0666)
		if err != nil {
			if os.IsExist(err) {
				log.Printf("INFO: file already exists: %s", absFilename)
				continue
			}
			log.Fatalf("ERROR: %s Unable to open %s", err, absFilename)
		}
		log.Printf("INFO: opening %s", absFilename)
		fi, err = f.out.Stat()
		if err != nil {
			log.Fatalf("ERROR: %s Unable to stat file %s", err, f.out.Name())
		}
		f.filesize = fi.Size()
		if f.filesize == 0 {
			break // ok, new file
		}
		if f.needsFileRotate() {
			continue // next rev
		}
		break // ok, don't need rotate
	}
	if f.compressMethod != NO_COMPRESS {
		f.compressWriter = f.newCompressWriter()
		f.writer = f.compressWriter
	} else {
		f.writer = f
	}
}

func NewFileLogger(gzipEnabled, lz4Enabled bool, compressionLevel int, filenameFormat, topic string) (*FileLogger, error) {
	// TODO: remove, deprecated, for compat <GZIPREV>
	filenameFormat = strings.Replace(filenameFormat, "<GZIPREV>", "<REV>", -1)
	if gzipEnabled || lz4Enabled || *rotateSize > 0 || *rotateInterval > 0 {
		if strings.Index(filenameFormat, "<REV>") == -1 {
			return nil, errors.New("missing <REV> in --filename-format when gzip or rotation enabled")
		}
	} else { // remove <REV> as we don't need it
		filenameFormat = strings.Replace(filenameFormat, "<REV>", "", -1)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	shortHostname := strings.Split(hostname, ".")[0]
	identifier := shortHostname
	if len(*hostIdentifier) != 0 {
		identifier = strings.Replace(*hostIdentifier, "<SHORT_HOST>", shortHostname, -1)
		identifier = strings.Replace(identifier, "<HOSTNAME>", hostname, -1)
	}
	filenameFormat = strings.Replace(filenameFormat, "<TOPIC>", topic, -1)
	filenameFormat = strings.Replace(filenameFormat, "<HOST>", identifier, -1)
	filenameFormat = strings.Replace(filenameFormat, "<PID>", fmt.Sprintf("%d", os.Getpid()), -1)
	if gzipEnabled && !strings.HasSuffix(filenameFormat, ".gz") {
		filenameFormat = filenameFormat + ".gz"
	} else if lz4Enabled && !strings.HasSuffix(filenameFormat, ".lz4") {
		filenameFormat = filenameFormat + ".lz4"
	}
	compressMethod := NO_COMPRESS
	switch {
	case lz4Enabled:
		compressMethod = LZ4
	case gzipEnabled:
		compressMethod = GZIP

	}
	f := &FileLogger{
		logChan:        make(chan *nsq.Message, 1),
		compressLevel:  compressionLevel,
		filenameFormat: filenameFormat,
		compressMethod: compressMethod,
		ExitChan:       make(chan int),
		termChan:       make(chan bool),
		hupChan:        make(chan bool),
	}
	return f, nil
}

func hasArg(s string) bool {
	for _, arg := range os.Args {
		if strings.Contains(arg, s) {
			return true
		}
	}
	return false
}

func newConsumerFileLogger(topic string) (*ConsumerFileLogger, error) {
	f, err := NewFileLogger(*gzipEnabled, *lz4Enabled, *compressLevel, *filenameFormat, topic)
	if err != nil {
		return nil, err
	}

	cfg := nsq.NewConfig()
	cfg.UserAgent = fmt.Sprintf("nsq_to_file/%s go-nsq/%s", util.BINARY_VERSION, nsq.VERSION)
	err = util.ParseOpts(cfg, consumerOpts)
	if err != nil {
		return nil, err
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := nsq.NewConsumer(topic, *channel, cfg)
	if err != nil {
		return nil, err
	}

	consumer.AddHandler(f)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	return &ConsumerFileLogger{
		C: consumer,
		F: f,
	}, nil
}

func (t *TopicDiscoverer) startTopicRouter(logger *ConsumerFileLogger) {
	t.wg.Add(1)
	defer t.wg.Done()
	go logger.F.router(logger.C)
	<-logger.F.ExitChan
}

func (t *TopicDiscoverer) allowTopicName(pattern string, name string) bool {
	match, err := regexp.MatchString(pattern, name)
	if err != nil {
		return false
	}

	return match
}

func (t *TopicDiscoverer) syncTopics(addrs []string, pattern string) {
	newTopics, err := lookupd.GetLookupdTopics(addrs)
	if err != nil {
		log.Printf("ERROR: could not retrieve topic list: %s", err)
	}
	for _, topic := range newTopics {
		if _, ok := t.topics[topic]; !ok {
			if !t.allowTopicName(pattern, topic) {
				log.Println("Skipping topic ", topic, "as it didn't match required pattern:", pattern)
				continue
			}
			logger, err := newConsumerFileLogger(topic)
			if err != nil {
				log.Printf("ERROR: couldn't create logger for new topic %s: %s", topic, err)
				continue
			}
			t.topics[topic] = logger
			go t.startTopicRouter(logger)
		}
	}
}

func (t *TopicDiscoverer) stop() {
	for _, topic := range t.topics {
		topic.F.termChan <- true
	}
}

func (t *TopicDiscoverer) hup() {
	for _, topic := range t.topics {
		topic.F.hupChan <- true
	}
}

func (t *TopicDiscoverer) watch(addrs []string, sync bool, pattern string) {
	ticker := time.Tick(*topicPollRate)
	for {
		select {
		case <-ticker:
			if sync {
				t.syncTopics(addrs, pattern)
			}
		case <-t.termChan:
			t.stop()
			t.wg.Wait()
			return
		case <-t.hupChan:
			t.hup()
		}
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_file v%s\n", util.BINARY_VERSION)
		return
	}
	writeDir = *outputDir
	if *tmpAndRename {
		writeDir = filepath.Join(*outputDir, "/tmp")
	}

	if *channel == "" {
		log.Fatal("--channel is required")
	}

	var topicsFromNSQLookupd bool

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required.")
	}
	if len(nsqdTCPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if *lz4Enabled && *gzipEnabled {
		log.Fatalf("you shold use either --lz4 or --gzip, not both")
	}

	if *compressLevel < 1 || *compressLevel > 9 {
		log.Fatalf("invalid --compress-level value (%d), should be 1-9", *compressLevel)
	}

	if *tmpAndRename {
		moveAllFilesFromTempDir()
	}

	discoverer := newTopicDiscoverer()

	signal.Notify(discoverer.hupChan, syscall.SIGHUP)
	signal.Notify(discoverer.termChan, syscall.SIGINT, syscall.SIGTERM)

	if len(topics) < 1 {
		if len(lookupdHTTPAddrs) < 1 {
			log.Fatal("use --topic to list at least one topic to subscribe to or specify at least one --lookupd-http-address to subscribe to all its topics")
		}
		topicsFromNSQLookupd = true
		var err error
		topics, err = lookupd.GetLookupdTopics(lookupdHTTPAddrs)
		if err != nil {
			log.Fatalf("ERROR: could not retrieve topic list: %s", err)
		}
	}

	for _, topic := range topics {
		if !discoverer.allowTopicName(*topicPattern, topic) {
			log.Println("Skipping topic", topic, "as it didn't match required pattern:", *topicPattern)
			continue
		}

		logger, err := newConsumerFileLogger(topic)
		if err != nil {
			log.Fatalf("ERROR: couldn't create logger for topic %s: %s", topic, err)
		}
		discoverer.topics[topic] = logger
		go discoverer.startTopicRouter(logger)
	}

	discoverer.watch(lookupdHTTPAddrs, topicsFromNSQLookupd, *topicPattern)
}

// lz4Writer combines buffer and lz4 and have methods to properly flush everything
// Logic is bw -> lz4 -> original writer
type lz4Writer struct {
	bw   *bufio.Writer
	lz4w *lz4.Writer
}

func newLz4Writer(w io.Writer, compressLevel int) *lz4Writer {
	lz4w := lz4.NewWriter(w)
	lz4w.Header = lz4.Header{
		BlockChecksum:   false,
		BlockDependency: false,
		NoChecksum:      true,
		HighCompression: compressLevel >= 9,
	}
	return &lz4Writer{
		lz4w: lz4w,
		bw:   bufio.NewWriterSize(lz4w, LZ4_BUF_SIZE),
	}
}

func (l *lz4Writer) Write(p []byte) (int, error) {
	return l.bw.Write(p)
}

func (l *lz4Writer) Close() error {
	err := l.bw.Flush()
	if err != nil {
		return err
	}
	return l.lz4w.Close()
}
