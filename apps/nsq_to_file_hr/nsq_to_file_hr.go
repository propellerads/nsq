package main

import (
	"compress/gzip"
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
	"github.com/pierrec/lz4"
	"github.com/propellerads/nsq/util"
	"github.com/propellerads/nsq/util/lookupd"
)

const CSVSemicolon = ','

var (
	showVersion = flag.Bool("version", false, "print version string")

	channel = flag.String("channel", "nsq_to_file", "nsq channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	outputDir = flag.String("output-dir", "/tmp", "directory to write output files to")
	tmpAndRename = flag.Bool("tmp-and-rename", false, "When set, files will be written to <output-dir>/tmp and moved on rotate to <output-dir>. If file with same name exists, \".<number>\" suffix will be added.")

	filenameFormat = flag.String("filename-format", "<TOPIC>.<HOST><REV>.<DATETIME>.log", "output filename format (<TOPIC>, <HOST>, <PID>, <DATETIME>, <REV> are replaced. <REV> is increased when file already exists)")
	hostIdentifier = flag.String("host-identifier", "", "value to output in log filename in place of hostname. <SHORT_HOST> and <HOSTNAME> are valid replacement tokens")
	compressLevel = flag.Int("compress-level", 6, "compression level for gzip and lz4 (1-9, 1=BestSpeed, 9=BestCompression). For lz4, 9 is best compression, 1-8 is best speed.")
	gzipEnabled = flag.Bool("gzip", false, "gzip output files.")
	lz4Enabled = flag.Bool("lz4", false, "use lz4 to compress output files.")

	topicPollRate = flag.Duration("topic-refresh", time.Minute, "how frequently the topic list should be refreshed")
	topicPattern = flag.String("topic-pattern", ".*", "Only log topics matching the following pattern")

	//skipEmptyFiles = flag.Bool("skip-empty-files", false, "Skip writting empty files")
	//rotateSize 	= flag.Int64("rotate-size", 0, "rotate the file when it grows bigger than `rotate-size` bytes")
	//rotateInterval = flag.Duration("rotate-interval", 0 * time.Second, "rotate the file every duration")
	//rotateByHour 	= flag.Bool("rotate-by-hour", true, "use datetime in message for rotate by hour")

	consumerOpts = util.StringArray{}
	nsqdTCPAddrs = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
	topics = util.StringArray{}

	syncEvery = time.Duration(30) * time.Second
	closeEvery = time.Hour

	writeDir string // dir to write files.
)

type CompressMethod int

const (
	NO_COMPRESS CompressMethod = 0
	GZIP = iota
	LZ4 = iota

	LZ4_BUF_SIZE = 65536
)

func GetMessageHour(m *nsq.Message) string {
	date := make([]byte, 0, 17)
	var semCount int
	for i := range m.Body {
		if m.Body[i] == CSVSemicolon {
			semCount ++
			continue
		}
		if semCount == 1 {
			date = append(date, m.Body[i])
		} else if semCount > 1 {
			break
		}
	}
	if len(date) > 6 {
		return string(date[:len(date) - 6])
	}
	return "error"
}

func init() {
	// TODO: remove, deprecated
	flag.Var(&consumerOpts, "reader-opt", "(deprecated) use --consumer-opt")
	flag.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/bitly/go-nsq#Config)")

	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&topics, "topic", "nsq topic (may be given multiple times)")
}

type ConsumerFileLogger struct {
	F *FileLoggerHourRotator
	C *nsq.Consumer
}

type TopicDiscoverer struct {
	topics   map[string]*ConsumerFileLogger
	termChan chan os.Signal
	hupChan  chan os.Signal
	wg       sync.WaitGroup
}

func NewConsumerFileLogger(topic string) (*ConsumerFileLogger, error) {
	f, err := NewFileHourRotateLogger(*gzipEnabled, *lz4Enabled, *compressLevel, *filenameFormat, topic)
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

func (t *TopicDiscoverer) StartTopicRouter(logger *ConsumerFileLogger) {
	t.wg.Add(1)
	defer t.wg.Done()
	go logger.F.router(logger.C)
	<-logger.F.ExitChan
}

func (t *TopicDiscoverer) AllowTopicName(pattern string, name string) bool {
	match, err := regexp.MatchString(pattern, name)
	if err != nil {
		return false
	}

	return match
}

func (t *TopicDiscoverer) SyncTopics(addrs []string, pattern string) {
	newTopics, err := lookupd.GetLookupdTopics(addrs)
	if err != nil {
		log.Printf("ERROR: could not retrieve topic list: %s", err)
	}
	for _, topic := range newTopics {
		if _, ok := t.topics[topic]; !ok {
			if !t.AllowTopicName(pattern, topic) {
				log.Println("Skipping topic ", topic, "as it didn't match required pattern:", pattern)
				continue
			}
			logger, err := NewConsumerFileLogger(topic)
			if err != nil {
				log.Printf("ERROR: couldn't create logger for new topic %s: %s", topic, err)
				continue
			}
			t.topics[topic] = logger
			go t.StartTopicRouter(logger)
		}
	}
}

func (t *TopicDiscoverer) Stop() {
	for _, topic := range t.topics {
		topic.F.termChan <- true
	}
}

func (t *TopicDiscoverer) Hup() {
	for _, topic := range t.topics {
		topic.F.hupChan <- true
	}
}

func (t *TopicDiscoverer) Watch(addrs []string, sync bool, pattern string) {
	ticker := time.Tick(*topicPollRate)
	for {
		select {
		case <-ticker:
			if sync {
				t.SyncTopics(addrs, pattern)
			}
		case <-t.termChan:
			t.Stop()
			t.wg.Wait()
			return
		case <-t.hupChan:
			t.Hup()
		}
	}
}

type FileHandler struct {
	out            *os.File
	writer         io.Writer
	compressWriter io.WriteCloser
}

func (f *FileHandler) Close() error {
	if f.out != nil {
		if f.compressWriter != nil {
			f.compressWriter.Close()
		}
		err := f.out.Sync()
		if err != nil {
			log.Printf("WARN err in sync %v", err)
			return err
		}
		filename := f.out.Name()
		err = f.out.Close()
		if err != nil {
			log.Printf("WARN err in close %v: %v", filename, err)
			return err
		}
		f.out = nil
		if *tmpAndRename {
			MoveFileToOutputDir(filename)
		}
	}
	return nil
}

func (f *FileHandler) Sync() error {
	return f.out.Sync()
}

type FileLoggerHourRotator struct {
	compressLevel  int
	compressMethod CompressMethod
	logChan        chan *nsq.Message
	filenameFormat string

	ExitChan       chan int
	termChan       chan bool
	hupChan        chan bool

	//for many files and rotating by hour in messages
	files          map[string]*FileHandler
	currentHour    string
}

func (l *FileLoggerHourRotator) GetHourWriter(hour string) io.Writer {
	if l.currentHour != hour {
		l.currentHour = hour
	}
	if f, ok := l.files[l.currentHour]; !ok || f.out == nil {
		l.openFile()
	}
	return l.files[l.currentHour].writer
}

func (l *FileLoggerHourRotator) HandleMessage(m *nsq.Message) error {
	m.DisableAutoResponse()
	l.logChan <- m
	return nil
}

func (f *FileLoggerHourRotator) router(r *nsq.Consumer) {
	pos := 0
	output := make([]*nsq.Message, *maxInFlight)
	sync := false
	syncTicker := time.NewTicker(syncEvery)
	closeFileTicker := time.NewTicker(closeEvery)

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
			syncTicker.Stop()
			r.Stop()
			sync = true
			closing = true

		case <-f.hupChan:
			sync = true
			closeFile = true

		case <-syncTicker.C:
			sync = true

		case <-closeFileTicker.C:
			f.refreshFiles()

		case m := <-f.logChan:
			hour := GetMessageHour(m)
			writer := f.GetHourWriter(hour)
			_, err := writer.Write(m.Body)
			if err != nil {
				log.Fatalf("ERROR: writing message to disk - %s", err)
			}
			_, err = writer.Write([]byte("\n"))
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
				f.Sync()
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

func (f *FileLoggerHourRotator) Close() error {
	for fn, fi := range f.files {
		log.Printf("Close: %v", fn)
		fi.Close()
	}
	return nil
}

func (f *FileLoggerHourRotator) refreshFiles() {
	f.Close()
	f.files = map[string]*FileHandler{}
}

func (f *FileLoggerHourRotator) Write(p []byte) (int, error) {
	return f.files[f.currentHour].out.Write(p)
}

func (flhr *FileLoggerHourRotator) Sync() {
	for h, f := range flhr.files {
		log.Printf("Sync: %v", h)
		if f.compressWriter != nil {
			f.compressWriter.Close()
			f.compressWriter = flhr.newCompressWriter()
			f.writer = f.compressWriter
		}
		f.Sync()
	}
}

func (f *FileLoggerHourRotator) newCompressWriter() io.WriteCloser {
	switch f.compressMethod {
	case LZ4:
		return NewLz4Writer(f, f.compressLevel)
	case GZIP:
		cw, _ := gzip.NewWriterLevel(f, f.compressLevel)
		return cw
	}
	return f
}

func (f *FileLoggerHourRotator) calculateCurrentFilename() string {
	result := strings.Replace(f.filenameFormat, "<DATETIME>", f.currentHour, -1)
	return result
}

func (f *FileLoggerHourRotator) openFile() {
	filename := f.calculateCurrentFilename()
	fullPath := path.Join(writeDir, filename)
	dir, _ := filepath.Split(fullPath)
	if dir != "" {
		err := os.MkdirAll(dir, 0770)
		if err != nil {
			log.Fatalf("ERROR: %s Unable to create %s", err, dir)
		}
	}

	var err error
	fh := &FileHandler{}

	out, err := os.OpenFile(fullPath, os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("ERROR: %s. Unable to open %s", err, fullPath)
	}
	fh.out = out

	if f.compressMethod != NO_COMPRESS {
		fh.compressWriter = f.newCompressWriter()
		fh.writer = fh.compressWriter
	} else {
		fh.writer = f
	}

	f.files[f.currentHour] = fh
}

func NewFileHourRotateLogger(gzipEnabled, lz4Enabled bool, compressionLevel int, filenameFormat, topic string) (*FileLoggerHourRotator, error) {
	// remove <REV> as we don't need it
	filenameFormat = strings.Replace(filenameFormat, "<REV>", "", -1)

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
	f := &FileLoggerHourRotator{
		logChan:        make(chan *nsq.Message, 1),
		compressLevel:  compressionLevel,
		filenameFormat: filenameFormat,
		compressMethod: compressMethod,
		ExitChan:       make(chan int),
		termChan:       make(chan bool),
		hupChan:        make(chan bool),

		files:          make(map[string]*FileHandler),

	}
	return f, nil
}

func NewTopicDiscoverer() *TopicDiscoverer {
	return &TopicDiscoverer{
		topics:   make(map[string]*ConsumerFileLogger),
		termChan: make(chan os.Signal),
		hupChan:  make(chan os.Signal),
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_file_rh v%s\n", util.BINARY_VERSION)
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
		log.Fatal("you shold use either --lz4 or --gzip, not both")
	}

	if *compressLevel < 1 || *compressLevel > 9 {
		log.Fatalf("invalid --compress-level value (%d), should be 1-9", *compressLevel)
	}

	if *tmpAndRename {
		MoveAllFilesFromTempDir()
	}

	discoverer := NewTopicDiscoverer()

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
		if !discoverer.AllowTopicName(*topicPattern, topic) {
			log.Println("Skipping topic", topic, "as it didn't match required pattern:", *topicPattern)
			continue
		}

		logger, err := NewConsumerFileLogger(topic)
		if err != nil {
			log.Fatalf("ERROR: couldn't create logger for topic %s: %s", topic, err)
		}
		discoverer.topics[topic] = logger
		log.Printf("Start topic router %v", topic)
		go discoverer.StartTopicRouter(logger)
	}

	discoverer.Watch(lookupdHTTPAddrs, topicsFromNSQLookupd, *topicPattern)
}

func MoveToOutputWalkFunc(name string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if !info.IsDir() {
		MoveFileToOutputDir(name)
	}
	return nil
}

func MoveFileToOutputDir(srcFilename string) {
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
		log.Fatalf("Could not create directories %s: %v", destDir, err)
	}
	err = os.Rename(srcFilename, destFilename)
	if err != nil {
		log.Fatalf("Could not rename %s -> %s: %v", srcFilename, destFilename, err)
	}
}

func MoveAllFilesFromTempDir() {
	err := filepath.Walk(writeDir, MoveToOutputWalkFunc)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		log.Fatalf("Could not move files from temp folder: %v", err)
	}
}

type lz4Writer struct {
	bw   *bufio.Writer
	lz4w *lz4.Writer
}

func NewLz4Writer(w io.Writer, compressLevel int) *lz4Writer {
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

