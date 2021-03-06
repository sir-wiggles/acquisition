package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/yewno/acquisition/config"
	"github.com/yewno/acquisition/publishers"
	"github.com/yewno/acquisition/publishers/acm"
	"github.com/yewno/acquisition/publishers/bmj"
	"github.com/yewno/acquisition/publishers/cup"
	"github.com/yewno/acquisition/publishers/nas"
	"github.com/yewno/acquisition/publishers/oup"
	"github.com/yewno/acquisition/publishers/pnas"
	"github.com/yewno/acquisition/publishers/tandf"
	"github.com/yewno/acquisition/services"
)

var (
	cfg *config.Config

	__workers__ int

	__processed_bucket__  string
	__ftp_bucket__        string
	__new_content_queue__ string
	__processed_queue__   string
	__key__               string
	__secret__            string
	__region__            string
)

func init() {

	flag.IntVar(&__workers__, "workers", 2, "number of workers to run")
	flag.StringVar(&__processed_bucket__, "processed-bucket", "yewno-content", "bucket where processed items go")
	flag.StringVar(&__ftp_bucket__, "ftp-bucket", "yewno-ftp", "bucket with raw files")
	flag.StringVar(&__new_content_queue__, "new-content-queue", "yewno-cobalt-list", "queue that has new content from sns")
	flag.StringVar(&__processed_queue__, "processed-queue", "yewno-ingestion", "queue where pairs to sent for ingestion")
	flag.StringVar(&__key__, "key", "", "aws key")
	flag.StringVar(&__secret__, "secret", "", "aws secret")
	flag.StringVar(&__region__, "region", "us-west-2", "region")

	log.SetFlags(log.Lshortfile | log.Ltime)

}

func main() {

	flag.Parse()

	cfg = &config.Config{

		ProcessedBucket: __processed_bucket__,
		FtpBucket:       __ftp_bucket__,
		NewContentQueue: __new_content_queue__,
		ProcessedQueue:  __processed_queue__,
		Key:             __key__,
		Secret:          __secret__,
		Region:          __region__,
	}

	log.Println("Starting acquisition ...")

	var (
		wg sync.WaitGroup
	)

	storage, err := services.NewCobaltS3(cfg)
	if err != nil {
		log.Fatal(err)
	}

	queue, err := services.NewCobaltSqs(cfg, cfg.NewContentQueue, cfg.ProcessedQueue)
	if err != nil {
		log.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	stats := publishers.NewStats()
	control := make(chan bool, 1)
	go logger(stats, control)

	pool := make(chan bool, __workers__)
	for m := range queue.Poll(cfg.NewContentQueue, 1) {

		select {
		case <-sig:
			goto PollBreak
		default:
		}

		if m == nil {
			log.Println("No messages received")
			break
		}

		message, err := parse(m.Body)
		if err != nil {
			log.Println(m.Body)
			log.Println(err)
			continue
		}

		publisher := strings.Split(message.Records[0].S3.Object.Key, "/")[1]

		switch publisher {
		case "pnas":
			obj := pnas.NewObject(storage, queue, &wg, cfg, pool, stats)
			go obj.Process(m.Receipt, message)
		case "bmj":
			obj := bmj.NewObject(storage, queue, &wg, cfg, pool, stats)
			go obj.Process(m.Receipt, message)
		case "cup":
			obj := cup.NewObject(storage, queue, &wg, cfg, pool, stats)
			go obj.Process(m.Receipt, message)
		case "tandf":
			obj := tandf.NewObject(storage, queue, &wg, cfg, pool, stats)
			go obj.Process(m.Receipt, message)
		case "oup":
			obj := oup.NewObject(storage, queue, &wg, cfg, pool, stats)
			go obj.Process(m.Receipt, message)
		case "acm":
			obj := acm.NewObject(storage, queue, &wg, cfg, pool, stats)
			go obj.Process(m.Receipt, message)
		case "nas":
			obj := nas.NewObject(storage, queue, &wg, cfg, pool, stats)
			go obj.Process(m.Receipt, message)
		default:
			log.Printf("Missing process for (%s)", publisher)
			continue
		}

		pool <- true
	}

PollBreak:
	wg.Wait()
	control <- true
	log.Println("Done")
	<-control

}

func parse(msg string) (*services.SnsMessage, error) {

	message := &services.SnsMessage{}
	err := json.Unmarshal([]byte(msg), message)

	return message, err
}

func logger(stats *publishers.Stats, control chan bool) {
	var (
		archive         int
		meta            int
		content         int
		other           int
		pairs           int
		missingMeta     int
		missingContent  int
		publisherReport = make(map[string]int64, 10)
	)

	file, err := os.OpenFile("report.log", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	logger := log.New(file, "", log.LstdFlags)

	for {
		select {
		case <-stats.Archive:
			archive++
		case <-stats.Meta:
			meta++
		case <-stats.Content:
			content++
		case <-stats.Other:
			other++
		case <-stats.Pairs:
			pairs++
		case <-stats.MissingMeta:
			missingMeta++
		case <-stats.MissingContent:
			missingContent++
		case fn := <-stats.ProblemFilenames:
			logger.Println(fn)
		case report := <-stats.Report:
			parts := strings.Split(report, ":")
			val := publisherReport[parts[0]]
			sent, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				log.Println(err)
				continue
			}
			publisherReport[parts[0]] = val + sent

		case <-control:
			goto LogBreak
		}
	}
LogBreak:
	logger.Printf(logFormat, archive, meta, content, other, pairs, missingMeta, missingContent)

	for k, v := range publisherReport {
		logger.Printf("%s: %d", k, v)
		log.Printf("%s: %d", k, v)
	}

	control <- true
}

var logFormat = `
Number of archive files processed
Archive       : %d

Number of meta objects handled
Meta          : %d

Number of content objects handled
Content       : %d

Number of other objects handled
Other         : %d

Number of pairings (meta to content) found
Pairs         : %d

Content objects without meta objects
MissingMeta   : %d

Meta objects without content objects
MissingContent: %d
`
