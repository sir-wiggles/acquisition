package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/yewno/acquisition/config"
	"github.com/yewno/acquisition/publishers/bmj"
	"github.com/yewno/acquisition/services"
)

var (
	cfg *config.Config
)

func init() {
	log.SetFlags(log.Lshortfile | log.Ltime)

	cfg = &config.Config{

		ProcessedBucket: "yewno-content",
		FtpBucket:       "yewno-ftp",

		NewContentQueue: "yewno-cobalt-list",
		ProcessedQueue:  "yewno-ingestion",

		Key:    "",
		Secret: "",
		Region: "us-west-2",
	}
}

func main() {
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

	pool := make(chan bool, 20)
	for m := range queue.Poll(cfg.NewContentQueue, 1) {

		if m == nil {
			log.Println("No messages received")
			break
		}

		message, err := parse(m.Body)
		if err != nil {
			log.Println(err)
			continue
		}

		obj := bmj.NewObject(storage, queue, &wg, cfg, pool)

		pool <- true
		go obj.Process(m.Receipt, message)
	}

	wg.Wait()
	log.Println("Done")

}

func parse(msg string) (*services.SnsMessage, error) {

	message := &services.SnsMessage{}
	err := json.Unmarshal([]byte(msg), message)

	return message, err
}
