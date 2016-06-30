package pnas

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/yewno/acquisition/config"
	"github.com/yewno/acquisition/publishers"
	"github.com/yewno/acquisition/services"
)

func TestProcess(t *testing.T) {

	conn := &publishers.MockStorage{}
	queue := &publishers.MockQueue{}
	stats := publishers.NewStats()
	var wg sync.WaitGroup
	wg.Add(1)

	pool := make(chan bool, 1)
	pool <- true

	cfg := &config.Config{
		ProcessedQueue:  "processQueue",
		ProcessedBucket: "../../test/",
		NewContentQueue: "newContentQueue",
	}

	obj := &Object{
		conn:  conn,
		queue: queue,
		wg:    &wg,
		cfg:   cfg,
		pool:  pool,
		stats: stats,
	}

	msgString := fmt.Sprintf(
		`{"Records":[{
			"s3":{
				"bucket":{"name":"%s"},
				"object":{"key":"%s","size":%s}
				}
			}
		]}`,
		"../../test",
		"files/pnas/113_26/pnas_113_26.xml.zip",
		"1",
	)

	msg := &services.SnsMessage{}

	err := json.Unmarshal([]byte(msgString), msg)

	err = obj.Process("foo", msg)

	log.Println(err)

	log.Println(queue.Messages)
	log.Println(len(queue.Messages))
}
