package unpack

import (
	"encoding/json"
	"log"
	"strings"
	"sync"

	"github.com/yewno/acquisition/publishers/pnas"
	"github.com/yewno/acquisition/services"
)

var (
	key         string
	secret      string
	region      string
	inputQueue  string
	outputQueue string
)

type sns struct {
	Records []struct {
		S3 struct {
			Object struct {
				Key  string `json:"key"`
				Size int64  `json:"size"`
			} `json:"object"`
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
		} `json:"s3"`
	} `json:"Records"`
}

var PublisherUnpackingMethod = map[string]func(services.CobaltStorage, string, string) error{
	"pnas": pnas.Process,
}

type Unpacker struct {
	// The queue that sns will put messages into
	InputQueue services.CobaltQueue

	// queue we'll populate for grouper to consume
	OutputQueue services.CobaltQueue

	// connection to our buckets
	Storage services.CobaltStorage

	wg *sync.WaitGroup
}

func NewUnpacker(wg *sync.WaitGroup) (*Unpacker, error) {

	input, err := services.NewCobaltSqs(key, secret, region, inputQueue)
	if err != nil {
		return nil, err
	}

	output, err := services.NewCobaltSqs(key, secret, region, outputQueue)
	if err != nil {
		return nil, err
	}

	return &Unpacker{
		InputQueue:  input,
		OutputQueue: output,
	}, nil
}

func (z *Unpacker) Run() {
	defer z.wg.Done()

	for m := range z.InputQueue.Poll() {

		message := &sns{}

		err := json.Unmarshal([]byte(m.Body), message)
		if err != nil {
			log.Println(err)
			continue
		}

		if len(message.Records) != 1 {
			log.Printf("Expected 1 record got %d", len(message.Records))
			continue
		}

		record := message.Records[0]

		key := record.S3.Object.Key
		bucket := record.S3.Bucket.Name

		publisher := strings.Split(key, "/")[1]

		if process, ok := PublisherUnpackingMethod[publisher]; ok {
			err = process(z.Storage, bucket, key)
		} else {
			log.Println("Processor not found for publisher (%s)", publisher)
			continue
		}

		if err != nil {
			log.Println(err)
			continue
		}

	}
}
