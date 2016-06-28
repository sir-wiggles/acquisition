package services

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/satori/go.uuid"
	"github.com/yewno/acquisition/config"
)

// Message
type Message struct {
	QueueUrl string `json:"-"`
	Receipt  string `json:"-"`
	Body     string `json:"body"`
}

type PairMessage struct {
	Source  string `json:"source"`
	Bucket  string `json:"bucket"`
	Key     string `json:"key"`
	MetaKey string `json:"metakey,omitempty"`
}

type SnsMessage struct {
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

// CobalsSqs is a struct that handles communication for SQS
type CobaltSqs struct {
	Conn      *sqs.SQS
	queueURLs map[string]string
}

// NewCobaltSqs will return a queue connection. If key and secret are empty strings then they
// will be pulled from the .aws credentials file.  Queue is the name of the queue and not the
// url. If a queue with that name doesn't exist then an error will be returned.
func NewCobaltSqs(cfg *config.Config, queues ...string) (CobaltQueue, error) {

	var conn *sqs.SQS

	if cfg.Key == "" && cfg.Secret == "" {
		conn = sqs.New(session.New(&aws.Config{
			Region: aws.String(cfg.Region),
		}))

	} else {
		conn = sqs.New(session.New(&aws.Config{
			Region:      aws.String(cfg.Region),
			Credentials: credentials.NewStaticCredentials(cfg.Key, cfg.Secret, ""),
		}))
	}

	queueURLs := make(map[string]string, len(queues))
	for _, q := range queues {
		resp, err := conn.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(q)})
		if err != nil {
			return nil, err
		}
		queueURLs[q] = *resp.QueueUrl

	}

	return &CobaltSqs{
		Conn:      conn,
		queueURLs: queueURLs,
	}, nil
}

// Poll receives messages from the queue and populates the channel. Poll will continue
// to check the queue for messages returning nil when no messages were found.
func (c *CobaltSqs) Poll(queue string, max int) chan *Message {
	response := make(chan *Message, max)

	go func(channel chan *Message) {
		defer close(channel)

		params := &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(c.queueURLs[queue]),
			MaxNumberOfMessages:   aws.Int64(int64(max)),
			MessageAttributeNames: []*string{aws.String("ALL")},
			WaitTimeSeconds:       aws.Int64(2),
		}

		for {

			resp, err := c.Conn.ReceiveMessage(params)
			if err != nil {
				log.Println(err)
				continue
			}

			if len(resp.Messages) == 0 {
				channel <- nil
				continue
			}

			for _, m := range resp.Messages {
				channel <- &Message{Body: *m.Body, Receipt: *m.ReceiptHandle}
			}
		}
	}(response)
	return response
}

// Pop removes a message from the queue
func (c *CobaltSqs) Pop(queue, receipt string) error {

	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURLs[queue]),
		ReceiptHandle: aws.String(receipt),
	}
	_, err := c.Conn.DeleteMessage(params)

	if err != nil {
		return err
	}
	return nil
}

// NewBatch returns a batch struct that will bundle sqs messages and send them up
// onces the buffer is full (10 messages) or if the last message received was more than
// 10 seconds ago.
func (c *CobaltSqs) NewBatch(queue string) CobaltQueueBatch {
	return NewCobaltSqsBatch(c.Conn, c.queueURLs[queue])
}

// NewCobaltSqsBatch creates a new batch struct attached to the queue it was called from.
func NewCobaltSqsBatch(conn *sqs.SQS, queue string) CobaltQueueBatch {
	var wg sync.WaitGroup

	batch := &CobaltSqsBatch{
		messages: make(chan *sqs.SendMessageBatchRequestEntry, 10),
		conn:     conn,
		queue:    queue,
		wg:       &wg,
	}

	wg.Add(1)
	go batch.process()
	return batch
}

// CobaltSqsBatch struct that handles the actual batching.
type CobaltSqsBatch struct {
	messages chan *sqs.SendMessageBatchRequestEntry
	conn     *sqs.SQS
	queue    string
	wg       *sync.WaitGroup
}

// Add adds a message to the batch to be send up to the queue
func (c *CobaltSqsBatch) Add(msg interface{}) error {

	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	entry := &sqs.SendMessageBatchRequestEntry{
		Id:          aws.String(fmt.Sprintf("%s", uuid.NewV4())),
		MessageBody: aws.String(string(b)),
	}

	c.messages <- entry
	return nil
}

// Flush sends any remaining messages to the queue.
func (c *CobaltSqsBatch) Flush() {
	close(c.messages)
	c.wg.Wait()
}

func (c *CobaltSqsBatch) process() {
	defer c.wg.Done()

	params := &sqs.SendMessageBatchInput{QueueUrl: aws.String(c.queue)}
	ticker := time.NewTicker(time.Second * 1)
	count := 0
	batch := make([]*sqs.SendMessageBatchRequestEntry, 0, 10)
	lastMessage := time.Now()

	for {
		select {

		case m, ok := <-c.messages:
			if !ok {
				goto BatchClosed
			}
			lastMessage = time.Now()

			count++

			batch = append(batch, m)
			if len(batch) < 10 {
				continue
			}

		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}

			if time.Now().Sub(lastMessage) < time.Second*10 {
				continue
			}
		}

		params.Entries = batch
		batch = c.send(params)
	}

BatchClosed:

	if len(batch) > 0 {
		params.Entries = batch
		c.send(params)
	}
}

func (c *CobaltSqsBatch) send(params *sqs.SendMessageBatchInput) []*sqs.SendMessageBatchRequestEntry {
	resp, err := c.conn.SendMessageBatch(params)
	if err != nil {
		log.Println(err)
		return make([]*sqs.SendMessageBatchRequestEntry, 0, 10)
	}
	if len(resp.Failed) > 0 {
		for _, item := range resp.Failed {
			log.Printf(fmt.Sprintf("Code: %s, Message: %s", *item.Code, *item.Message))
		}
	}
	return make([]*sqs.SendMessageBatchRequestEntry, 0, 10)
}
