package services

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/satori/go.uuid"
)

// Message
type Message struct {
	QueueUrl      string `json:"-"`
	ReceiptHandle string `json:"-"`
	Body          string `json:"body"`
}

// CobalsSqs is a struct that handles communication for SQS
type CobaltSqs struct {
	Conn  *sqs.SQS
	queue string
}

// NewCobaltSqs will return a queue connection. If key and secret are empty strings then they
// will be pulled from the .aws credentials file.  Queue is the name of the queue and not the
// url. If a queue with that name doesn't exist then an error will be returned.
func NewCobaltSqs(key, secret, region, queue string) (CobaltQueue, error) {

	var conn *sqs.SQS

	if key == "" && secret == "" {
		conn = sqs.New(session.New(&aws.Config{
			Region: aws.String(region),
		}))

	} else {
		conn = sqs.New(session.New(&aws.Config{
			Region:      aws.String(region),
			Credentials: credentials.NewStaticCredentials(key, secret, ""),
		}))
	}

	resp, err := conn.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(queue)})
	if err != nil {
		return nil, err
	}

	return &CobaltSqs{
		Conn:  conn,
		queue: *resp.QueueUrl,
	}, nil
}

// Poll receives messages from the queue and populates the channel. Poll will continue
// to check the queue for messages returning nil when no messages were found.
func (c *CobaltSqs) Poll() chan *Message {
	response := make(chan *Message, 10)

	go func(channel chan *Message) {
		defer close(channel)

		params := &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(c.queue),
			MaxNumberOfMessages:   aws.Int64(10),
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

				msg := &Message{}

				body := strings.Replace(*m.Body, "\n", "", -1)
				err = json.Unmarshal([]byte(body), msg)
				if err != nil {
					log.Println(err)
					continue
				}

				msg.ReceiptHandle = *m.ReceiptHandle
				msg.QueueUrl = c.queue

				channel <- msg
			}
		}
	}(response)
	return response
}

// Pop removes a message from the queue
func (c *CobaltSqs) Pop(msg *Message) error {

	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(msg.QueueUrl),
		ReceiptHandle: aws.String(msg.ReceiptHandle),
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
func (c *CobaltSqs) NewBatch() CobaltQueueBatch {
	return NewCobaltSqsBatch(c.Conn, c.queue)
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
