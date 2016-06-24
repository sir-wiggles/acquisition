package unpack

import (
	"fmt"
	"sync"
	"testing"

	"github.com/yewno/acquisition/services"
)

type mockQueue1 struct{}

func (z *mockQueue1) Poll() chan *services.Message {
	messages := make(chan *services.Message, 1)
	go func() {
		defer close(messages)
		for _, p := range snss {
			messages <- &services.Message{
				Body: fmt.Sprintf(snsTemplate, p.bucket, p.key, p.size),
			}
		}
	}()
	return messages
}

func (z *mockQueue1) NewBatch() services.CobaltQueueBatch {
	return nil
}

func (z *mockQueue1) Pop(m *services.Message) error {
	return nil
}

var snsTemplate = `{"Records":[{"s3":{"bucket":{"name":"%s"},"object":{"key":"%s","size":%d}}}]}`

var snss = []struct {
	bucket string
	key    string
	size   int
}{
	{"../test", "files/pnas/113_1/pnas_113_1.img.zip", 0},
	{"../test", "files/pnas/113_1/pnas_113_1.pdf.zip", 0},
	{"../test", "files/pnas/113_1/pnas_113_1.peripherals.zip", 0},
	{"../test", "files/pnas/113_1/pnas_113_1.text_images.zip", 0},
	{"../test", "files/pnas/113_1/pnas_113_1.xml.zip", 0},
}

func TestRun(t *testing.T) {

	var wg sync.WaitGroup

	wg.Add(1)
	unpacker := &Unpacker{
		InputQueue: &mockQueue1{},
		wg:         &wg,
	}

	unpacker.Run()
}
