package publishers

import (
	"encoding/json"
	"io"
	"os"
	"path"

	"github.com/yewno/acquisition/services"
)

type MockStorage struct{}

func (z *MockStorage) Get(o *services.Object) error {

	p := path.Join(o.Bucket, o.Key)
	file, err := os.Open(p)
	if err != nil {
		return err
	}

	o.File = file

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	o.Size = stat.Size()

	return nil
}

func (z *MockStorage) Put(o *services.Object) error {

	filepath := path.Join(o.Bucket, o.Key)

	dir, _ := path.Split(filepath)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return err
	}

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, o.File)
	if err != nil {
		return err
	}
	return nil
}

type MockQueue struct {
	Messages []string
}

func (z *MockQueue) Poll() chan *services.Message {
	return nil
}

func (z *MockQueue) Pop(m string) error {
	return nil
}

func (z *MockQueue) NewBatch() services.CobaltQueueBatch {
	return &MockBatch{
		Queue: z,
	}
}

type MockBatch struct {
	Queue *MockQueue
}

func (z *MockBatch) Add(m interface{}) error {

	body, err := json.Marshal(m)
	if err != nil {
		return err
	}

	z.Queue.Messages = append(z.Queue.Messages, string(body))
	return nil
}

func (z *MockBatch) Flush() {

	return
}
