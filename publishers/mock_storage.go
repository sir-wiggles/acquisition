package publishers

import (
	"encoding/json"
	"io"
	"io/ioutil"
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

func (z *MockQueue) Poll(queue string, amount int) chan *services.Message {
	return nil
}

func (z *MockQueue) Pop(queue, receipt string) error {
	return nil
}

func (z *MockQueue) NewBatch(queue string) services.CobaltQueueBatch {
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

type Pair struct {
	Meta    string
	Content string
}

func ArchiveEntryToFile(reader io.Reader) (*os.File, int64, error) {

	temp, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, -1, err
	}

	size, err := io.Copy(temp, reader)
	_, err = temp.Seek(0, 0)
	return temp, size, err
}

type Stats struct {
	Archive chan int

	Meta    chan int
	Content chan int
	Other   chan int

	Pairs          chan int
	MissingMeta    chan int
	MissingContent chan int

	ProblemFilenames chan string
}

func NewStats() *Stats {
	return &Stats{
		Archive:          make(chan int, 100),
		Meta:             make(chan int, 100),
		Content:          make(chan int, 100),
		Other:            make(chan int, 100),
		Pairs:            make(chan int, 100),
		MissingMeta:      make(chan int, 100),
		MissingContent:   make(chan int, 100),
		ProblemFilenames: make(chan string, 100),
	}
}
