package bmj

import (
	"log"
	"sync"
	"testing"

	"github.com/yewno/acquisition/publishers"
)

var YewnoProcessedBucket string

func init() {
	log.SetFlags(log.Lshortfile)
}

func TestProcess(t *testing.T) {

	YewnoProcessedBucket = "../../test/processed"

	var wg sync.WaitGroup

	conn := &publishers.MockStorage{}
	queue := &publishers.MockQueue{make([]string, 0)}

	obj := NewObject(conn, queue, &wg)

	err := obj.Process("../../test", "files/bmj/test.tar.gz")

	log.Println(err, queue.Messages)
}
