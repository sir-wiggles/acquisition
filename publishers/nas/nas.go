package nas

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"strings"
	"sync"

	"github.com/yewno/acquisition/config"
	"github.com/yewno/acquisition/publishers"
	"github.com/yewno/acquisition/services"
)

type OnixRecord struct {
	XMLName  xml.Name `xml:"Product"`
	Content  []byte   `xml:",innerxml"`
	YewnoIDs []struct {
		Type  string `xml:"ProductIDType"`
		Value string `xml:"IDValue"`
	} `xml:"ProductIdentifier"`
}

type Object struct {
	conn  services.CobaltStorage
	queue services.CobaltQueue
	cfg   *config.Config
	wg    *sync.WaitGroup
	pool  chan bool

	stats *publishers.Stats
}

func NewObject(conn services.CobaltStorage, queue services.CobaltQueue, wg *sync.WaitGroup, cfg *config.Config, pool chan bool, stats *publishers.Stats) *Object {
	wg.Add(1)
	return &Object{
		conn:  conn,
		queue: queue,
		wg:    wg,
		cfg:   cfg,
		pool:  pool,

		stats: stats,
	}
}

func (o *Object) Process(receipt string, message *services.SnsMessage) error {

	defer o.wg.Done()
	defer func() { <-o.pool }()

	var (
		bucket = message.Records[0].S3.Bucket.Name
		key    = message.Records[0].S3.Object.Key
		size   = message.Records[0].S3.Object.Size
		pairs  = make([]string, 0, 100)
	)

	object := services.NewObject(nil, bucket, key, size)
	err := o.conn.Get(object)
	if err != nil {
		return err
	}

	ext := path.Ext(key)

	switch ext {
	case ".pdf":
		key = strings.TrimPrefix(key, "files/")
		pdfObject := services.NewObject(object.File, o.cfg.ProcessedBucket, key, object.Size)
		err = pdfObject.Save(o.conn)
		pdfObject.Close()
	case ".xml":
		decoder := xml.NewDecoder(object.File)
		for {
			t, _ := decoder.Token()

			if t == nil {
				break
			}

			switch se := t.(type) {
			case xml.StartElement:

				if se.Name.Local == "Product" {
					var record OnixRecord
					err := decoder.DecodeElement(&record, &se)

					if err != nil {
						log.Println(err)
						return err
					}

					var bookId string
					for _, id := range record.YewnoIDs {
						if id.Type == "15" {
							bookId = id.Value
						}
					}

					if bookId == "" {
						continue
					}

					key := fmt.Sprintf("nas/%s.xml", bookId)

					bytesArr, err := xml.Marshal(&record)
					if err != nil {
						return err
					}

					tf, err := ioutil.TempFile("", "")
					if err != nil {
						log.Println(err)
						continue
					}

					size, err := tf.Write(bytesArr)
					if err != nil {
						log.Println(err)
						continue
					}

					object := services.NewObject(tf, o.cfg.ProcessedBucket, key, int64(size))
					err = object.Save(o.conn)
					object.Close()

					pairs = append(pairs, object.Key)

				}
			}
		}
		batch := o.queue.NewBatch(o.cfg.ProcessedQueue)
		count := 0
		for _, meta := range pairs {
			count++
			content := fmt.Sprintf("%s.pdf", strings.TrimSuffix(meta, ".xml.gz"))
			m := &services.PairMessage{
				Source:  "nas",
				Bucket:  o.cfg.ProcessedBucket,
				Key:     content,
				MetaKey: meta,
			}
			o.stats.Pairs <- 1
			batch.Add(m)
		}
		o.stats.Report <- fmt.Sprintf("nas:%d", count)
		batch.Flush()
	}

	if err := o.queue.Pop(o.cfg.NewContentQueue, receipt); err != nil {
		log.Println(err)
		return err
	}
	return nil
}
