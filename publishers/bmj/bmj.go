package bmj

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/yewno/acquisition/config"
	"github.com/yewno/acquisition/services"
)

type Object struct {
	conn  services.CobaltStorage
	queue services.CobaltQueue
	wg    *sync.WaitGroup
	cfg   *config.Config
	pool  chan bool
}

func NewObject(conn services.CobaltStorage, queue services.CobaltQueue, wg *sync.WaitGroup, cfg *config.Config, pool chan bool) *Object {
	wg.Add(1)
	return &Object{
		conn:  conn,
		queue: queue,
		wg:    wg,
		cfg:   cfg,
		pool:  pool,
	}
}

func (o *Object) Process(receipt string, message *services.SnsMessage) error {

	defer o.wg.Done()
	defer func() { <-o.pool }()

	var (
		bucket = message.Records[0].S3.Bucket.Name
		key    = message.Records[0].S3.Object.Key
		size   = message.Records[0].S3.Object.Size
	)
	log.Println(bucket, key, size)

	publisher := strings.Split(key, "/")[1]
	if publisher != "bmj" {
		return nil
	}

	object := services.NewObject(nil, bucket, key, size)

	if err := o.conn.Get(object); err != nil {
		log.Println("Getting key: ", err)
		return err
	}
	defer object.Close()

	gzipReader, err := gzip.NewReader(object.File)
	if err != nil {
		log.Println("New reader: ", err)
		return err
	}

	tarReader := tar.NewReader(gzipReader)

	pairs := make(map[string]*Pair, 100)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			log.Println("Reading tar: ", err)
			continue
		}

		if header.FileInfo().IsDir() {
			continue
		}

		key := fmt.Sprintf("bmj/%s", header.Name)
		file, size, err := ArchiveEntryToFile(tarReader)
		if err != nil {
			continue
		}

		base := strings.Split(header.Name, ".")[0]

		object := services.NewObject(file, o.cfg.ProcessedBucket, key, size)

		ext := path.Ext(header.Name)
		err = object.Save(o.conn)
		object.Close()

		switch ext {
		case ".pdf":
			pair, ok := pairs[base]
			if !ok {
				pairs[base] = &Pair{Content: object.Key}
			} else {
				pair.Content = object.Key
			}

		case ".xml":
			pair, ok := pairs[base]
			if !ok {
				pairs[base] = &Pair{Meta: object.Key}
			} else {
				pair.Meta = object.Key
			}
		}
		if err != nil {
			log.Println(err)
		}
	}

	//log.Printf("Pairs %d", len(pairs))

	batch := o.queue.NewBatch(o.cfg.ProcessedQueue)
	for _, p := range pairs {
		if p.Meta != "" && p.Content != "" {
			m := &services.PairMessage{
				Source:  "bmj",
				Bucket:  o.cfg.ProcessedBucket,
				Key:     p.Content,
				MetaKey: p.Meta,
			}
			batch.Add(m)
		}
	}
	batch.Flush()

	if err = o.queue.Pop(o.cfg.NewContentQueue, receipt); err != nil {
		log.Println(err)
		return err
	}

	return nil
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
