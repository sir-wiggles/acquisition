package pnas

import (
	"archive/zip"
	"fmt"
	"log"
	"path"
	"strings"
	"sync"

	"github.com/yewno/acquisition/config"
	"github.com/yewno/acquisition/publishers"
	"github.com/yewno/acquisition/services"
)

var extensions = []string{".xml.zip", ".pdf.zip"}

type Object struct {
	conn  services.CobaltStorage
	queue services.CobaltQueue
	cfg   *config.Config
	wg    *sync.WaitGroup
	pool  chan bool

	stats *publishers.Stats
}

func (o *Object) Process(receipt string, message *services.SnsMessage) error {

	defer o.wg.Done()
	defer func() { <-o.pool }()

	var (
		bucket = message.Records[0].S3.Bucket.Name
		key    = message.Records[0].S3.Object.Key
		size   = message.Records[0].S3.Object.Size
		pairs  = make(map[string]*publishers.Pair, 100)
	)
	zipFilename := key

	log.Println(message, bucket, key)

	if !strings.HasSuffix(key, ".xml.zip") {
		return o.removeMessage(receipt)
	}

	// Get meta files
	object := services.NewObject(nil, bucket, key, size)

	err := o.conn.Get(object)
	if err != nil {
		return err
	}

	readerXml, err := zip.NewReader(object.File, object.Size)
	if err != nil {
		return err
	}

	// Get content files
	key = fmt.Sprintf("%s.pdf.zip", strings.TrimSuffix(key, ".xml.zip"))
	object = services.NewObject(nil, bucket, key, 0)

	err = o.conn.Get(object)
	if err != nil {
		return err
	}

	readerZip, err := zip.NewReader(object.File, object.Size)
	if err != nil {
		return err
	}

	dir, _ := path.Split(object.Key)
	prefix := strings.Split(dir, "/")[2]

	for _, reader := range []*zip.Reader{readerXml, readerZip} {

		for _, zipFile := range reader.File {

			if zipFile.FileInfo().IsDir() {
				continue
			}

			key := fmt.Sprintf("pnas/%s-%s", prefix, zipFile.Name)
			base := strings.Split(zipFile.Name, ".")[0]
			ext := path.Ext(zipFile.Name)

			log.Println(key)

			f, err := zipFile.Open()
			if err != nil {
				continue
			}

			file, size, err := publishers.ArchiveEntryToFile(f)
			if err != nil {
				continue
			}
			f.Close()

			object := services.NewObject(file, o.cfg.ProcessedBucket, key, size)

			err = object.Save(o.conn)
			object.Close()

			switch ext {

			case ".pdf":
				pair, ok := pairs[base]
				if !ok {
					pairs[base] = &publishers.Pair{Content: object.Key}
				} else {
					pair.Content = object.Key
				}
				o.stats.Content <- 1

			case ".xml":
				pair, ok := pairs[base]
				if !ok {
					pairs[base] = &publishers.Pair{Meta: object.Key}
				} else {
					pair.Meta = object.Key
				}
				o.stats.Meta <- 1

			default:
				o.stats.Other <- 1
			}

			if err != nil {
				log.Println(err)
			}
		}
	}

	batch := o.queue.NewBatch(o.cfg.ProcessedQueue)
	for _, p := range pairs {
		if p.Meta != "" && p.Content != "" {
			m := &services.PairMessage{
				Source:  "pnas",
				Bucket:  o.cfg.ProcessedBucket,
				Key:     p.Content,
				MetaKey: p.Meta,
			}
			o.stats.Pairs <- 1
			batch.Add(m)
		} else {
			var key string
			if p.Meta == "" {
				o.stats.MissingMeta <- 1
				key = p.Content
			} else {
				o.stats.MissingContent <- 1
				key = p.Meta
			}
			o.stats.ProblemFilenames <- fmt.Sprintf("%s/%s", zipFilename, key)
		}
	}
	batch.Flush()

	o.removeMessage(receipt)
	return nil
}

func (o *Object) removeMessage(receipt string) error {
	if err := o.queue.Pop(o.cfg.NewContentQueue, receipt); err != nil {
		log.Println(err)
		return err
	}
	return nil
}
