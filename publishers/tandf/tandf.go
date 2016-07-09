package tandf

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

type Object struct {
	conn  services.CobaltStorage
	queue services.CobaltQueue
	wg    *sync.WaitGroup
	cfg   *config.Config
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
		pairs  = make(map[string]*publishers.Pair, 100)
	)
	zipFilename := key
	log.Println(zipFilename)

	object := services.NewObject(nil, bucket, key, size)

	if err := o.conn.Get(object); err != nil {
		log.Println(err)
		return err
	}
	defer object.Close()

	zipReader, err := zip.NewReader(object.File, object.Size)
	if err != nil {
		log.Println(err)
		return err
	}

	for _, item := range zipReader.File {

		base := strings.Split(item.Name, "/")[1]

		f, err := item.Open()
		if err != nil {
			log.Println(err)
			continue
		}

		file, size, err := publishers.ArchiveEntryToFile(f)
		if err != nil {
			log.Println(err)
		}

		_, fn := path.Split(item.Name)

		ext := path.Ext(item.Name)
		key := ""
		switch ext {
		case ".pdf", ".xml":
			key = fmt.Sprintf("tandf/%s", fn)
		default:
			key = fmt.Sprintf("tandf/%s/%s", base, fn)
		}
		log.Println(key)

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
	}

	batch := o.queue.NewBatch(o.cfg.ProcessedQueue)
	for _, p := range pairs {
		if p.Meta != "" && p.Content != "" {
			m := &services.PairMessage{
				Source:  "tandf",
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

	if err = o.queue.Pop(o.cfg.NewContentQueue, receipt); err != nil {
		log.Println(err)
		return err
	}
	return nil

}
