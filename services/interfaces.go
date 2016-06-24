package services

type CobaltQueue interface {
	Poll() chan *Message
	Pop(*Message) error
	NewBatch() CobaltQueueBatch
}

type CobaltQueueBatch interface {
	Add(interface{}) error
	Flush()
}

type CobaltStorage interface {
	Get(*Object) error
	Put(*Object) error
}
