package services

type CobaltQueue interface {
	Poll(string, int) chan *Message
	Pop(string, string) error
	NewBatch(string) CobaltQueueBatch
}

type CobaltQueueBatch interface {
	Add(interface{}) error
	Flush()
}

type CobaltStorage interface {
	Get(*Object) error
	Put(*Object) error
}
