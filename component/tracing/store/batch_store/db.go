package batch_store

type DB interface {
	Write(tasks []*WriteDBTask) error
}
