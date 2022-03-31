package db

type DB interface {
	Write(tasks []*WriteDBTask) error
}
