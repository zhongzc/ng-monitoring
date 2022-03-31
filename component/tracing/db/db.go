package db

import "github.com/pingcap/kvproto/pkg/tracepb"

type WriteDBTask struct {
	TraceID      string
	Instance     string
	InstanceType string
	CreatedTsMs  int64
	Spans        []*tracepb.Span
}

type DB interface {
	Write(tasks []*WriteDBTask) error
}
