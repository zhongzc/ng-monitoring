package db

import "github.com/pingcap/kvproto/pkg/tracepb"

type WriteDBTask TraceItem

type TraceItem struct {
	TraceID      uint64
	Instance     string
	InstanceType string
	CreatedTsMs  int64
	Spans        []*tracepb.Span
}

type DB interface {
	Put(tasks []*WriteDBTask) error
	Get(traceID uint64, fill *[]*TraceItem) error

	//// RemoveAllBefore removes all traces with a CreatedTsMs less than
	//// the given `createdTimeMs`.
	//RemoveAllBefore(createdTimeMs int64) error
}
