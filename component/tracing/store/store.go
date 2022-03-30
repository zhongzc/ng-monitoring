package store

import (
	"time"

	"github.com/pingcap/kvproto/pkg/tracepb"
)

type Store interface {
	// TraceRecord stores a record of trace spans.
	// Whether it is synchronous or asynchronous is determined by the implementation.
	TraceRecord(instance, instanceType string, createdTime time.Time, record *tracepb.Report) error
	Close()
}
