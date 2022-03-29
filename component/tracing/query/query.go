package query

import "github.com/pingcap/ng-monitoring/component/tracing/model"

type Query interface {
	Trace(traceID uint64) (*model.Trace, error)
	Close()
}
