package query

import "github.com/pingcap/ng-monitoring/component/tracing/model"

type Query interface {
	Trace(traceID string) (*model.Trace, error)
	Close()
}
