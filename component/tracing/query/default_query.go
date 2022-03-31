package query

import (
	"github.com/pingcap/ng-monitoring/component/tracing/db"
	"github.com/pingcap/ng-monitoring/component/tracing/model"
)

var (
	traceItemSliceP = &TraceItemSlicePool{}
)

type DefaultQuery struct {
	db db.DB
}

func NewDefaultQuery(db db.DB) *DefaultQuery {
	return &DefaultQuery{db: db}
}

var _ Query = &DefaultQuery{}

func (q *DefaultQuery) Trace(traceID uint64) (*model.Trace, error) {
	traceItems := traceItemSliceP.Get()
	defer traceItemSliceP.Put(traceItems)

	err := q.db.Get(traceID, traceItems)
	if err != nil {
		return nil, err
	}

	spanGroups := make([]model.SpanGroup, 0, len(*traceItems))
	for _, item := range *traceItems {
		spanGroups = append(spanGroups, model.SpanGroup{
			Instance:     item.Instance,
			InstanceType: item.InstanceType,
			Spans:        item.Spans,
		})
	}
	trace := &model.Trace{TraceID: traceID, SpanGroups: spanGroups}
	return trace, nil
}

func (q *DefaultQuery) Close() {}
