package batch_store

import (
	"errors"
	"strconv"
	"time"

	"github.com/pingcap/ng-monitoring/component/tracing/store"

	"github.com/pingcap/kvproto/pkg/tracepb"
)

type TaskTransfer struct {
	taskChan chan *WriteDBTask
}

func NewTaskTransfer(capacity int) *TaskTransfer {
	return &TaskTransfer{
		taskChan: make(chan *WriteDBTask, capacity),
	}
}

var _ store.Store = &TaskTransfer{}

func (t *TaskTransfer) TraceRecord(instance, instanceType string, createdTime time.Time, record *tracepb.Report) error {
	tsMs := createdTime.UnixNano() / int64(time.Millisecond)
	for i, parent := range record.RemoteParentSpans {
		traceID := parent.TraceId
		parentID := parent.SpanId

		// clone spans if needed
		var spans []*tracepb.Span
		if i == len(record.RemoteParentSpans)-1 {
			spans = record.Spans
		} else {
			spans = cloneSpans(record.Spans)
		}

		// modify parent id
		accessRootSpans(spans, func(span *tracepb.Span) {
			span.ParentId = parentID
		})

		task := &WriteDBTask{
			Instance:     instance,
			InstanceType: instanceType,
			TraceID:      strconv.FormatUint(traceID, 10),
			CreatedTsMs:  tsMs,
			Spans:        spans,
		}

		select {
		case t.taskChan <- task:
		default:
			return errors.New("task channel is full")
		}
	}

	return nil
}

func (t *TaskTransfer) Close() {}

func (t *TaskTransfer) Receiver() <-chan *WriteDBTask {
	return t.taskChan
}

func accessRootSpans(spans []*tracepb.Span, fn func(span *tracepb.Span)) {
	for _, span := range spans {
		if span.ParentId == 0 {
			fn(span)
		}
	}
}

func cloneSpans(spans []*tracepb.Span) []*tracepb.Span {
	newSpans := make([]*tracepb.Span, len(spans))
	for i, span := range spans {
		newSpan := *span
		newSpans[i] = &newSpan
	}
	return newSpans
}
