package store

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/tracepb"
	"github.com/pingcap/ng-monitoring/component/tracing/model"
	"github.com/stretchr/testify/require"
)

func TestStoreBasic(t *testing.T) {
	store := NewMockStore()
	defer store.Close()

	instance := "tidb:10080"
	instanceType := "tidb"
	parents := []*tracepb.RemoteParentSpan{{
		TraceId: 42,
		SpanId:  10,
	}}
	spans := []*tracepb.Span{{
		SpanId:   20,
		ParentId: 0,
	}}
	record := &tracepb.Report{RemoteParentSpans: parents, Spans: spans}

	err := store.TraceRecord(instance, instanceType, time.Now(), record)
	require.NoError(t, err)

	traceID := parents[0].TraceId
	trace, err := store.GetTrace(traceID)
	require.NoError(t, err)
	require.Equal(t, trace.TraceID, traceID)
	require.Equal(t, len(trace.SpanGroups), 1)

	spanGroup := trace.SpanGroups[0]
	require.Equal(t, spanGroup.Instance, instance)
	require.Equal(t, spanGroup.InstanceType, instanceType)
	require.Equal(t, len(spanGroup.Spans), len(spans))

	span := spanGroup.Spans[0]
	require.Equal(t, span.SpanId, spans[0].SpanId)
	require.Equal(t, span.ParentId, parents[0].SpanId)
}

func TestStoreNil(t *testing.T) {
	store := NewMockStore()
	defer store.Close()

	instance := "tidb:10080"
	instanceType := "tidb"
	err := store.TraceRecord(instance, instanceType, time.Now(), &tracepb.Report{})
	require.NoError(t, err)
}

func TestStoreMultiParents(t *testing.T) {
	store := NewMockStore()
	defer store.Close()

	instance := "tidb:10080"
	instanceType := "tidb"
	parents := []*tracepb.RemoteParentSpan{{
		TraceId: 42,
		SpanId:  10,
	}, {
		TraceId: 43,
		SpanId:  11,
	}}
	spans := []*tracepb.Span{{
		SpanId:   20,
		ParentId: 0,
	}}
	record := &tracepb.Report{RemoteParentSpans: parents, Spans: spans}

	err := store.TraceRecord(instance, instanceType, time.Now(), record)
	require.NoError(t, err)

	for _, parent := range parents {
		traceID := parent.TraceId
		parentID := parent.SpanId

		trace, err := store.GetTrace(traceID)
		require.NoError(t, err)
		require.Equal(t, trace.TraceID, traceID)
		require.Equal(t, len(trace.SpanGroups), 1)

		spanGroup := trace.SpanGroups[0]
		require.Equal(t, spanGroup.Instance, instance)
		require.Equal(t, spanGroup.InstanceType, instanceType)
		require.Equal(t, len(spanGroup.Spans), len(spans))

		span := spanGroup.Spans[0]
		require.Equal(t, span.SpanId, spans[0].SpanId)
		require.Equal(t, span.ParentId, parentID)
	}
}

func TestStoreSameTraceIDTwice(t *testing.T) {
	store := NewMockStore()
	defer store.Close()

	traceID := uint64(42)

	// trace record from tidb with the same traceID
	instance0 := "tidb:10080"
	instanceType0 := "tidb"
	parents0 := []*tracepb.RemoteParentSpan{{
		TraceId: traceID,
		SpanId:  10,
	}}
	spans0 := []*tracepb.Span{{
		SpanId:   20,
		ParentId: 0,
	}}
	record0 := &tracepb.Report{RemoteParentSpans: parents0, Spans: spans0}
	err := store.TraceRecord(instance0, instanceType0, time.Now(), record0)
	require.NoError(t, err)

	// trace record from tikv with the same traceID
	instance1 := "tikv:20180"
	instanceType1 := "tikv"
	parents1 := []*tracepb.RemoteParentSpan{{
		TraceId: traceID,
		SpanId:  11,
	}}
	spans1 := []*tracepb.Span{{
		SpanId:   21,
		ParentId: 0,
	}}
	record1 := &tracepb.Report{RemoteParentSpans: parents1, Spans: spans1}
	err = store.TraceRecord(instance1, instanceType1, time.Now(), record1)
	require.NoError(t, err)

	// get the trace result expected to be with two span groups
	trace, err := store.GetTrace(traceID)
	require.NoError(t, err)

	require.Equal(t, trace.TraceID, traceID)
	require.Equal(t, len(trace.SpanGroups), 2)

	spanGroup0 := trace.SpanGroups[0]
	require.Equal(t, spanGroup0.Instance, instance0)
	require.Equal(t, spanGroup0.InstanceType, instanceType0)
	require.Equal(t, len(spanGroup0.Spans), len(spans0))

	span0 := spanGroup0.Spans[0]
	require.Equal(t, span0.SpanId, span0.SpanId)
	require.Equal(t, span0.ParentId, parents0[0].SpanId)

	spanGroup1 := trace.SpanGroups[1]
	require.Equal(t, spanGroup1.Instance, instance1)
	require.Equal(t, spanGroup1.InstanceType, instanceType1)
	require.Equal(t, len(spanGroup1.Spans), len(spans1))

	span1 := spanGroup1.Spans[0]
	require.Equal(t, span1.SpanId, span1.SpanId)
	require.Equal(t, span1.ParentId, parents1[0].SpanId)
}

type MockStore struct {
	store map[uint64]model.Trace
}

func NewMockStore() *MockStore {
	return &MockStore{
		store: make(map[uint64]model.Trace),
	}
}

var _ Store = &MockStore{}

func (m *MockStore) TraceRecord(instance, instanceType string, createdTime time.Time, record *tracepb.Report) error {
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

		item := m.store[traceID]
		item.TraceID = strconv.FormatUint(traceID, 10)
		spanGroup := model.SpanGroup{
			Instance:     instance,
			InstanceType: instanceType,
			Spans:        spans,
		}
		item.SpanGroups = append(item.SpanGroups, spanGroup)
		m.store[traceID] = item
	}

	return nil
}

func (m *MockStore) Close() {
}

func (m *MockStore) GetTrace(traceID uint64) (*model.Trace, error) {
	trace, ok := m.store[traceID]
	if ok {
		return &trace, nil
	}
	return nil, errors.New("not found")
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
