package tracing

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/tracepb"
	"github.com/stretchr/testify/require"
)

func TestPutGet(t *testing.T) {
	table := NewStageTable(100)

	err := table.Put(1, &TraceInfo{
		Instance: "instance1",
		Spans:    []*tracepb.Span{{SpanId: 1}},
	}, 10)
	require.NoError(t, err)

	err = table.Put(1, &TraceInfo{
		Instance: "instance2",
		Spans:    []*tracepb.Span{{SpanId: 2}},
	}, 11)
	require.NoError(t, err)

	err = table.Put(2, &TraceInfo{
		Instance: "instance3",
		Spans:    []*tracepb.Span{{SpanId: 2}},
	}, 12)
	require.NoError(t, err)

	trace, found := table.Get(1)
	require.True(t, found)
	require.Equal(t, len(trace), 2)
	require.Equal(t, "instance1", trace[0].Instance)
	require.Equal(t, "instance2", trace[1].Instance)

	trace, found = table.Get(2)
	require.True(t, found)
	require.Equal(t, len(trace), 1)
	require.Equal(t, "instance3", trace[0].Instance)

	trace, found = table.Get(3)
	require.False(t, found)
	require.Equal(t, len(trace), 0)
}

func TestCommit(t *testing.T) {
	table := NewStageTable(100)

	traceIDs := table.GetCommittedID()
	require.Equal(t, len(traceIDs), 0)

	// Put first, then commit.
	err := table.Put(1, &TraceInfo{
		Instance: "instance1",
		Spans:    []*tracepb.Span{{SpanId: 1}},
	}, 10)
	require.NoError(t, err)

	err = table.Commit(1, 11)
	require.NoError(t, err)

	traceIDs = table.GetCommittedID()
	require.Equal(t, traceIDs, []uint64{1})

	// Commit first, then put
	err = table.Commit(2, 13)
	require.NoError(t, err)

	err = table.Put(2, &TraceInfo{
		Instance: "instance2",
		Spans:    []*tracepb.Span{{SpanId: 2}},
	}, 12)
	require.NoError(t, err)

	traceIDs = table.GetCommittedID()
	require.Equal(t, traceIDs, []uint64{1, 2})
}

func TestAdvance(t *testing.T) {
	table := NewStageTable(100)

	// Put -> Commit -> Advance
	err := table.Put(1, &TraceInfo{
		Instance: "instance1",
		Spans:    []*tracepb.Span{{SpanId: 1}},
	}, 10)
	require.NoError(t, err)

	_, found := table.Get(1)
	require.True(t, found)

	err = table.Commit(1, 11)
	require.NoError(t, err)

	_, found = table.Get(1)
	require.True(t, found)

	traces := table.Advance(15)
	require.Equal(t, len(traces), 1)
	require.Equal(t, "instance1", traces[1][0].Instance)

	_, found = table.Get(1)
	require.False(t, found)

	traceIDs := table.GetCommittedID()
	require.Equal(t, len(traceIDs), 0)

	err = table.Put(2, &TraceInfo{
		Instance: "instance2",
		Spans:    []*tracepb.Span{{SpanId: 2}},
	}, 12)
	require.Error(t, err)
	require.Equal(t, "version too old", err.Error())

	err = table.Put(2, &TraceInfo{
		Instance: "instance2",
		Spans:    []*tracepb.Span{{SpanId: 2}},
	}, 16)
	require.NoError(t, err)

	_, found = table.Get(2)
	require.True(t, found)

	traces = table.Advance(20)
	require.Equal(t, len(traces), 0)

	_, found = table.Get(2)
	require.False(t, found)

	traceIDs = table.GetCommittedID()
	require.Equal(t, len(traceIDs), 0)
}

func TestMaxSpanCount(t *testing.T) {
	table := NewStageTable(2)

	err := table.Put(1, &TraceInfo{
		Instance: "instance1",
		Spans:    []*tracepb.Span{{SpanId: 1}},
	}, 10)
	require.NoError(t, err)

	err = table.Put(1, &TraceInfo{
		Instance: "instance2",
		Spans:    []*tracepb.Span{{SpanId: 2}},
	}, 11)
	require.NoError(t, err)

	err = table.Put(2, &TraceInfo{
		Instance: "instance3",
		Spans:    []*tracepb.Span{{SpanId: 2}},
	}, 12)
	require.Error(t, err)
	require.Equal(t, "span count exceeds the limit", err.Error())

	trace, found := table.Get(1)
	require.True(t, found)
	require.Equal(t, len(trace), 2)
	require.Equal(t, "instance1", trace[0].Instance)
	require.Equal(t, "instance2", trace[1].Instance)

	trace, found = table.Get(2)
	require.False(t, found)
	require.Equal(t, len(trace), 0)
}
