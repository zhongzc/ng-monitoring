package tracing

import (
	"sort"
	"testing"

	"github.com/pingcap/kvproto/pkg/tracepb"
	"github.com/stretchr/testify/require"
)

func TestPutGet(t *testing.T) {
	table := NewStageTable(100)

	err := table.Put(1, &TraceInfo{
		Instance:    "instance1",
		Spans:       []*tracepb.Span{{SpanId: 1}},
		BeginUnixNs: 100,
		EndUnixNs:   200,
	}, 10)
	require.NoError(t, err)

	err = table.Put(1, &TraceInfo{
		Instance:    "instance2",
		Spans:       []*tracepb.Span{{SpanId: 2}},
		BeginUnixNs: 200,
		EndUnixNs:   300,
	}, 11)
	require.NoError(t, err)

	err = table.Put(2, &TraceInfo{
		Instance:    "instance3",
		Spans:       []*tracepb.Span{{SpanId: 2}},
		BeginUnixNs: 300,
		EndUnixNs:   400,
	}, 12)
	require.NoError(t, err)

	traces, found := table.Get(1)
	require.True(t, found)
	require.Equal(t, len(traces.TraceInfos), 2)
	require.Equal(t, traces.MinBeginUnixNs, uint64(100))
	require.Equal(t, traces.MaxEndUnixNs, uint64(300))
	require.Equal(t, traces.TraceInfos[0].Instance, "instance1")
	require.Equal(t, traces.TraceInfos[1].Instance, "instance2")

	traces, found = table.Get(2)
	require.True(t, found)
	require.Equal(t, len(traces.TraceInfos), 1)
	require.Equal(t, traces.MinBeginUnixNs, uint64(300))
	require.Equal(t, traces.MaxEndUnixNs, uint64(400))
	require.Equal(t, traces.TraceInfos[0].Instance, "instance3")

	traces, found = table.Get(3)
	require.False(t, found)
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
	sort.Slice(traceIDs, func(i, j int) bool {
		return traceIDs[i] < traceIDs[j]
	})
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
	require.Equal(t, "instance1", traces[1].TraceInfos[0].Instance)

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
		Spans:    []*tracepb.Span{{SpanId: 3}},
	}, 12)
	require.Error(t, err)
	require.Equal(t, "span count exceeds the limit", err.Error())

	traces, found := table.Get(1)
	require.True(t, found)
	require.Equal(t, len(traces.TraceInfos), 2)
	require.Equal(t, "instance1", traces.TraceInfos[0].Instance)
	require.Equal(t, "instance2", traces.TraceInfos[1].Instance)

	traces, found = table.Get(2)
	require.False(t, found)

	err = table.Commit(1, 12)
	require.NoError(t, err)

	table.Advance(13)

	err = table.Put(3, &TraceInfo{
		Instance: "instance4",
		Spans:    []*tracepb.Span{{SpanId: 4}},
	}, 14)
	require.NoError(t, err)
}
