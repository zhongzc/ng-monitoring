package batch_store

import (
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/tracing/db"

	"github.com/pingcap/kvproto/pkg/tracepb"
	"github.com/stretchr/testify/require"
)

func TestTaskTransferBasic(t *testing.T) {
	t.Parallel()

	transfer := NewTaskTransfer(100)
	defer transfer.Close()

	now := time.Now()
	record := &tracepb.Report{
		RemoteParentSpans: []*tracepb.RemoteParentSpan{{
			TraceId: 40,
			SpanId:  10,
		}, {
			TraceId: 42,
			SpanId:  12,
		}},
		Spans: []*tracepb.Span{{
			SpanId:      20,
			ParentId:    0,
			BeginUnixNs: uint64(now.UnixNano()),
			DurationNs:  110,
			Event:       "bar",
		}},
	}

	err := transfer.TraceRecord("tidb:10080", "tidb", now, record)
	require.NoError(t, err)

	expectedTasks := []*db.WriteDBTask{{
		TraceID:      40,
		Instance:     "tidb:10080",
		InstanceType: "tidb",
		CreatedTsMs:  now.UnixNano() / int64(time.Millisecond),
		Spans: []*tracepb.Span{{
			SpanId:      20,
			ParentId:    10,
			BeginUnixNs: uint64(now.UnixNano()),
			DurationNs:  110,
			Event:       "bar",
		}},
	}, {
		TraceID:      42,
		Instance:     "tidb:10080",
		InstanceType: "tidb",
		CreatedTsMs:  now.UnixNano() / int64(time.Millisecond),
		Spans: []*tracepb.Span{{
			SpanId:      20,
			ParentId:    12,
			BeginUnixNs: uint64(now.UnixNano()),
			DurationNs:  110,
			Event:       "bar",
		}},
	}}

	require.Equal(t, expectedTasks, receiveAll(transfer.Receiver()))
}

func TestTaskTransferFull(t *testing.T) {
	t.Parallel()

	transfer := NewTaskTransfer(4)
	defer transfer.Close()

	now := time.Now()
	record := &tracepb.Report{
		RemoteParentSpans: []*tracepb.RemoteParentSpan{{
			TraceId: 42,
			SpanId:  12,
		}},
		Spans: []*tracepb.Span{{
			SpanId:      20,
			ParentId:    0,
			BeginUnixNs: uint64(now.UnixNano()),
			DurationNs:  110,
			Event:       "bar",
		}},
	}

	// send 4 records
	for i := 0; i < 4; i++ {
		err := transfer.TraceRecord("tidb:10080", "tidb", now, record)
		require.NoError(t, err)
	}

	// full
	err := transfer.TraceRecord("tidb:10080", "tidb", now, record)
	require.Error(t, err)

	// receive one
	_ = <-transfer.Receiver()

	// not full
	err = transfer.TraceRecord("tidb:10080", "tidb", now, record)
	require.NoError(t, err)

	// receive all
	tasks := receiveAll(transfer.Receiver())
	require.Equal(t, 4, len(tasks))

	// send 4 records
	for i := 0; i < 4; i++ {
		err := transfer.TraceRecord("tidb:10080", "tidb", now, record)
		require.NoError(t, err)
	}

	// receive all
	tasks = receiveAll(transfer.Receiver())
	require.Equal(t, 4, len(tasks))
}

func receiveAll(rx <-chan *db.WriteDBTask) []*db.WriteDBTask {
	var tasks []*db.WriteDBTask
	for {
		select {
		case task := <-rx:
			tasks = append(tasks, task)
		default:
			return tasks
		}
	}
}
