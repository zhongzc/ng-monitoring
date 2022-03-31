package batch_store

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/tracing/db"

	"github.com/pingcap/kvproto/pkg/tracepb"
	"github.com/stretchr/testify/require"
)

func TestBatchStoreBasic(t *testing.T) {
	mockDB := &db.MockDB{}
	store := NewBatchStore(context.Background(), mockDB)
	defer store.Close()

	instance := "tidb:10080"
	instanceType := "tidb"
	createdTime := time.Now()
	record := &tracepb.Report{
		RemoteParentSpans: []*tracepb.RemoteParentSpan{{
			TraceId: 20,
		}},
		Spans: []*tracepb.Span{{
			SpanId:      30,
			ParentId:    0,
			BeginUnixNs: uint64(createdTime.UnixNano()),
			DurationNs:  100,
			Event:       "SELECT 1",
		}},
	}
	err := store.TraceRecord(instance, instanceType, createdTime, record)
	require.NoError(t, err)

	expectedItem := &db.TraceItem{
		TraceID:      20,
		Instance:     instance,
		InstanceType: instanceType,
		CreatedTsMs:  createdTime.UnixNano() / int64(time.Millisecond),
		Spans:        record.Spans,
	}

	time.Sleep(100 * time.Millisecond)
	items := mockDB.TakeAll()
	require.Equal(t, []*db.TraceItem{expectedItem}, items)
}
