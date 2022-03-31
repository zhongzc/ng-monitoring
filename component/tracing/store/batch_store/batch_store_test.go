package batch_store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/tracing/db"

	"github.com/pingcap/kvproto/pkg/tracepb"
	"github.com/stretchr/testify/require"
)

func TestBatchStoreBasic(t *testing.T) {
	mockDB := &MockDB{}
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

	expectedTask := &db.WriteDBTask{
		TraceID:      "20",
		Instance:     instance,
		InstanceType: instanceType,
		CreatedTsMs:  createdTime.UnixNano() / int64(time.Millisecond),
		Spans:        record.Spans,
	}

	time.Sleep(100 * time.Millisecond)
	tasks := mockDB.GetTasks()
	require.Equal(t, []*db.WriteDBTask{expectedTask}, tasks)
}

type MockDB struct {
	sync.Mutex
	tasks []*db.WriteDBTask
}

var _ db.DB = &MockDB{}

func (m *MockDB) Write(tasks []*db.WriteDBTask) error {
	m.Lock()
	m.tasks = append(m.tasks, tasks...)
	m.Unlock()
	return nil
}

func (m *MockDB) GetTasks() []*db.WriteDBTask {
	m.Lock()
	tasks := m.tasks
	m.tasks = nil
	m.Unlock()
	return tasks
}
