package batch_store

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/tracing/db"

	"github.com/stretchr/testify/require"
)

func TestWriteDBWorkerBasic(t *testing.T) {
	mockDB := &MockDB{}
	ch := make(chan *db.WriteDBTask, 100)
	ctx, cancel := context.WithCancel(context.Background())
	worker := NewWriteDBWorker(ctx, mockDB, ch)
	worker.Start()
	defer func() {
		cancel()
		worker.Wait()
	}()

	for i := 0; i < 50; i++ {
		ch <- &db.WriteDBTask{}
	}

	time.Sleep(time.Millisecond * 100)

	tasks := mockDB.GetTasks()
	require.Equal(t, 50, len(tasks))
}

func TestWriteDBWorkerWork(t *testing.T) {
	mockDB := &MockDB{}
	ch := make(chan *db.WriteDBTask, 100)
	worker := NewWriteDBWorker(context.Background(), mockDB, ch)

	for i := 0; i < 50; i++ {
		ch <- &db.WriteDBTask{}
	}

	taskBuffer := make([]*db.WriteDBTask, 0, 1024)
	worker.doWork(&taskBuffer)
	require.Equal(t, 0, len(taskBuffer))

	tasks := mockDB.GetTasks()
	require.Equal(t, 50, len(tasks))

	worker.doWork(&taskBuffer)
	require.Equal(t, 0, len(taskBuffer))

	tasks = mockDB.GetTasks()
	require.Equal(t, 0, len(tasks))
}
