package batch_store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteDBWorkerBasic(t *testing.T) {
	db := &MockDB{}
	ch := make(chan *WriteDBTask, 100)
	ctx, cancel := context.WithCancel(context.Background())
	worker := NewWriteDBWorker(ctx, db, ch)
	worker.Start()
	defer func() {
		cancel()
		worker.Wait()
	}()

	for i := 0; i < 50; i++ {
		ch <- &WriteDBTask{}
	}

	time.Sleep(time.Millisecond * 100)

	tasks := db.GetTasks()
	require.Equal(t, 50, len(tasks))
}

func TestWriteDBWorkerWork(t *testing.T) {
	db := &MockDB{}
	ch := make(chan *WriteDBTask, 100)
	worker := NewWriteDBWorker(context.Background(), db, ch)

	for i := 0; i < 50; i++ {
		ch <- &WriteDBTask{}
	}

	taskBuffer := make([]*WriteDBTask, 0, 1024)
	worker.doWork(&taskBuffer)
	require.Equal(t, 0, len(taskBuffer))

	tasks := db.GetTasks()
	require.Equal(t, 50, len(tasks))

	worker.doWork(&taskBuffer)
	require.Equal(t, 0, len(taskBuffer))

	tasks = db.GetTasks()
	require.Equal(t, 0, len(tasks))
}

type MockDB struct {
	sync.Mutex
	tasks []*WriteDBTask
}

var _ DB = &MockDB{}

func (m *MockDB) Write(tasks []*WriteDBTask) error {
	m.Lock()
	m.tasks = append(m.tasks, tasks...)
	m.Unlock()
	return nil
}

func (m *MockDB) GetTasks() []*WriteDBTask {
	m.Lock()
	tasks := m.tasks
	m.tasks = nil
	m.Unlock()
	return tasks
}
