package batch_store

import (
	"context"
	"errors"
	"time"

	"github.com/pingcap/ng-monitoring/component/tracing/db"
	"github.com/pingcap/ng-monitoring/component/tracing/store"

	"github.com/pingcap/kvproto/pkg/tracepb"
)

var (
	DefaultWriteTaskChannelSize = 10240
)

type BatchStore struct {
	ctx    context.Context
	cancel context.CancelFunc

	taskTransfer  *TaskTransfer
	writeDBWorker *WriteDBWorker
}

func NewBatchStore(ctx context.Context, db db.DB) *BatchStore {
	ctx, cancel := context.WithCancel(ctx)

	taskTransfer := NewTaskTransfer(DefaultWriteTaskChannelSize)
	writeDBWorker := NewWriteDBWorker(ctx, db, taskTransfer.Receiver())
	writeDBWorker.Start()

	return &BatchStore{
		ctx:    ctx,
		cancel: cancel,

		taskTransfer:  taskTransfer,
		writeDBWorker: writeDBWorker,
	}
}

var _ store.Store = &BatchStore{}

func (ds *BatchStore) TraceRecord(instance, instanceType string, createdTime time.Time, record *tracepb.Report) error {
	if ds == nil {
		return errors.New("batch store is nil")
	}
	return ds.taskTransfer.TraceRecord(instance, instanceType, createdTime, record)
}

func (ds *BatchStore) Close() {
	if ds == nil {
		return
	}

	ds.cancel()
	ds.writeDBWorker.Wait()
	ds.taskTransfer.Close()
}
