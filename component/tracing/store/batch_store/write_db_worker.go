package batch_store

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	DefaultWriteBatchSize  = 100
	DefaultMaxRecvWaitTime = 100 * time.Millisecond
)

type WriteDBWorker struct {
	ctx context.Context
	wg  *sync.WaitGroup

	batchBuilder *BatchTaskBuilder
	db           DB
}

func NewWriteDBWorker(ctx context.Context, db DB, taskChan <-chan *WriteDBTask) *WriteDBWorker {
	return &WriteDBWorker{
		ctx: ctx,
		wg:  &sync.WaitGroup{},

		batchBuilder: NewBatchTaskBuilder(taskChan),
		db:           db,
	}
}

func (ww *WriteDBWorker) Start() {
	if ww == nil {
		return
	}

	ww.wg.Add(1)
	utils.GoWithRecovery(func() {
		defer ww.wg.Done()

		// prepare batch which will be reused
		tasks := make([]*WriteDBTask, 0, DefaultWriteBatchSize)
		for {
			select {
			case <-ww.ctx.Done():
				return
			default:
				// fetch tasks from builder
				ctx, cancel := context.WithTimeout(ww.ctx, DefaultMaxRecvWaitTime)
				ww.batchBuilder.FetchBatch(ctx, DefaultWriteBatchSize, &tasks)

				// write the batch to db
				if err := ww.db.Write(tasks); err != nil {
					log.Warn("write records to db failed", zap.Error(err))
				}

				// reset the batch
				tasks = tasks[:0]
				cancel()
			}
		}
	}, nil)
}

func (ww *WriteDBWorker) Wait() {
	if ww != nil {
		ww.wg.Wait()
	}
}
