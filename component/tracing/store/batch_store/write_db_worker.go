package batch_store

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/ng-monitoring/component/tracing/db"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	DefaultWriteBatchSize  = 200
	DefaultMaxRecvWaitTime = 10 * time.Millisecond
)

type WriteDBWorker struct {
	ctx context.Context
	wg  *sync.WaitGroup

	batchBuilder *BatchTaskBuilder
	db           db.DB
}

func NewWriteDBWorker(ctx context.Context, db db.DB, taskChan <-chan *db.WriteDBTask) *WriteDBWorker {
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
	go utils.GoWithRecovery(func() {
		defer ww.wg.Done()

		// prepare batch which will be reused
		taskBuffer := make([]*db.WriteDBTask, 0, DefaultWriteBatchSize)
		for {
			select {
			case <-ww.ctx.Done():
				return
			default:
				ww.doWork(&taskBuffer)
			}
		}
	}, nil)
}

func (ww *WriteDBWorker) doWork(taskBuffer *[]*db.WriteDBTask) {
	if ww == nil {
		return
	}

	// fetch task from builder
	ctx, cancel := context.WithTimeout(ww.ctx, DefaultMaxRecvWaitTime)
	defer cancel()

	ww.batchBuilder.FetchBatch(ctx, DefaultWriteBatchSize, taskBuffer)

	// write the batch to db
	if err := ww.db.Put(*taskBuffer); err != nil {
		log.Warn("write records to db failed", zap.Error(err))
	}

	// reset the batch
	*taskBuffer = (*taskBuffer)[:0]
}

func (ww *WriteDBWorker) Wait() {
	if ww != nil {
		ww.wg.Wait()
	}
}
