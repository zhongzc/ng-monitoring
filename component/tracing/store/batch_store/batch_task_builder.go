package batch_store

import (
	"context"

	"github.com/pingcap/ng-monitoring/component/tracing/db"
)

type BatchTaskBuilder struct {
	taskChan <-chan *db.WriteDBTask
}

func NewBatchTaskBuilder(taskChan <-chan *db.WriteDBTask) *BatchTaskBuilder {
	return &BatchTaskBuilder{
		taskChan: taskChan,
	}
}

// FetchBatch fetches a batch of tasks from the task channel. The batch of tasks is returned
// by appending to the given slice `fill` for the purpose of saving allocations.
//
// `maxSize` is the maximum number of tasks to receive.
// `ctx` is used to cancel the receiving if a timeout is specified with context.WithTimeout.
func (b *BatchTaskBuilder) FetchBatch(ctx context.Context, maxSize int, fill *[]*db.WriteDBTask) {
	if b == nil || b.taskChan == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case task := <-b.taskChan:
			*fill = append(*fill, task)
			if len(*fill) >= maxSize {
				return
			}
		}
	}
}
