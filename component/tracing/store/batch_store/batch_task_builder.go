package batch_store

import "context"

type BatchTaskBuilder struct {
	taskChan <-chan *WriteDBTask
}

func NewBatchTaskBuilder(taskChan <-chan *WriteDBTask) *BatchTaskBuilder {
	return &BatchTaskBuilder{
		taskChan: taskChan,
	}
}

// FetchBatch fetches a batch of tasks from the task channel. The batch of tasks is returned
// by appending to the given slice `fill` for the purpose of saving allocations.
//
// `maxSize` is the maximum number of tasks to receive.
// `ctx` is used to cancel the receiving if a timeout is specified with context.WithTimeout.
func (b *BatchTaskBuilder) FetchBatch(ctx context.Context, maxSize int, fill *[]*WriteDBTask) {
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
