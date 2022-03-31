package batch_store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatchTaskBuilderBasic(t *testing.T) {
	ch := make(chan *WriteDBTask, 10000)
	builder := NewBatchTaskBuilder(ch)

	for i := 0; i < 100; i++ {
		ch <- &WriteDBTask{}
	}

	var tasks []*WriteDBTask
	builder.FetchBatch(context.Background(), 40, &tasks)
	require.Equal(t, 40, len(tasks))

	tasks = tasks[:0]
	builder.FetchBatch(context.Background(), 10, &tasks)
	require.Equal(t, 10, len(tasks))

	tasks = tasks[:0]
	builder.FetchBatch(context.Background(), 50, &tasks)
	require.Equal(t, 50, len(tasks))
}

func TestBatchTaskBuilderNotEnough(t *testing.T) {
	ch := make(chan *WriteDBTask, 10000)
	builder := NewBatchTaskBuilder(ch)

	var tasks []*WriteDBTask

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	builder.FetchBatch(ctx, 50, &tasks)
	require.Equal(t, 0, len(tasks))

	for i := 0; i < 10; i++ {
		ch <- &WriteDBTask{}
	}

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	builder.FetchBatch(ctx, 50, &tasks)
	require.Equal(t, 10, len(tasks))
}
