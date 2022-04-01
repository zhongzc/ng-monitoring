package batch_store

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/tracing/db"

	"github.com/stretchr/testify/require"
)

func TestBatchTaskBuilderBasic(t *testing.T) {
	t.Parallel()

	ch := make(chan *db.WriteDBTask, 10000)
	builder := NewBatchTaskBuilder(ch)

	for i := 0; i < 100; i++ {
		ch <- &db.WriteDBTask{}
	}

	var tasks []*db.WriteDBTask
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
	t.Parallel()

	ch := make(chan *db.WriteDBTask, 10000)
	builder := NewBatchTaskBuilder(ch)

	var tasks []*db.WriteDBTask

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	builder.FetchBatch(ctx, 50, &tasks)
	require.Equal(t, 0, len(tasks))

	for i := 0; i < 10; i++ {
		ch <- &db.WriteDBTask{}
	}

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	builder.FetchBatch(ctx, 50, &tasks)
	require.Equal(t, 10, len(tasks))
}
