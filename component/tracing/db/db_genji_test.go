package db

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/pingcap/kvproto/pkg/tracepb"
	"github.com/stretchr/testify/require"
)

func initGenji(t require.TestingT, path string) *genji.DB {
	opts := badger.DefaultOptions(path).
		WithZSTDCompressionLevel(3).
		WithBlockSize(8 * 1024).
		WithValueThreshold(128 * 1024).
		WithLogger(nil)

	engine, err := badgerengine.NewEngine(opts)
	require.NoError(t, err)

	db, err := genji.New(context.Background(), engine)
	require.NoError(t, err)

	return db
}

func TestDBGenjiBasic(t *testing.T) {
	t.Parallel()

	path, err := ioutil.TempDir("", "test-db-genji-basic-*")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	db := initGenji(t, path)
	defer db.Close()

	storeDB, err := NewDBGenji(db)
	require.NoError(t, err)

	tasks := []*WriteDBTask{{
		TraceID:      20,
		CreatedTsMs:  time.Now().UnixNano() / int64(time.Millisecond),
		Instance:     "tidb:10080",
		InstanceType: "tidb",
		Spans: []*tracepb.Span{{
			SpanId:      20,
			ParentId:    0,
			BeginUnixNs: uint64(time.Now().UnixNano()),
			DurationNs:  1000000,
			Event:       "SELECT 1",
		}},
	}, {
		TraceID:      20,
		CreatedTsMs:  time.Now().UnixNano() / int64(time.Millisecond),
		Instance:     "tikv:10080",
		InstanceType: "tikv",
		Spans: []*tracepb.Span{{
			SpanId:      30,
			ParentId:    0,
			BeginUnixNs: uint64(time.Now().UnixNano()),
			DurationNs:  10000,
			Event:       "kv_get",
		}},
	}}

	for _, task := range tasks {
		err = storeDB.Put([]*WriteDBTask{task})
		require.NoError(t, err)
	}

	var items []*TraceItem
	err = storeDB.Get(20, &items)
	require.NoError(t, err)

	for i, task := range tasks {
		expectedItem := TraceItem(*task)
		require.Equal(t, &expectedItem, items[i])
	}
}

// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/ng-monitoring/component/tracing/store/batch_store
// cpu: AMD Ryzen 7 4800U with Radeon Graphics
// BenchmarkDBGenjiWrite
// BenchmarkDBGenjiWrite/task_100_span_10
// BenchmarkDBGenjiWrite/task_100_span_10-16         	     434	   2973145 ns/op
// BenchmarkDBGenjiWrite/task_200_span_10
// BenchmarkDBGenjiWrite/task_200_span_10-16         	     222	   5210435 ns/op
// BenchmarkDBGenjiWrite/task_500_span_10
// BenchmarkDBGenjiWrite/task_500_span_10-16         	      98	  11997514 ns/op
// BenchmarkDBGenjiWrite/task_100_span_50
// BenchmarkDBGenjiWrite/task_100_span_50-16         	     294	   3979580 ns/op
// BenchmarkDBGenjiWrite/task_200_span_50
// BenchmarkDBGenjiWrite/task_200_span_50-16         	     151	   7250883 ns/op
// BenchmarkDBGenjiWrite/task_500_span_50
// BenchmarkDBGenjiWrite/task_500_span_50-16         	      70	  18163965 ns/op
// BenchmarkDBGenjiWrite/task_100_span_100
// BenchmarkDBGenjiWrite/task_100_span_100-16        	     253	   4981867 ns/op
// BenchmarkDBGenjiWrite/task_200_span_100
// BenchmarkDBGenjiWrite/task_200_span_100-16        	     121	   9305700 ns/op
// BenchmarkDBGenjiWrite/task_500_span_100
// BenchmarkDBGenjiWrite/task_500_span_100-16        	      48	  23215153 ns/op
func BenchmarkDBGenjiWrite(b *testing.B) {
	taskCounts := []int{100, 200, 500}
	spanCountPerTasks := []int{10, 50, 100}

	for _, spanCountPerTask := range spanCountPerTasks {
		for _, taskCount := range taskCounts {
			b.Run(fmt.Sprintf("task_%d_span_%d", taskCount, spanCountPerTask), func(b *testing.B) {
				path, err := ioutil.TempDir("", "bench-db-genji-write-*")
				require.NoError(b, err)
				defer os.RemoveAll(path)

				db := initGenji(b, path)
				defer db.Close()

				storeDB, err := NewDBGenji(db)
				require.NoError(b, err)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					tasks := genWriteTasks(taskCount, spanCountPerTask)
					b.StartTimer()
					err = storeDB.Put(tasks)
					require.NoError(b, err)
				}
			})
		}
	}
}

func genWriteTasks(taskCount, spanCountPerTask int) []*WriteDBTask {
	var tasks []*WriteDBTask
	for i := 0; i < taskCount; i++ {
		var spans []*tracepb.Span
		for j := 0; j < spanCountPerTask; j++ {
			spans = append(spans, &tracepb.Span{
				SpanId:      rand.Uint64(),
				ParentId:    rand.Uint64(),
				BeginUnixNs: rand.Uint64(),
				DurationNs:  rand.Uint64(),
				Event:       randStringRunes(20),
			})
		}

		tasks = append(tasks, &WriteDBTask{
			TraceID:      rand.Uint64(),
			CreatedTsMs:  rand.Int63(),
			Instance:     randStringRunes(16),
			InstanceType: randStringRunes(5),
			Spans:        spans,
		})
	}
	return tasks
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
