package batch_store

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/types"
	"github.com/pingcap/kvproto/pkg/tracepb"
	"github.com/stretchr/testify/require"
)

func initGenji(t *testing.T, path string) *genji.DB {
	opts := badger.DefaultOptions(path).
		WithZSTDCompressionLevel(3).
		WithBlockSize(8 * 1024).
		WithValueThreshold(128 * 1024)

	engine, err := badgerengine.NewEngine(opts)
	require.NoError(t, err)

	db, err := genji.New(context.Background(), engine)
	require.NoError(t, err)

	return db
}

func TestDBGenjiBasic(t *testing.T) {
	path, err := ioutil.TempDir("", "topsql-test")
	require.NoError(t, err)
	defer os.ReadDir(path)

	db := initGenji(t, path)
	defer db.Close()

	storeDB, err := NewDBGenji(db)
	require.NoError(t, err)

	traceID := "20"
	createdTsMs := time.Now().UnixNano() / int64(time.Millisecond)
	instance := "tidb:10080"
	instanceType := "tidb"
	span := &tracepb.Span{
		SpanId:      20,
		ParentId:    0,
		BeginUnixNs: uint64(time.Now().UnixNano()),
		DurationNs:  1000000,
		Event:       "SELECT 1",
	}
	err = storeDB.Write([]*WriteDBTask{{
		TraceID:      traceID,
		Instance:     instance,
		InstanceType: instanceType,
		CreatedTsMs:  createdTsMs,
		Spans:        []*tracepb.Span{span},
	}})
	require.NoError(t, err)

	result, err := db.Query("SELECT * FROM traces WHERE trace_id = '20'")
	require.NoError(t, err)
	defer result.Close()

	err = result.Iterate(func(d types.Document) error {
		value, err := d.GetByField("trace_id")
		if err != nil {
			return err
		}
		require.Equal(t, "20", value.V().(string))

		value, err = d.GetByField("created_ts_ms")
		if err != nil {
			return err
		}
		require.Equal(t, createdTsMs, value.V().(int64))

		value, err = d.GetByField("instance")
		if err != nil {
			return err
		}
		require.Equal(t, instance, value.V().(string))

		value, err = d.GetByField("instance_type")
		if err != nil {
			return err
		}
		require.Equal(t, instanceType, value.V().(string))

		value, err = d.GetByField("spans")
		if err != nil {
			return err
		}
		r := &tracepb.Report{}
		err = r.Unmarshal(value.V().([]byte))
		if err != nil {
			return err
		}
		require.Equal(t, span, r.Spans[0])

		return nil
	})
	require.NoError(t, err)
}
