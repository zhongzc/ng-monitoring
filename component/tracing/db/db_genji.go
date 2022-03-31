package db

import (
	"strings"

	"github.com/pingcap/ng-monitoring/utils"

	"github.com/genjidb/genji"
	"github.com/pingcap/kvproto/pkg/tracepb"
)

var (
	prepareSliceP utils.PrepareSlicePool
)

type DBGenji struct {
	db *genji.DB
}

func NewDBGenji(db *genji.DB) (*DBGenji, error) {
	if err := initTables(db); err != nil {
		return nil, err
	}
	return &DBGenji{db: db}, nil
}

var _ DB = &DBGenji{}

func (db *DBGenji) Write(tasks []*WriteDBTask) error {
	if len(tasks) == 0 {
		return nil
	}

	stmt := prepareInsertStmt(len(tasks))
	preparedStmt, err := db.db.Prepare(stmt)
	if err != nil {
		return err
	}

	args := prepareSliceP.Get()
	defer prepareSliceP.Put(args)

	for _, task := range tasks {
		*args = append(*args, task.TraceID)
		*args = append(*args, task.CreatedTsMs)
		*args = append(*args, task.Instance)
		*args = append(*args, task.InstanceType)

		pbStruct := tracepb.Report{Spans: task.Spans}
		bytes, err := pbStruct.Marshal()
		if err != nil {
			return err
		}
		*args = append(*args, bytes)
	}

	return preparedStmt.Exec(*args...)
}

func initTables(documentDB *genji.DB) error {
	createTableStmts := []string{
		"CREATE TABLE IF NOT EXISTS traces (trace_id TEXT NOT NULL, created_ts_ms INT NOT NULL)",
		"CREATE INDEX IF NOT EXISTS traces_trace_id ON traces (trace_id)",
		"CREATE INDEX IF NOT EXISTS traces_created_ts_idx ON traces (created_ts)",
	}

	for _, stmt := range createTableStmts {
		if err := documentDB.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func prepareInsertStmt(count int) string {
	var builder strings.Builder
	builder.WriteString("INSERT INTO traces (trace_id, created_ts_ms, instance, instance_type, spans) VALUES ")
	for i := 0; i < count; i++ {
		builder.WriteString("(?, ?, ?, ?, ?)")
		if i != count-1 {
			builder.WriteByte(',')
		}
	}
	return builder.String()
}
