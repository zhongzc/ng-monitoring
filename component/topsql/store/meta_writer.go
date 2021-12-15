package store

import (
    "encoding/hex"
    "errors"
    "sync"

    "github.com/pingcap/ng_monitoring/utils"

    "github.com/genjidb/genji"
    "github.com/hashicorp/golang-lru/simplelru"
    "github.com/pingcap/log"
    "github.com/pingcap/tipb/go-tipb"
    "go.uber.org/zap"
)

type metaWriter struct {
    db *genji.DB

    sqlMetaCh  chan *tipb.SQLMeta
    planMetaCh chan *tipb.PlanMeta

    mu sync.Mutex
    instanceCache *simplelru.LRU

    stopCh     chan struct{}
    waitStopCh chan struct{}
}

func newMetaWriter(documentDB *genji.DB) (*metaWriter, error) {
    stopCh := make(chan struct{})
    waitStopCh := make(chan struct{})

    sqlMetaCh := make(chan *tipb.SQLMeta, 100000)
    planMetaCh := make(chan *tipb.PlanMeta, 100000)

    instanceCache, _ := simplelru.NewLRU(100, nil)

    mw := &metaWriter{
        db: documentDB,

        sqlMetaCh:  sqlMetaCh,
        planMetaCh: planMetaCh,

        instanceCache: instanceCache,

        stopCh:     stopCh,
        waitStopCh: waitStopCh,
    }
    if err := mw.initTables(); err != nil {
        return nil, err
    }

    go utils.GoWithRecovery(mw.run, nil)
    return mw, nil
}

func (mw *metaWriter) run() {
    <-mw.stopCh

    sqlMetaCache, _ := simplelru.NewLRU(10000, nil)


    mw.waitStopCh <- struct{}{}
}

func (mw *metaWriter) stop() {
    mw.stopCh <- struct{}{}
    <-mw.waitStopCh
}

func (mw *metaWriter) asyncWriteSQLMeta(meta *tipb.SQLMeta) error {
    select {
    case mw.sqlMetaCh <- meta:
        return nil
    default:
        return errors.New("SQL meta channel is full")
    }

    //prepareStmt := "INSERT INTO sql_digest(digest, sql_text, is_internal) VALUES (?, ?, ?) ON CONFLICT DO NOTHING"
    //prepare, err := mw.db.Prepare(prepareStmt)
    //if err != nil {
    //    return err
    //}
    //
    //return prepare.Exec(hex.EncodeToString(meta.SqlDigest), meta.NormalizedSql, meta.IsInternalSql)
}

func (mw *metaWriter) asyncWritePlanMeta(meta *tipb.PlanMeta) error {
    select {
    case mw.planMetaCh <- meta:
        return nil
    default:
        return errors.New("plan meta channel is full")
    }

    //prepareStmt := "INSERT INTO plan_digest(digest, plan_text) VALUES (?, ?) ON CONFLICT DO NOTHING"
    //prepare, err := mw.db.Prepare(prepareStmt)
    //if err != nil {
    //    return err
    //}
    //
    //return prepare.Exec(hex.EncodeToString(meta.PlanDigest), meta.NormalizedPlan)
}

func (mw *metaWriter) syncWriteInstance(instance, instanceType string) error {
    mw.mu.Lock()
    defer mw.mu.Unlock()

    if _, ok := mw.instanceCache.Get(instance); ok {
        return nil
    }

    prepareStmt := "INSERT INTO instance(instance, instance_type) VALUES (?, ?) ON CONFLICT DO NOTHING"
    prepare, err := mw.db.Prepare(prepareStmt)
    if err != nil {
        return err
    }

    if err = prepare.Exec(instance, instanceType); err != nil {
        return err
    }

    mw.instanceCache.Add(instance, nil)
    return nil
}

func insert(
    header string,          // INSERT INTO {table}({fields}...) VALUES
    elem string, times int, // (?, ?, ... , ?), (?, ?, ... , ?), ... (?, ?, ... , ?)
    footer string,          // ON CONFLICT DO NOTHING
    fill func(target *[]interface{}),
) error {
    if times == 0 {
        log.Fatal("unexpected zero times", zap.Int("times", times))
    }

    prepareStmt := buildPrepareStmt(header, elem, times, footer)
    return execStmt(prepareStmt, fill)
}

func execStmt(prepareStmt string, fill func(target *[]interface{})) error {
    stmt, err := documentDB.Prepare(prepareStmt)
    if err != nil {
        return err
    }

    ps := prepareSliceP.Get()
    defer prepareSliceP.Put(ps)

    fill(ps)
    return stmt.Exec(*ps...)
}

func (mw *metaWriter) initTables() error {
    createTableStmts := []string{
        "CREATE TABLE IF NOT EXISTS sql_digest (digest VARCHAR(255) PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS plan_digest (digest VARCHAR(255) PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS instance (instance VARCHAR(255) PRIMARY KEY)",
    }

    for _, stmt := range createTableStmts {
        if err := mw.db.Exec(stmt); err != nil {
            return err
        }
    }

    return nil
}
