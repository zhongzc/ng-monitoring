package tracing

import (
	"errors"

	"github.com/emirpasic/gods/trees/btree"
	"github.com/pingcap/kvproto/pkg/tracepb"
)

type TraceInfo struct {
	Instance     string
	InstanceType string
	RootEvent    string
	Spans        []*tracepb.Span
	BeginUnixNs  uint64
	EndUnixNs    uint64
}

type Traces struct {
	TraceID        uint64
	TraceInfos     []*TraceInfo
	Version        uint64
	MinBeginUnixNs uint64
	MaxEndUnixNs   uint64
}

type StageTable interface {
	// Put adds Spans with associated information to the stage table.
	Put(traceID uint64, traceInfo *TraceInfo, version uint64) error

	// Get returns the trace information of the traceID.
	Get(traceID uint64) (Traces, bool)

	// Commit marks the traceID as needed to be persisted.
	Commit(traceID uint64, version uint64) error

	// GetCommittedID returns traceIDs that need to be persisted but not persisted yet.
	GetCommittedID() []uint64

	// Advance advances the version of the stage table, and returns Spans that need
	// to be persisted. Caller should persist the Spans.
	Advance(version uint64) map[uint64]*Traces
}

var (
	ErrVersionTooOld   = errors.New("version too old")
	ErrSpanCountExceed = errors.New("span count exceeds the limit")
)

type DefaultStageTable struct {
	// maxSpanCount is the maximum number of Spans that can
	// be stored in the stage table. It protects the memory
	// usage of the stage table.
	//
	// When the number of Spans exceeds this number, Put
	// will refuse to put Spans and return error.
	maxSpanCount int
	curSpanCount int

	version uint64

	// traceID -> TraceInfo
	traceMap map[uint64]*Traces
	// version -> [traceID]
	traceVersions *btree.Tree

	// traceID
	committedMap map[uint64]*Traces
	// version -> [traceID]
	committedVersions *btree.Tree
}

func NewStageTable(maxSpanCount int) *DefaultStageTable {
	return &DefaultStageTable{
		maxSpanCount:      maxSpanCount,
		traceMap:          make(map[uint64]*Traces),
		traceVersions:     btree.NewWith(4, traceIDComparator),
		committedMap:      make(map[uint64]*Traces),
		committedVersions: btree.NewWith(4, traceIDComparator),
	}
}

var _ StageTable = &DefaultStageTable{}

func (st *DefaultStageTable) Put(traceID uint64, traceInfo *TraceInfo, version uint64) error {
	if version < st.version {
		return ErrVersionTooOld
	}

	if st.curSpanCount+len(traceInfo.Spans) > st.maxSpanCount {
		return ErrSpanCountExceed
	}

	st.curSpanCount += len(traceInfo.Spans)

	// Case 1: it's a committed trace
	if traces := st.committedMap[traceID]; traces != nil {
		st.appendTraceInfoToTraces(traces, traceInfo)
		return nil
	}

	// Case 2: it's an uncommitted but staged trace
	if traces := st.traceMap[traceID]; traces != nil {
		st.appendTraceInfoToTraces(traces, traceInfo)
		return nil
	}

	// Case 3: it's a new trace to be staged
	traces := &Traces{
		TraceID:        traceID,
		TraceInfos:     []*TraceInfo{traceInfo},
		Version:        version,
		MinBeginUnixNs: traceInfo.BeginUnixNs,
		MaxEndUnixNs:   traceInfo.EndUnixNs,
	}
	st.traceMap[traceID] = traces
	st.appendTraceToVersion(traceID, version, st.traceVersions)
	return nil
}

func (st *DefaultStageTable) Get(traceID uint64) (Traces, bool) {
	traces, found := st.traceMap[traceID]
	if !found {
		traces, found = st.committedMap[traceID]
	}
	if !found {
		return Traces{}, false
	}

	// Copy trace infos to avoid data race
	traceInfos := make([]*TraceInfo, len(traces.TraceInfos))
	copy(traceInfos, traces.TraceInfos)
	res := *traces
	res.TraceInfos = traceInfos
	return res, found
}

func (st *DefaultStageTable) Commit(traceID uint64, version uint64) error {
	if version < st.version {
		return ErrVersionTooOld
	}

	if _, ok := st.committedMap[traceID]; !ok {
		traces := st.traceMap[traceID]
		delete(st.traceMap, traceID)

		if traces == nil {
			traces = &Traces{
				TraceID: traceID,
				Version: version,
			}
		} else {
			st.removeTraceIDFromVersion(traceID, traces.Version, st.traceVersions)
			traces.Version = version
		}
		st.committedMap[traceID] = traces
		st.appendTraceToVersion(traceID, version, st.committedVersions)
	}
	return nil
}

func (st *DefaultStageTable) GetCommittedID() []uint64 {
	res := make([]uint64, 0, len(st.committedMap))
	for traceID := range st.committedMap {
		res = append(res, traceID)
	}
	return res
}

func (st *DefaultStageTable) Advance(version uint64) map[uint64]*Traces {
	if version < st.version {
		return nil
	}

	res := make(map[uint64]*Traces)

	st.version = version
	it := st.committedVersions.Iterator()
	for it.Next() {
		ver := it.Key().(uint64)
		if ver > version {
			break
		}
		st.committedVersions.Remove(ver)

		traceIDs := it.Value().(*[]uint64)
		for _, traceID := range *traceIDs {
			res[traceID] = st.committedMap[traceID]
			st.removeTrace(traceID, st.committedMap)
		}
	}

	it = st.traceVersions.Iterator()
	for it.Next() {
		ver := it.Key().(uint64)
		if ver > version {
			break
		}
		st.traceVersions.Remove(ver)
		traceIDs := it.Value().(*[]uint64)
		for _, traceID := range *traceIDs {
			st.removeTrace(traceID, st.traceMap)
		}
	}

	return res
}

func (st *DefaultStageTable) appendTraceToVersion(traceID, version uint64, tree *btree.Tree) {
	t, found := tree.Get(version)
	if !found {
		t = &[]uint64{}
	}
	traceIDs := t.(*[]uint64)
	*traceIDs = append(*traceIDs, traceID)
	tree.Put(version, traceIDs)
}

func (st *DefaultStageTable) removeTrace(traceID uint64, m map[uint64]*Traces) {
	if traces := m[traceID]; traces != nil {
		for _, traceInfo := range traces.TraceInfos {
			st.curSpanCount -= len(traceInfo.Spans)
		}
	}
	delete(m, traceID)
}

func (st *DefaultStageTable) removeTraceIDFromVersion(traceID uint64, version uint64, versions *btree.Tree) {
	t, _ := versions.Get(version)
	traceIDs := t.(*[]uint64)
	for i, id := range *traceIDs {
		if id == traceID {
			*traceIDs = append((*traceIDs)[:i], (*traceIDs)[i+1:]...)
			break
		}
	}
	versions.Put(version, traceIDs)
}

func (st *DefaultStageTable) appendTraceInfoToTraces(traces *Traces, info *TraceInfo) {
	if info.BeginUnixNs < traces.MinBeginUnixNs {
		traces.MinBeginUnixNs = info.BeginUnixNs
	}
	if info.EndUnixNs > traces.MaxEndUnixNs {
		traces.MaxEndUnixNs = info.EndUnixNs
	}
	traces.TraceInfos = append(traces.TraceInfos, info)
}

func traceIDComparator(a, b interface{}) int {
	aAsserted := a.(uint64)
	bAsserted := b.(uint64)
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}
