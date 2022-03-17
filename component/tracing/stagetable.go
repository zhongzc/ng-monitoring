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
}

type StageTable interface {
	// Put adds Spans with associated information to the stage table.
	Put(traceID uint64, traceInfo *TraceInfo, version uint64) error

	// Get returns the trace information of the traceID.
	Get(traceID uint64) ([]*TraceInfo, bool)

	// Commit marks the traceID as needed to be persisted.
	Commit(traceID uint64, version uint64) error

	// GetCommittedID returns traceIDs that need to be persisted but not persisted yet.
	GetCommittedID() []uint64

	// Advance advances the version of the stage table, and returns Spans that need
	// to be persisted. Caller should persist the Spans.
	Advance(version uint64) map[uint64][]*TraceInfo
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
	traceMap map[uint64][]*TraceInfo
	// version -> [traceID]
	traceVersions *btree.Tree

	// traceID
	committedMap map[uint64]struct{}
	// version -> [traceID]
	committedVersions *btree.Tree
}

func NewStageTable(maxSpanCount int) *DefaultStageTable {
	return &DefaultStageTable{
		maxSpanCount:      maxSpanCount,
		traceMap:          make(map[uint64][]*TraceInfo),
		traceVersions:     btree.NewWith(4, traceIDComparator),
		committedMap:      make(map[uint64]struct{}),
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

	traces, ok := st.traceMap[traceID]
	st.traceMap[traceID] = append(traces, traceInfo)
	if !ok {
		t, found := st.traceVersions.Get(version)
		if !found {
			t = &[]uint64{}
		}
		traceIDs := t.(*[]uint64)
		*traceIDs = append(*traceIDs, traceID)
		st.traceVersions.Put(version, traceIDs)
	}

	return nil
}

func (st *DefaultStageTable) Get(traceID uint64) ([]*TraceInfo, bool) {
	traces, found := st.traceMap[traceID]
	res := make([]*TraceInfo, len(traces))
	copy(res, traces)
	return res, found
}

func (st *DefaultStageTable) Commit(traceID uint64, version uint64) error {
	if version < st.version {
		return ErrVersionTooOld
	}

	if _, ok := st.committedMap[traceID]; !ok {
		st.committedMap[traceID] = struct{}{}

		t, found := st.committedVersions.Get(version)
		if !found {
			t = &[]uint64{}
		}
		traceIDs := t.(*[]uint64)
		*traceIDs = append(*traceIDs, traceID)
		st.committedVersions.Put(version, traceIDs)
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

func (st *DefaultStageTable) Advance(version uint64) map[uint64][]*TraceInfo {
	if version < st.version {
		return nil
	}

	res := make(map[uint64][]*TraceInfo)

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
			delete(st.committedMap, traceID)

			if traces, ok := st.traceMap[traceID]; ok {
				for _, traceInfo := range traces {
					st.curSpanCount -= len(traceInfo.Spans)
				}
				delete(st.traceMap, traceID)
				res[traceID] = traces
			}
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
			delete(st.traceMap, traceID)
		}
	}

	return res
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
