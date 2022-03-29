package query

import (
	"errors"
	"testing"

	"github.com/pingcap/kvproto/pkg/tracepb"
	"github.com/pingcap/ng-monitoring/component/tracing/model"
	"github.com/stretchr/testify/require"
)

func TestQueryBasic(t *testing.T) {
	query := NewMockQuery(map[uint64]model.Trace{
		20: {
			TraceID: 20,
			SpanGroups: []model.SpanGroup{{
				InstanceType: "tidb",
				Instance:     "127.0.0.1:10080",
				Spans: []*tracepb.Span{{
					ParentId:    0,
					SpanId:      10,
					BeginUnixNs: 1648539194000000000,
					DurationNs:  1000000000,
					Event:       "SELECT sleep(1)",
				}, {
					ParentId:    10,
					SpanId:      11,
					BeginUnixNs: 1648539194010000000,
					DurationNs:  1000,
					Event:       "Parse",
				}},
			}},
		},

		30: {
			TraceID: 30,
			SpanGroups: []model.SpanGroup{{
				InstanceType: "tidb",
				Instance:     "127.0.0.1:10081",
				Spans: []*tracepb.Span{{
					ParentId:    0,
					SpanId:      40,
					BeginUnixNs: 1648539196000000000,
					DurationNs:  1000000000,
					Event:       "INSERT INTO t VALUES (1)",
				}, {
					ParentId:    40,
					SpanId:      41,
					BeginUnixNs: 1648539196500000000,
					DurationNs:  1000,
					Event:       "Execute",
				}},
			}, {
				InstanceType: "tikv",
				Instance:     "127.0.0.1:20180",
				Spans: []*tracepb.Span{{
					ParentId:    41,
					SpanId:      42,
					BeginUnixNs: 1648539196000000000,
					DurationNs:  100000,
					Event:       "Prewrite",
				}},
			}},
		},
	})
	defer query.Close()

	trace, err := query.Trace(20)
	require.NoError(t, err)
	require.Equal(t, trace.TraceID, uint64(20))
	require.Equal(t, len(trace.SpanGroups), 1)

	trace, err = query.Trace(30)
	require.NoError(t, err)
	require.Equal(t, trace.TraceID, uint64(30))
	require.Equal(t, len(trace.SpanGroups), 2)

	trace, err = query.Trace(40)
	require.Error(t, err)
}

type MockQuery struct {
	store map[uint64]model.Trace
}

func NewMockQuery(store map[uint64]model.Trace) *MockQuery {
	return &MockQuery{
		store: store,
	}
}

var _ Query = &MockQuery{}

func (m *MockQuery) Trace(traceID uint64) (*model.Trace, error) {
	trace, ok := m.store[traceID]
	if ok {
		return &trace, nil
	}
	return nil, errors.New("not found")
}

func (m *MockQuery) Close() {
}
