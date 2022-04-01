package query

import (
	"testing"

	"github.com/pingcap/ng-monitoring/component/tracing/db"
	"github.com/pingcap/ng-monitoring/component/tracing/model"

	"github.com/pingcap/kvproto/pkg/tracepb"
	"github.com/stretchr/testify/require"
)

func TestDefaultQueryBasic(t *testing.T) {
	mockDB := &db.MockDB{}
	query := NewDefaultQuery(mockDB)
	defer query.Close()

	err := mockDB.Put([]*db.WriteDBTask{{
		TraceID:      20,
		Instance:     "tidb:10080",
		InstanceType: "tidb",
		CreatedTsMs:  1234,
		Spans: []*tracepb.Span{{
			SpanId:      20,
			BeginUnixNs: 5678,
			DurationNs:  8080,
			Event:       "dead",
		}},
	}, {
		TraceID:      20,
		Instance:     "tikv:20180",
		InstanceType: "tikv",
		CreatedTsMs:  4321,
		Spans: []*tracepb.Span{{
			SpanId:      40,
			BeginUnixNs: 8765,
			DurationNs:  7070,
			Event:       "beef",
		}},
	}})
	require.NoError(t, err)

	trace, err := query.Trace(20)
	require.NoError(t, err)

	require.Equal(t, trace.TraceID, uint64(20))
	require.Equal(t, trace.SpanGroups, []model.SpanGroup{{
		Instance:     "tidb:10080",
		InstanceType: "tidb",
		Spans: []*tracepb.Span{{
			SpanId:      20,
			BeginUnixNs: 5678,
			DurationNs:  8080,
			Event:       "dead",
		}},
	}, {
		Instance:     "tikv:20180",
		InstanceType: "tikv",
		Spans: []*tracepb.Span{{
			SpanId:      40,
			BeginUnixNs: 8765,
			DurationNs:  7070,
			Event:       "beef",
		}},
	}})

	trace, err = query.Trace(30)
	require.NoError(t, err)
	require.Equal(t, trace.TraceID, uint64(30))
	require.Equal(t, trace.SpanGroups, []model.SpanGroup{})
}
