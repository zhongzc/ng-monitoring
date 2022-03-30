package model

import "github.com/pingcap/kvproto/pkg/tracepb"

type Trace struct {
	TraceID    string
	SpanGroups []SpanGroup
}

type SpanGroup struct {
	Instance     string
	InstanceType string
	Spans        []*tracepb.Span
}
