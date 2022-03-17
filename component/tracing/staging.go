package tracing

import (
	"time"

	"github.com/pingcap/kvproto/pkg/tracepb"
)

type Staging struct {
	table StageTable
}

func (s *Staging) PutRecord(instance, instanceType string, report *tracepb.Report) error {
	now := time.Now().Unix()

	// A group of Spans from the root trace
	if len(report.RemoteParentSpans) == 1 && report.RemoteParentSpans[0].SpanId == 0 {
		// find the root span of the root trace whose
		// event is treated as the root event
		rootEvent := ""
		for _, span := range report.Spans {
			if span.ParentId == 0 {
				rootEvent = span.Event
				break
			}
		}
		return s.table.Put(report.RemoteParentSpans[0].TraceId, &TraceInfo{
			Instance:     instance,
			InstanceType: instanceType,
			Spans:        report.Spans,
			RootEvent:    rootEvent,
		}, uint64(now))
	}

	for _, remoteParentSpan := range report.RemoteParentSpans {
		spans := make([]*tracepb.Span, 0, len(report.Spans))
		copy(spans, report.Spans)

		// fill back the parent span id of root spans
		for _, span := range spans {
			if span.ParentId == 0 {
				span.ParentId = remoteParentSpan.SpanId
			}
		}

		if err := s.table.Put(remoteParentSpan.TraceId, &TraceInfo{
			Instance:     instance,
			InstanceType: instanceType,
			Spans:        spans,
		}, uint64(now)); err != nil {
			return err
		}
	}

	return nil
}

func (s *Staging) NotifyCollect(notify *tracepb.NotifyCollect) error {
	now := time.Now().Unix()
	return s.table.Commit(notify.TraceId, uint64(now))
}
