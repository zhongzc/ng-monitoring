package tracing

import (
	"github.com/pingcap/kvproto/pkg/tracepb"
)

type Staging struct {
	table StageTable
}

func (s *Staging) PutRecord(timestamp uint64, instance, instanceType string, report *tracepb.Report) error {
	beginNs, endNs := getBeginEnd(report)

	// A group of Spans from the root trace
	if len(report.RemoteParentSpans) == 1 && report.RemoteParentSpans[0].SpanId == 0 {
		// find the root span of the root trace since its event is treated as the root event
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
			BeginUnixNs:  beginNs,
			EndUnixNs:    endNs,
			RootEvent:    rootEvent,
		}, timestamp)
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
			BeginUnixNs:  beginNs,
			EndUnixNs:    endNs,
		}, timestamp); err != nil {
			return err
		}
	}

	return nil
}

func (s *Staging) NotifyCollect(timestamp uint64, notify *tracepb.NotifyCollect) error {
	return s.table.Commit(notify.TraceId, timestamp)
}

func (s *Staging) StoreCommitted(timestamp uint64) error {
	//return s.table.Advance(timestamp)

	// TODO: take committed trace and store them into database
	return nil
}

func getBeginEnd(report *tracepb.Report) (uint64, uint64) {
	if len(report.Spans) == 0 {
		return 0, 0
	}

	beginNs := report.Spans[0].BeginUnixNs
	endNs := report.Spans[0].DurationNs + beginNs
	for _, span := range report.Spans {
		b := span.BeginUnixNs
		e := span.DurationNs + b
		if b < beginNs {
			beginNs = b
		}
		if e > endNs {
			endNs = e
		}
	}

	return beginNs, endNs
}
