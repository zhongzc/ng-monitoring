package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/pingcap/ng_monitoring/utils"

	"github.com/genjidb/genji"
	rsmetering "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/log"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	vminsertHandler http.HandlerFunc
	mWriter *metaWriter

	bytesP         = utils.BytesBufferPool{}
	headerP        = utils.HeaderPool{}
	stringBuilderP = StringBuilderPool{}
	prepareSliceP  = PrepareSlicePool{}
)

func Init(vminsertHandler_ http.HandlerFunc, documentDB *genji.DB) {
	vminsertHandler = vminsertHandler_

	mw, err := newMetaWriter(documentDB)
	if err != nil {
		log.Fatal("failed to create the meta writer", zap.Error(err))
	}
	mWriter = mw
}

func Stop() {
	mWriter.stop()
}

func Instance(instance, instanceType string) error {
	return mWriter.syncWriteInstance(instance, instanceType)
}

func TopSQLRecord(instance, instanceType string, record *tipb.CPUTimeRecord) error {
	m := topSQLProtoToMetric(instance, instanceType, record)
	return writeTimeseriesDB(m)
}

func ResourceMeteringRecord(
	instance, instanceType string,
	record *rsmetering.ResourceUsageRecord,
) error {
	m, err := rsMeteringProtoToMetric(instance, instanceType, record)
	if err != nil {
		return err
	}
	return writeTimeseriesDB(m)
}

func SQLMeta(meta *tipb.SQLMeta) error {
	return mWriter.asyncWriteSQLMeta(meta)
}

func PlanMeta(meta *tipb.PlanMeta) error {
	return mWriter.asyncWritePlanMeta(meta)
}

func buildPrepareStmt(header string, elem string, times int, footer string) string {
	sb := stringBuilderP.Get()
	defer stringBuilderP.Put(sb)

	sb.WriteString(header)
	sb.WriteString(elem)
	for i := 0; i < times-1; i++ {
		sb.WriteString(", ")
		sb.WriteString(elem)
	}
	sb.WriteString(footer)

	return sb.String()
}

// transform tipb.CPUTimeRecord to util.Metric
func topSQLProtoToMetric(
	instance, instanceType string,
	record *tipb.CPUTimeRecord,
) (m Metric) {
	m.Metric.Name = "cpu_time"
	m.Metric.Instance = instance
	m.Metric.InstanceType = instanceType
	m.Metric.SQLDigest = hex.EncodeToString(record.SqlDigest)
	m.Metric.PlanDigest = hex.EncodeToString(record.PlanDigest)

	for i := range record.RecordListCpuTimeMs {
		tsMillis := record.RecordListTimestampSec[i] * 1000
		cpuTime := record.RecordListCpuTimeMs[i]

		m.Timestamps = append(m.Timestamps, tsMillis)
		m.Values = append(m.Values, cpuTime)
	}

	return
}

// transform resource_usage_agent.CPUTimeRecord to util.Metric
func rsMeteringProtoToMetric(
	instance, instanceType string,
	record *rsmetering.ResourceUsageRecord,
) (m Metric, err error) {
	tag := tipb.ResourceGroupTag{}

	m.Metric.Name = "cpu_time"
	m.Metric.Instance = instance
	m.Metric.InstanceType = instanceType

	tag.Reset()
	if err = tag.Unmarshal(record.ResourceGroupTag); err != nil {
		return
	}

	m.Metric.SQLDigest = hex.EncodeToString(tag.SqlDigest)
	m.Metric.PlanDigest = hex.EncodeToString(tag.PlanDigest)

	for i := range record.RecordListCpuTimeMs {
		tsMillis := record.RecordListTimestampSec[i] * 1000
		cpuTime := record.RecordListCpuTimeMs[i]

		m.Timestamps = append(m.Timestamps, tsMillis)
		m.Values = append(m.Values, cpuTime)
	}

	return
}

func writeTimeseriesDB(metric Metric) error {
	bufReq := bytesP.Get()
	bufResp := bytesP.Get()
	header := headerP.Get()

	defer bytesP.Put(bufReq)
	defer bytesP.Put(bufResp)
	defer headerP.Put(header)

	if err := encodeMetric(bufReq, metric); err != nil {
		return err
	}

	respR := utils.NewRespWriter(bufResp, header)
	req, err := http.NewRequest("POST", "/api/v1/import", bufReq)
	if err != nil {
		return err
	}
	vminsertHandler(&respR, req)

	if statusOK := respR.Code >= 200 && respR.Code < 300; !statusOK {
		log.Warn("failed to write timeseries db", zap.String("error", respR.Body.String()))
	}
	return nil
}

func encodeMetric(buf *bytes.Buffer, metric Metric) error {
	encoder := json.NewEncoder(buf)
	return encoder.Encode(metric)
}
