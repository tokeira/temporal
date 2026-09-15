package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokeiraHTTPServiceMetricBridgePreservesDimensionsAndDelta(t *testing.T) {
	line := `tokeira_edge_http_service_requests_total{operation="/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",namespace="test-ns"} 4`
	name, labels, value, ok := parsePromCounterLine(line)
	require.True(t, ok)
	require.Equal(t, "http_service_requests", tokeiraMetricRename[name])
	require.Equal(t, "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo", labels["operation"])
	require.Equal(t, "test-ns", labels["namespace"])

	key := seriesKey(name, labels)
	namespaces := newConformanceNamespaceSet()
	namespaces.add("test-ns")
	baseline := map[string]scrapedCounter{key: {
		temporalName: tokeiraMetricRename[name],
		labels:       labels,
		value:        value - 1,
	}}
	final := map[string]scrapedCounter{key: {
		temporalName: tokeiraMetricRename[name],
		labels:       labels,
		value:        value,
	}}
	recordings := synthesizeDelta(baseline, final, namespaces)["http_service_requests"]
	require.Len(t, recordings, 1)
	require.Equal(t, int64(1), recordings[0].Value)
	require.Equal(t, labels, recordings[0].Tags)
}
