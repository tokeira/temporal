package testcore

// Tokeira Tier-2 conformance: scrape-backed metrics bridge.
//
// Under Shape-2 the backend `tokeirad` runs out-of-process, so the corpus's in-process
// CaptureMetricsHandler taps nothing (a separate process cannot share the in-memory metric
// registry the capture handler observes). This bridge closes that gap WITHOUT faking
// values: it scrapes the metrics tokeirad genuinely emits on its Prometheus /metrics
// endpoint and replays them into the capture surface under Temporal's metric names.
//
// It is delta-based and honest. A metricstest source factory (installed via
// NewCaptureHandlerWithSource in the conformance cluster) is invoked at StartCapture, where
// it records tokeirad's cumulative counter baseline; the func it returns is invoked at
// Snapshot, where it scrapes again and synthesizes one CapturedRecording per genuine
// counter increment that occurred during the capture window, tagged with the exact labels
// tokeira emitted. Only the metric NAME is translated (tokeira-native -> Temporal); label
// keys and values pass through verbatim. If a scrape fails or a metric was never emitted,
// the window's delta is zero and nothing is synthesized — the test then sees an empty
// snapshot and fails honestly, rather than being handed a fabricated value.

import (
	"bufio"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/server/common/metrics/metricstest"
)

// tokeiraMetricsAddrEnv carries the tokeirad Prometheus /metrics host:port from the harness
// to the conformance cluster, paired with TOKEIRA_CONFORMANCE_FRONTEND_ADDR.
const tokeiraMetricsAddrEnv = "TOKEIRA_CONFORMANCE_METRICS_ADDR"

// tokeiraMetricRename maps a tokeira-native Prometheus counter name to the Temporal metric
// name the functional corpus reads via snap["<temporal name>"]. Only counters the corpus
// asserts on are mapped; latency histograms are intentionally omitted (no functional test
// asserts them, and faithful Prometheus-bucket -> capture replay is out of scope).
var tokeiraMetricRename = map[string]string{
	"tokeira_runtime_nexus_outbound_requests_total":                 "nexus_outbound_requests",
	"tokeira_edge_nexus_completion_requests_total":                  "nexus_completion_requests",
	"tokeira_edge_nexus_completion_request_preprocess_errors_total": "nexus_completion_request_preprocess_errors",
	"tokeira_edge_nexus_task_requests_total":                        "nexus_task_requests",
	// Speculative workflow task outcome counters (spec speculative-wft M.1/M.2).
	// commits/rollbacks are read count-only; the timer-task counters carry an
	// "operation" label = TimerActiveTaskSpeculativeWorkflowTaskTimeout that the
	// corpus filters on, preserved verbatim through the label round-trip.
	"tokeira_runtime_speculative_workflow_task_commits_total":   "speculative_workflow_task_commits",
	"tokeira_runtime_speculative_workflow_task_rollbacks_total": "speculative_workflow_task_rollbacks",
	"tokeira_runtime_speculative_timer_task_requests_total":     "task_requests",
	"tokeira_runtime_speculative_start_to_close_timeout_total":  "start_to_close_timeout",
}

// scrapeTimeout bounds a single /metrics fetch; a slow or dead endpoint yields an empty
// scrape (zero delta) rather than blocking the capture's Snapshot.
const scrapeTimeout = 3 * time.Second

// scrapedCounter is one Prometheus counter time series at scrape time.
type scrapedCounter struct {
	temporalName string            // the renamed (Temporal) metric name
	labels       map[string]string // label key -> value, verbatim from tokeira
	value        float64           // cumulative counter value
}

// newTokeiraMetricsScrapeSource returns a metricstest source factory bridging an
// out-of-process tokeirad's Prometheus /metrics into the corpus capture surface. The
// factory runs at StartCapture (captures the cumulative baseline) and returns two funcs:
//
//   - onStop (StopCapture): freezes the final scrape, bounding the window to
//     [StartCapture, StopCapture] — the same window in-process recording covers, since the
//     handler stops capturing after StopCapture. This matters for tests that StopCapture
//     before Snapshot while the server keeps working (e.g. an operation that retries after
//     the capture is stopped): without freezing, the Snapshot-time scrape would over-count.
//   - onSnapshot (Snapshot): freezes too if StopCapture has not run yet (tests that
//     Snapshot before a deferred StopCapture), then diffs baseline->frozen and synthesizes
//     one recording per genuine counter increment in the window.
//
// The final scrape is taken exactly once, at whichever of StopCapture/Snapshot fires first.
func newTokeiraMetricsScrapeSource(metricsURL string, namespaces *conformanceNamespaceSet) func() (func(), func() metricstest.CaptureSnapshot) {
	client := &http.Client{Timeout: scrapeTimeout}
	return func() (func(), func() metricstest.CaptureSnapshot) {
		baseline := scrapeRenamedCounters(client, metricsURL)
		var frozen map[string]scrapedCounter // nil until the window is frozen
		freeze := func() {
			if frozen == nil {
				frozen = scrapeRenamedCounters(client, metricsURL)
			}
		}
		onStop := func() { freeze() }
		onSnapshot := func() metricstest.CaptureSnapshot {
			freeze()
			return synthesizeDelta(baseline, frozen, namespaces)
		}
		return onStop, onSnapshot
	}
}

// scrapeRenamedCounters GETs the Prometheus exposition and returns the renamed counter
// series keyed by (name + sorted labels). Best-effort: any error returns an empty map, so
// the delta is taken against a zero baseline/final and no value is invented.
func scrapeRenamedCounters(client *http.Client, metricsURL string) map[string]scrapedCounter {
	out := map[string]scrapedCounter{}
	resp, err := client.Get(metricsURL)
	if err != nil {
		return out
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return out
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, labels, value, ok := parsePromCounterLine(line)
		if !ok {
			continue
		}
		temporalName, mapped := tokeiraMetricRename[name]
		if !mapped {
			continue
		}
		sc := scrapedCounter{temporalName: temporalName, labels: labels, value: value}
		out[seriesKey(name, labels)] = sc
	}
	return out
}

// parsePromCounterLine parses a single Prometheus exposition sample line of the form
// `name{k="v",...} value [timestamp]` (or `name value`). Returns the metric name, label
// map, value, and ok=false if the line is not a parseable sample.
func parsePromCounterLine(line string) (name string, labels map[string]string, value float64, ok bool) {
	labels = map[string]string{}
	var rest string
	if i := strings.IndexByte(line, '{'); i >= 0 {
		name = line[:i]
		j := strings.IndexByte(line, '}')
		if j < 0 || j < i {
			return "", nil, 0, false
		}
		labels = parsePromLabels(line[i+1 : j])
		rest = strings.TrimSpace(line[j+1:])
	} else {
		i := strings.IndexByte(line, ' ')
		if i < 0 {
			return "", nil, 0, false
		}
		name = line[:i]
		rest = strings.TrimSpace(line[i:])
	}
	// rest is `value [timestamp]`; take the first field as the value.
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "", nil, 0, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return "", nil, 0, false
	}
	return name, labels, v, true
}

// parsePromLabels parses `k1="v1",k2="v2"` into a map, honoring the Prometheus value
// escapes (\\ \" \n). Label values for tokeira's Nexus metrics are simple, but the escapes
// are handled so any value (e.g. an outcome containing a quote) round-trips.
func parsePromLabels(s string) map[string]string {
	labels := map[string]string{}
	s = strings.TrimSpace(s)
	for len(s) > 0 {
		eq := strings.IndexByte(s, '=')
		if eq < 0 {
			break
		}
		key := strings.TrimSpace(s[:eq])
		s = strings.TrimSpace(s[eq+1:])
		if len(s) == 0 || s[0] != '"' {
			break
		}
		// Scan the quoted value, honoring backslash escapes.
		var b strings.Builder
		i := 1
		for i < len(s) {
			c := s[i]
			if c == '\\' && i+1 < len(s) {
				switch s[i+1] {
				case 'n':
					b.WriteByte('\n')
				case '"':
					b.WriteByte('"')
				case '\\':
					b.WriteByte('\\')
				default:
					b.WriteByte(s[i+1])
				}
				i += 2
				continue
			}
			if c == '"' {
				i++
				break
			}
			b.WriteByte(c)
			i++
		}
		labels[key] = b.String()
		s = strings.TrimSpace(s[i:])
		s = strings.TrimPrefix(s, ",")
		s = strings.TrimSpace(s)
	}
	return labels
}

// seriesKey identifies a counter series across scrapes: metric name plus its labels in
// sorted order, so the same series matches between baseline and final regardless of label
// emission order.
func seriesKey(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteByte('\x1f')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	return b.String()
}

// synthesizeDelta builds the capture snapshot from the per-series counter increase between
// baseline and final. For each renamed series with a positive integer delta it appends that
// many CapturedRecordings (Value int64(1) each, mirroring the in-process one-record-per-
// increment model so s.Len assertions hold), tagged with tokeira's labels, keyed by the
// Temporal metric name. Records for each metric are ordered so StartOperation precedes
// CancelOperation — mirroring in-process emission chronology, so a test reading index [0]
// (e.g. the pending StartOperation in TestNexusAsyncOperationErrorRehydration) sees it.
// Series whose `namespace` label was not registered through this cluster are skipped: under
// Shape-2 every dedicated cluster shares one tokeirad /metrics, so without this scope a
// capture window would over-count by picking up concurrently-running sibling sub-tests'
// series (the in-process server isolates this via a separate metric registry per cluster).
// Series with no `namespace` label pass through unfiltered.
func synthesizeDelta(baseline, final map[string]scrapedCounter, namespaces *conformanceNamespaceSet) metricstest.CaptureSnapshot {
	snap := metricstest.CaptureSnapshot{}
	for key, fin := range final {
		if ns, ok := fin.labels["namespace"]; ok && namespaces != nil && !namespaces.contains(ns) {
			continue
		}
		prev := 0.0
		if b, ok := baseline[key]; ok {
			prev = b.value
		}
		delta := int(fin.value - prev + 0.5) // round; counters increase by whole numbers
		if delta <= 0 {
			continue
		}
		for i := 0; i < delta; i++ {
			snap[fin.temporalName] = append(snap[fin.temporalName], &metricstest.CapturedRecording{
				Value: int64(1),
				Tags:  fin.labels,
				Unit:  "",
			})
		}
	}
	for name := range snap {
		recs := snap[name]
		sort.SliceStable(recs, func(i, j int) bool {
			return methodOrder(recs[i].Tags["method"]) < methodOrder(recs[j].Tags["method"])
		})
	}
	return snap
}

// methodOrder ranks a Nexus method tag for deterministic record ordering that mirrors
// in-process emission chronology (a StartOperation always precedes any later CancelOperation
// on the same operation).
func methodOrder(method string) int {
	switch method {
	case "StartOperation":
		return 0
	case "CancelOperation":
		return 1
	default:
		return 2
	}
}
