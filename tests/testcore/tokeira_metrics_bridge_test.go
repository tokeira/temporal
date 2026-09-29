package testcore

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics/metricstest"
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

// Overlapping windows retain independent deltas and release cluster isolation only
// after the last window freezes; Update-with-Start keeps error captures open.
func TestTokeiraMetricsBridgeAllowsOverlappingCaptureWindows(t *testing.T) {
	var counter atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `tokeira_edge_http_service_requests_total{operation="op",namespace="test-ns"} %d`+"\n", counter.Load())
	}))
	defer server.Close()
	namespaces := newConformanceNamespaceSet()
	namespaces.add("test-ns")
	source := newTokeiraMetricsScrapeSource(server.URL, namespaces)
	stopFirst, first := source()
	defer stopFirst()
	counter.Store(3)
	stopSecond, second := source()
	defer stopSecond()
	counter.Store(5)
	firstSnapshot := first()
	require.Len(t, firstSnapshot["http_service_requests"], 5)
	for _, recording := range firstSnapshot["http_service_requests"] {
		require.Equal(t, int64(1), recording.Value)
	}
	stopFirst()
	if tokeiraMetricsCaptureMu.TryLock() {
		tokeiraMetricsCaptureMu.Unlock()
		t.Fatal("the remaining capture must retain cluster isolation")
	}
	counter.Store(8)
	stopSecond()
	secondSnapshot := second()
	require.Len(t, secondSnapshot["http_service_requests"], 5)
	counter.Store(13)
	require.Equal(t, firstSnapshot, first())
	require.Equal(t, secondSnapshot, second())
	if tokeiraMetricsCaptureMu.TryLock() {
		tokeiraMetricsCaptureMu.Unlock()
	} else {
		t.Fatal("the final frozen window must release cluster isolation")
	}
}

func TestTokeiraMetricsBridgeConcurrentCaptureLifecycle(t *testing.T) {
	var counter, scrapes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		scrapes.Add(1)
		_, _ = fmt.Fprintf(w, "tokeira_edge_http_service_requests_total{namespace=\"test-ns\"} %d\n", counter.Load())
	}))
	defer server.Close()
	namespaces := newConformanceNamespaceSet()
	namespaces.add("test-ns")
	source := newTokeiraMetricsScrapeSource(server.URL, namespaces)

	const captures, readers = 8, 4
	type window struct {
		stop     func()
		snapshot func() metricstest.CaptureSnapshot
	}
	windows := make([]window, captures)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range windows {
		workers.Go(func() {
			<-start
			windows[i].stop, windows[i].snapshot = source()
		})
	}
	close(start)
	workers.Wait()
	defer func() {
		for _, window := range windows {
			window.stop()
		}
	}()
	require.Equal(t, int64(captures), scrapes.Load())
	counter.Store(5)

	freeze := make(chan struct{})
	snapshots := make(chan metricstest.CaptureSnapshot, captures*readers)
	for _, window := range windows {
		for range readers {
			workers.Go(func() {
				<-freeze
				snapshots <- window.snapshot()
			})
			workers.Go(func() {
				<-freeze
				window.stop()
			})
		}
	}
	close(freeze)
	workers.Wait()
	close(snapshots)
	// A final scrape is published once to every concurrent reader, even when
	// StopCapture races Snapshot; no window may release another window's lease.
	require.Equal(t, int64(2*captures), scrapes.Load())
	for snapshot := range snapshots {
		require.Len(t, snapshot["http_service_requests"], 5)
		for _, recording := range snapshot["http_service_requests"] {
			require.Equal(t, int64(1), recording.Value)
		}
	}
	counter.Store(9)
	for _, window := range windows {
		require.Len(t, window.snapshot()["http_service_requests"], 5)
		window.stop()
	}
	require.Equal(t, int64(2*captures), scrapes.Load())
	stopNext, next := source()
	defer stopNext()
	counter.Store(10)
	require.Len(t, next()["http_service_requests"], 1)
}

func TestTokeiraMetricsBridgeOtherClusterWaitsForLastCapture(t *testing.T) {
	var counter atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, "tokeira_edge_http_service_requests_total{namespace=\"test-ns\"} %d\n", counter.Load())
	}))
	defer server.Close()
	namespaces := newConformanceNamespaceSet()
	namespaces.add("test-ns")
	firstCluster := newTokeiraMetricsScrapeSource(server.URL, namespaces)
	otherCluster := newTokeiraMetricsScrapeSource(server.URL, namespaces)
	stopFirst, _ := firstCluster()
	defer stopFirst()
	stopSecond, _ := firstCluster()
	defer stopSecond()

	attempting := make(chan struct{})
	acquired := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var otherSnapshot metricstest.CaptureSnapshot
	go func() {
		close(attempting)
		stop, snapshot := otherCluster()
		defer close(finished)
		defer stop()
		close(acquired)
		<-release
		otherSnapshot = snapshot()
	}()
	defer func() {
		stopFirst()
		stopSecond()
		close(release)
		<-finished
	}()
	<-attempting
	counter.Store(3)
	stopFirst()
	// Check the actual exclusion lock as well as the waiting goroutine, so a
	// scheduler delay cannot conceal an early release after only one stop.
	if tokeiraMetricsCaptureMu.TryLock() {
		tokeiraMetricsCaptureMu.Unlock()
		t.Fatal("the second capture must retain isolation")
	}
	select {
	case <-acquired:
		t.Fatal("another cluster acquired isolation before the last capture froze")
	default:
	}
	counter.Store(5)
	stopSecond()
	<-acquired
	counter.Store(7)
	// Completing via cleanup also guarantees no waiting goroutine survives a
	// failed assertion. The snapshot is checked after that goroutine joins.
	t.Cleanup(func() {
		require.Len(t, otherSnapshot["http_service_requests"], 2)
	})
}

func TestTokeiraMetricsBridgeConcurrentOpenAndClose(t *testing.T) {
	var scrapes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		scrapes.Add(1)
		_, _ = fmt.Fprintln(w, "tokeira_edge_http_service_requests_total{namespace=\"test-ns\"} 5")
	}))
	defer server.Close()
	namespaces := newConformanceNamespaceSet()
	namespaces.add("test-ns")
	source := newTokeiraMetricsScrapeSource(server.URL, namespaces)
	const workersCount, rounds = 8, 16
	start := make(chan struct{})
	snapshots := make(chan metricstest.CaptureSnapshot, workersCount*rounds)
	var workers sync.WaitGroup
	for range workersCount {
		workers.Go(func() {
			<-start
			for range rounds {
				stop, snapshot := source()
				stop()
				snapshots <- snapshot()
			}
		})
	}
	close(start)
	workers.Wait()
	close(snapshots)
	for snapshot := range snapshots {
		require.Empty(t, snapshot)
	}
	require.Equal(t, int64(2*workersCount*rounds), scrapes.Load())
	if tokeiraMetricsCaptureMu.TryLock() {
		tokeiraMetricsCaptureMu.Unlock()
	} else {
		t.Fatal("concurrent opening and closing must release isolation")
	}
}
