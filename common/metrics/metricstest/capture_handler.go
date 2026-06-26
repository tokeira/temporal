package metricstest

import (
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

// CapturedRecording is a single recording. Fields here should not be mutated.
type CapturedRecording struct {
	Value any
	Tags  map[string]string
	Unit  metrics.MetricUnit
}

// Capture is a specific capture instance.
type Capture struct {
	recordings     CaptureSnapshot
	recordingsLock sync.RWMutex
	// onSnapshot, when non-nil, is invoked by Snapshot to merge externally-sourced
	// recordings (e.g. metrics scraped from an out-of-process server by the Tokeira
	// conformance metrics bridge) into the returned snapshot. It is nil for every
	// in-process capture (StartCapture only sets it when the handler was created with
	// a source factory), so in-process capture behaviour is unchanged.
	onSnapshot func() CaptureSnapshot
	// onStop, when non-nil, is invoked by StopCapture so an external source can freeze
	// its window at the same moment in-process recording stops (in-process metrics are
	// no longer captured after StopCapture). This keeps the external snapshot bounded to
	// [StartCapture, StopCapture] rather than leaking later activity into Snapshot. nil
	// for in-process captures.
	onStop func()
}

type CaptureSnapshot = map[string][]*CapturedRecording

// Snapshot returns a copy of all metrics recorded, keyed by name. When the capture has
// an external source (onSnapshot), its recordings are merged in, appended after any
// in-process recordings for the same metric name.
func (c *Capture) Snapshot() CaptureSnapshot {
	c.recordingsLock.RLock()
	ret := maps.Clone(c.recordings)
	for k, v := range ret {
		ret[k] = slices.Clone(v)
	}
	c.recordingsLock.RUnlock()
	if c.onSnapshot != nil {
		for name, recs := range c.onSnapshot() {
			ret[name] = append(ret[name], recs...)
		}
	}
	return ret
}

func (c *Capture) record(name string, r *CapturedRecording) {
	c.recordingsLock.Lock()
	defer c.recordingsLock.Unlock()
	c.recordings[name] = append(c.recordings[name], r)
}

// CaptureHandler is a [metrics.Handler] that captures each metric recording.
type CaptureHandler struct {
	tags         []metrics.Tag
	captures     map[*Capture]struct{}
	capturesLock *sync.RWMutex
	captureCount *atomic.Int32
	// sourceFactory, when non-nil, is invoked once per StartCapture to begin an
	// external metric source for that capture. It returns two per-capture funcs: an
	// onStop the matching StopCapture invokes (to freeze the source's window), and an
	// onSnapshot the matching Snapshot invokes (to produce externally-sourced
	// recordings, e.g. the scrape-and-diff result of the Tokeira conformance metrics
	// bridge). Calling the factory at StartCapture lets the source record a baseline.
	// nil for the default in-process handler, so in-process behaviour is unchanged.
	sourceFactory func() (onStop func(), onSnapshot func() CaptureSnapshot)
}

var _ metrics.Handler = (*CaptureHandler)(nil)

// NewCaptureHandler creates a new [metrics.Handler] that captures.
func NewCaptureHandler() *CaptureHandler {
	return &CaptureHandler{
		captures:     map[*Capture]struct{}{},
		capturesLock: &sync.RWMutex{},
		captureCount: &atomic.Int32{},
	}
}

// NewCaptureHandlerWithSource creates a capture handler whose captures are populated
// from an external source rather than (only) in-process metric recordings. The factory
// is invoked once per StartCapture and returns the func that capture's Snapshot calls to
// produce externally-sourced recordings. Used by the Tokeira Tier-2 conformance bridge to
// feed metrics scraped from an out-of-process tokeirad into the corpus's capture surface.
func NewCaptureHandlerWithSource(factory func() (onStop func(), onSnapshot func() CaptureSnapshot)) *CaptureHandler {
	return &CaptureHandler{
		captures:      map[*Capture]struct{}{},
		capturesLock:  &sync.RWMutex{},
		captureCount:  &atomic.Int32{},
		sourceFactory: factory,
	}
}

// StartCapture returns a started capture. StopCapture should be called on
// complete.
func (c *CaptureHandler) StartCapture() *Capture {
	capture := &Capture{recordings: make(CaptureSnapshot)}
	// Begin the external source (if any) at capture start so it can record a baseline;
	// the returned funcs are invoked by capture's StopCapture and Snapshot.
	if c.sourceFactory != nil {
		capture.onStop, capture.onSnapshot = c.sourceFactory()
	}
	c.capturesLock.Lock()
	defer c.capturesLock.Unlock()

	c.captures[capture] = struct{}{}
	c.captureCount.Add(1)
	return capture
}

// StopCapture stops capturing metrics for the given capture instance.
func (c *CaptureHandler) StopCapture(capture *Capture) {
	// Let an external source freeze its window at the moment in-process recording stops,
	// so post-stop activity does not leak into a later Snapshot.
	if capture.onStop != nil {
		capture.onStop()
	}
	c.capturesLock.Lock()
	defer c.capturesLock.Unlock()

	delete(c.captures, capture)
	c.captureCount.Add(-1)
}

// WithTags implements [metrics.Handler.WithTags].
func (c *CaptureHandler) WithTags(tags ...metrics.Tag) metrics.Handler {
	return &CaptureHandler{
		tags:          append(append(make([]metrics.Tag, 0, len(c.tags)+len(tags)), c.tags...), tags...),
		captures:      c.captures,
		capturesLock:  c.capturesLock,
		captureCount:  c.captureCount,
		sourceFactory: c.sourceFactory,
	}
}

func (c *CaptureHandler) record(name string, v any, unit metrics.MetricUnit, tags ...metrics.Tag) {
	// If no captures are active, discard the metric to save memory.
	if c.captureCount.Load() == 0 {
		return
	}

	rec := &CapturedRecording{Value: v, Tags: make(map[string]string, len(c.tags)+len(tags)), Unit: unit}
	for _, tag := range c.tags {
		rec.Tags[tag.Key] = tag.Value
	}
	for _, tag := range tags {
		rec.Tags[tag.Key] = tag.Value
	}
	c.capturesLock.RLock()
	defer c.capturesLock.RUnlock()
	for cap := range c.captures {
		cap.record(name, rec)
	}
}

// Counter implements [metrics.Handler.Counter].
func (c *CaptureHandler) Counter(name string) metrics.CounterIface {
	return metrics.CounterFunc(func(v int64, tags ...metrics.Tag) { c.record(name, v, "", tags...) })
}

// Gauge implements [metrics.Handler.Gauge].
func (c *CaptureHandler) Gauge(name string) metrics.GaugeIface {
	return metrics.GaugeFunc(func(v float64, tags ...metrics.Tag) { c.record(name, v, "", tags...) })
}

// Timer implements [metrics.Handler.Timer].
func (c *CaptureHandler) Timer(name string) metrics.TimerIface {
	return metrics.TimerFunc(func(v time.Duration, tags ...metrics.Tag) { c.record(name, v, "", tags...) })
}

// Histogram implements [metrics.Handler.Histogram].
func (c *CaptureHandler) Histogram(name string, unit metrics.MetricUnit) metrics.HistogramIface {
	return metrics.HistogramFunc(func(v int64, tags ...metrics.Tag) { c.record(name, v, unit, tags...) })
}

func (c *CaptureHandler) Close() error {
	return nil
}

func (c *CaptureHandler) StartBatch(_ string) metrics.BatchHandler {
	return c
}

// Stop implements [metrics.Handler.Stop].
func (*CaptureHandler) Stop(log.Logger) {}
