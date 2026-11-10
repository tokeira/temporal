package testcore

// Shape-2 sends harness defaults and test overrides to the external control service.
// Unhonoured settings never skip tests. Fixed-prefix lines report each refused harness
// key once per cluster and each unhonoured test override with its full test name, key,
// value, and control answer. Test overrides equal to the encoded harness value need no
// test label. Suite settings are applied and checked separately for each test. Only
// delivered overrides register cleanup, which restores an honoured harness value or
// clears the key and fails the test on error.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/components/nexusoperations"
	runner "go.temporal.io/server/tests/tokeira_conformance_runner"
)

// tokeiraControlAddrEnv carries the tokeirad conformance control-service host:port from the
// harness to the corpus, paired with TOKEIRA_CONFORMANCE_FRONTEND_ADDR. It is set whenever the
// harness launches tokeirad; the listener answering there exists only when that tokeirad was
// built with the `conformance` feature.
const tokeiraControlAddrEnv = "TOKEIRA_CONFORMANCE_CONTROL_ADDR"

// controlRequestTimeout bounds a single control RPC. A dead or missing listener fails fast
// (connection refused) rather than stalling an override call for the full timeout.
const controlRequestTimeout = 5 * time.Second

// conformanceControlAddr returns the tokeirad control-service address, or "" when no control
// listener was wired — i.e. outside conformance mode, or when tokeirad was built without the
// `conformance` feature (the harness still exports the address, but nothing answers there).
func conformanceControlAddr() string {
	return strings.TrimSpace(os.Getenv(tokeiraControlAddrEnv))
}

type conformanceConfigTest interface {
	Name() string
	Logf(string, ...any)
	Errorf(string, ...any)
	Cleanup(func())
}

type conformanceDynamicConfigBridge struct {
	addr           string
	call           func(string, string, string) error
	defaults       map[dynamicconfig.Key]any
	harnessOnce    sync.Once
	harnessMu      sync.Mutex
	harnessErrors  map[dynamicconfig.Key]error
	suiteT         *testing.T
	suiteOverrides map[dynamicconfig.Key]any
}

func newConformanceDynamicConfigBridge(addr, frontend string) *conformanceDynamicConfigBridge {
	defaults := maps.Clone(dynamicConfigOverrides)
	defaults[nexusoperations.CallbackURLTemplate.Key()] = "http://" + frontend + "/namespaces/{{.NamespaceName}}/nexus/callback"
	defaults[dynamicconfig.MatchingNumTaskqueueReadPartitions.Key()] = 1
	defaults[dynamicconfig.MatchingNumTaskqueueWritePartitions.Key()] = 1
	return &conformanceDynamicConfigBridge{
		addr:           addr,
		call:           postConformanceControl,
		defaults:       defaults,
		harnessErrors:  make(map[dynamicconfig.Key]error),
		suiteOverrides: make(map[dynamicconfig.Key]any),
	}
}

func (b *conformanceDynamicConfigBridge) set(key dynamicconfig.Key, value any) error {
	if b.addr == "" {
		return errors.New("no control listener configured while running against tokeirad")
	}
	encoded, ok := encodeDynamicConfigOverrideValue(value)
	if !ok {
		return fmt.Errorf("value of type %T cannot be encoded", value)
	}
	body := fmt.Sprintf(`{"key":%s,"value":%s}`, strconv.Quote(key.String()), encoded)
	return b.call(b.addr, "SetDynamicConfigOverride", body)
}

func (b *conformanceDynamicConfigBridge) restore(t conformanceConfigTest, key dynamicconfig.Key) func() {
	return sync.OnceFunc(func() {
		var err error
		if value, ok := b.defaults[key]; ok && b.harnessError(key) == nil {
			err = b.set(key, value)
		} else {
			err = b.call(b.addr, "ClearDynamicConfigOverride", fmt.Sprintf(`{"key":%s}`, strconv.Quote(key.String())))
		}
		if err != nil {
			t.Errorf("tokeira conformance: cleanup of dynamic-config key %q failed: %v", key, err)
		}
	})
}

func (b *conformanceDynamicConfigBridge) applyHarness(t conformanceConfigTest) {
	b.harnessOnce.Do(func() {
		for _, key := range conformanceConfigKeys(b.defaults) {
			if err := b.set(key, b.defaults[key]); err != nil {
				b.recordHarnessError(t, key, b.defaults[key], err)
			}
		}
	})
}

func (b *conformanceDynamicConfigBridge) harnessError(key dynamicconfig.Key) error {
	b.harnessMu.Lock()
	defer b.harnessMu.Unlock()
	return b.harnessErrors[key]
}

func (b *conformanceDynamicConfigBridge) recordHarnessError(t conformanceConfigTest, key dynamicconfig.Key, value any, err error) {
	b.harnessMu.Lock()
	_, reported := b.harnessErrors[key]
	if !reported {
		b.harnessErrors[key] = err
	}
	b.harnessMu.Unlock()
	if !reported {
		t.Logf(runner.DynamicConfigHarnessLogFormat, key.String(), fmt.Sprint(value), err.Error())
	}
}

func (b *conformanceDynamicConfigBridge) deliver(t conformanceConfigTest, key dynamicconfig.Key, value any) func() {
	if err := b.set(key, value); err != nil {
		if !b.matchesHarness(key, value) {
			t.Logf(runner.DynamicConfigTestLogFormat, t.Name(), key.String(), fmt.Sprint(value), err.Error())
		}
		return func() {}
	}
	cleanup := b.restore(t, key)
	t.Cleanup(cleanup)
	return cleanup
}

func (b *conformanceDynamicConfigBridge) matchesHarness(key dynamicconfig.Key, value any) bool {
	harness, exists := b.defaults[key]
	if !exists {
		return false
	}
	encoded, ok := encodeDynamicConfigOverrideValue(value)
	harnessEncoded, harnessOK := encodeDynamicConfigOverrideValue(harness)
	return ok && harnessOK && encoded == harnessEncoded
}

func (b *conformanceDynamicConfigBridge) applySuite(t conformanceConfigTest) {
	if b == nil {
		return
	}
	for _, key := range conformanceConfigKeys(b.suiteOverrides) {
		b.deliver(t, key, b.suiteOverrides[key])
	}
}

func (c *TemporalImpl) overrideHarnessDynamicConfig(t *testing.T, key dynamicconfig.Key, value any) func() {
	if c.conformanceConfig == nil {
		return c.overrideDynamicConfig(t, key, value)
	}
	b := c.conformanceConfig
	local := c.dcClient.PartialOverrideValue(key, value)
	cleanup := sync.OnceFunc(local)
	t.Cleanup(cleanup)
	if b.harnessError(key) != nil {
		return cleanup
	}
	if err := b.set(key, value); err != nil {
		b.recordHarnessError(t, key, value, err)
		return cleanup
	}
	remote := b.restore(t, key)
	t.Cleanup(remote)
	return func() {
		remote()
		cleanup()
	}
}

func (c *TemporalImpl) deliverConformanceDynamicConfigOverride(t *testing.T, key dynamicconfig.Key, value any) func() {
	b := c.conformanceConfig
	if b == nil {
		return func() {}
	}
	if t == b.suiteT {
		previous, exists := b.suiteOverrides[key]
		b.suiteOverrides[key] = value
		cleanup := sync.OnceFunc(func() {
			if exists {
				b.suiteOverrides[key] = previous
			} else {
				delete(b.suiteOverrides, key)
			}
		})
		t.Cleanup(cleanup)
		return cleanup
	}
	return b.deliver(t, key, value)
}

// encodeDynamicConfigOverrideValue maps a Go dynamic-config value to the JSON body of the
// DynamicConfigValue oneof. Proto3 JSON encodes int64 (int_value, duration_nanos) as a string,
// matching tokeira's buffa serializer. Composite values are serialized into json_value; the wired
// consumer owns their setting-specific schema. Returns ok=false only when JSON cannot represent the
// value, so the caller can report the undeliverable override.
func encodeDynamicConfigOverrideValue(value any) (string, bool) {
	switch v := value.(type) {
	case []dynamicconfig.ConstrainedValue:
		// The control store is process-wide: a single constraint can carry its
		// scalar value, but parallel tests still share that value. Multiple
		// alternatives cannot be flattened without inventing precedence.
		if len(v) != 1 {
			return "", false
		}
		return encodeDynamicConfigOverrideValue(v[0].Value)
	case int:
		return fmt.Sprintf(`{"intValue":%s}`, strconv.Quote(strconv.FormatInt(int64(v), 10))), true
	case int32:
		return fmt.Sprintf(`{"intValue":%s}`, strconv.Quote(strconv.FormatInt(int64(v), 10))), true
	case int64:
		return fmt.Sprintf(`{"intValue":%s}`, strconv.Quote(strconv.FormatInt(v, 10))), true
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return "", false
		}
		return fmt.Sprintf(`{"doubleValue":%s}`, strconv.FormatFloat(v, 'g', -1, 64)), true
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return "", false
		}
		return fmt.Sprintf(`{"doubleValue":%s}`, strconv.FormatFloat(float64(v), 'g', -1, 32)), true
	case bool:
		return fmt.Sprintf(`{"boolValue":%t}`, v), true
	case string:
		return fmt.Sprintf(`{"stringValue":%s}`, strconv.Quote(v)), true
	case time.Duration:
		return fmt.Sprintf(`{"durationNanos":%s}`, strconv.Quote(strconv.FormatInt(int64(v), 10))), true
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return "", false
		}
		return fmt.Sprintf(`{"jsonValue":%s}`, strconv.Quote(string(raw))), true
	}
}

// postConformanceControl issues one Connect (JSON-over-HTTP-POST) unary call to the tokeirad
// control service, returning nil only on HTTP 200. A non-200 carries a Connect error envelope
// ({"code","message"}); its code/message are surfaced so an unimplemented (unhonoured key) is
// distinguishable in the test log from a transport failure.
func postConformanceControl(addr, method, body string) error {
	url := fmt.Sprintf("http://%s/tokeira.conformance.v1.ConformanceControlService/%s", addr, method)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")

	client := &http.Client{Timeout: controlRequestTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	var envelope struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
	_ = json.Unmarshal(raw, &envelope)
	if envelope.Code != "" {
		return fmt.Errorf("connect status %s: %s", envelope.Code, envelope.Message)
	}
	return fmt.Errorf("http %d", resp.StatusCode)
}

func withHarnessDynamicConfig(setting dynamicconfig.GenericSetting, value any) TestOption {
	return func(options *testOptions) {
		WithDynamicConfig(setting, value)(options)
		options.dynamicConfigSettings[len(options.dynamicConfigSettings)-1].harness = true
	}
}

func conformanceConfigKeys(values map[dynamicconfig.Key]any) []dynamicconfig.Key {
	return slices.SortedFunc(maps.Keys(values), func(a, b dynamicconfig.Key) int {
		return strings.Compare(a.String(), b.String())
	})
}
