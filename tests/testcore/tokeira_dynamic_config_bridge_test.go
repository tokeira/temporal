package testcore

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common/dynamicconfig"
)

func TestEncodeDynamicConfigOverrideValue_UnwrapsSingleNamespaceConstraint(t *testing.T) {
	encoded, ok := encodeDynamicConfigOverrideValue([]dynamicconfig.ConstrainedValue{{
		Constraints: dynamicconfig.Constraints{Namespace: "test-namespace"},
		Value:       1.0,
	}})
	if !ok {
		t.Fatal("expected a single constrained value to be encodable")
	}
	if encoded != `{"doubleValue":1}` {
		t.Fatalf("unexpected encoding: %s", encoded)
	}
}

func TestEncodeDynamicConfigOverrideValue_SerializesStructuredValueAsJSON(t *testing.T) {
	value := []any{
		map[string]any{"Pattern": "*", "AllowInsecure": true},
		map[string]any{"Pattern": "secure.example", "AllowInsecure": false},
	}
	encoded, ok := encodeDynamicConfigOverrideValue(value)
	if !ok {
		t.Fatal("expected structured value to be encodable")
	}
	var envelope struct {
		JSONValue string `json:"jsonValue"`
	}
	if err := json.Unmarshal([]byte(encoded), &envelope); err != nil {
		t.Fatalf("invalid oneof JSON: %v", err)
	}
	var roundTripped []map[string]any
	if err := json.Unmarshal([]byte(envelope.JSONValue), &roundTripped); err != nil {
		t.Fatalf("invalid structured JSON value: %v", err)
	}
	if len(roundTripped) != 2 || roundTripped[0]["Pattern"] != "*" {
		t.Fatalf("unexpected structured round trip: %#v", roundTripped)
	}
}

func TestEncodeDynamicConfigOverrideValue_RejectsUnserializableComposite(t *testing.T) {
	_, ok := encodeDynamicConfigOverrideValue(make(chan struct{}))
	if ok {
		t.Fatal("expected channel value to remain unsupported")
	}
}

func TestEncodeDynamicConfigOverrideValue_RejectsMultipleConstraints(t *testing.T) {
	_, ok := encodeDynamicConfigOverrideValue([]dynamicconfig.ConstrainedValue{
		{Value: 1.0},
		{Value: 2.0},
	})
	if ok {
		t.Fatal("expected multiple constrained values to remain unsupported")
	}
}

type conformanceConfigRecorder struct {
	name                  string
	logs, skips, failures []string
	cleanups              []func()
}

func (r *conformanceConfigRecorder) Name() string { return r.name }
func (r *conformanceConfigRecorder) Logf(format string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}
func (r *conformanceConfigRecorder) Skipf(format string, args ...any) {
	r.skips = append(r.skips, fmt.Sprintf(format, args...))
	panic("skipped")
}
func (r *conformanceConfigRecorder) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}
func (r *conformanceConfigRecorder) Cleanup(cleanup func()) { r.cleanups = append(r.cleanups, cleanup) }
func (r *conformanceConfigRecorder) finish() {
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		r.cleanups[i]()
	}
}

func TestConformanceDynamicConfigRefusal(t *testing.T) {
	for _, answer := range []string{"connect status unimplemented: kernel-excluded", "connect status invalid_argument: wrong type", "connection refused", "http 503"} {
		for _, key := range []dynamicconfig.Key{dynamicconfig.WorkflowIdReuseMinimalInterval.Key(), dynamicconfig.FrontendRPS.Key()} {
			t.Run(answer+"/"+key.String(), func(t *testing.T) {
				recorder := &conformanceConfigRecorder{name: "TestWorkflowTestSuite/TestStartWorkflowExecution_Terminate"}
				bridge := newConformanceDynamicConfigBridge("control", "frontend")
				calls := 0
				bridge.call = func(_, method, body string) error {
					calls++
					require.Equal(t, "SetDynamicConfigOverride", method)
					require.Contains(t, body, key.String())
					return errors.New(answer)
				}
				bridge.deliver(recorder, key, 0)()
				recorder.finish()
				require.Equal(t, 1, calls, "an undelivered override must neither restore nor clear")
				require.Empty(t, recorder.cleanups)
				require.Empty(t, recorder.skips)
				require.Empty(t, recorder.failures)
				require.Equal(t, []string{fmt.Sprintf(
					`TOKEIRA_CONFIG_TEST_UNHONOURED test=%q key=%q value="0" answer=%q`,
					recorder.name, key.String(), answer)}, recorder.logs)
			})
		}
	}
}

func TestConformanceDynamicConfigHarnessEquality(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		label bool
	}{
		{"int", 1, false},
		{"int64", int64(1), false},
		{"constrained", []dynamicconfig.ConstrainedValue{{Value: int64(1)}}, false},
		{"different value", 4, true},
		{"different encoding", "1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bridge := newConformanceDynamicConfigBridge("control", "frontend")
			calls := 0
			bridge.call = func(_, _, _ string) error { calls++; return errors.New("unimplemented") }
			recorder := &conformanceConfigRecorder{name: "TestSuite/TestLeaf"}
			bridge.deliver(recorder, dynamicconfig.MatchingNumTaskqueueReadPartitions.Key(), tc.value)()
			recorder.finish()
			require.Equal(t, 1, calls, "equal values must still be delivered")
			require.Empty(t, recorder.skips)
			require.Empty(t, recorder.failures)
			if tc.label {
				require.Len(t, recorder.logs, 1)
			} else {
				require.Empty(t, recorder.logs)
			}
		})
	}
}

func TestConformanceDynamicConfigHarness(t *testing.T) {
	recorder := &conformanceConfigRecorder{name: "TestSuite"}
	bridge := newConformanceDynamicConfigBridge("control", "frontend")
	var calls []string
	bridge.call = func(_, method, body string) error {
		require.Equal(t, "SetDynamicConfigOverride", method)
		calls = append(calls, body)
		return errors.New("connect status unimplemented: unknown key")
	}
	bridge.applyHarness(recorder)
	bridge.applyHarness(recorder)
	require.Len(t, calls, 34)
	require.Empty(t, recorder.skips)
	require.Len(t, recorder.logs, 34)
	for key, value := range bridge.defaults {
		require.Contains(t, recorder.logs, fmt.Sprintf(
			`TOKEIRA_CONFIG_HARNESS_UNHONOURED key=%q value=%q answer="connect status unimplemented: unknown key"`,
			key.String(), fmt.Sprint(value)))
	}
	bridge.deliver(recorder, dynamicconfig.FrontendRPS.Key(), int64(3000))()
	require.Len(t, recorder.logs, 34, "the cluster already reports the same harness value")
}

func TestConformanceDynamicConfigUndeliverable(t *testing.T) {
	for _, tc := range []struct {
		name, addr string
		value      any
		answer     string
	}{
		{"no listener", "", 5, "no control listener"},
		{"channel", "control", make(chan struct{}), "cannot be encoded"},
		{"infinity", "control", math.Inf(1), "cannot be encoded"},
		{"multiple constraints", "control", []dynamicconfig.ConstrainedValue{{Value: 1}, {Value: 2}}, "cannot be encoded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bridge := newConformanceDynamicConfigBridge(tc.addr, "frontend")
			bridge.call = func(_, _, _ string) error { t.Fatal("undeliverable value reached control call"); return nil }
			recorder := &conformanceConfigRecorder{name: "TestSuite/TestLeaf"}
			key := dynamicconfig.WorkflowTaskHeartbeatTimeout.Key()
			bridge.deliver(recorder, key, tc.value)()
			recorder.finish()
			require.Empty(t, recorder.skips)
			require.Empty(t, recorder.cleanups)
			require.Empty(t, recorder.failures)
			require.Len(t, recorder.logs, 1)
			require.Contains(t, recorder.logs[0], `TOKEIRA_CONFIG_TEST_UNHONOURED test="TestSuite/TestLeaf"`)
			require.Contains(t, recorder.logs[0], fmt.Sprintf("key=%q value=%q", key.String(), fmt.Sprint(tc.value)))
			require.Contains(t, recorder.logs[0], tc.answer)
		})
	}
}

func TestConformanceDynamicConfigHarnessParallel(t *testing.T) {
	bridge := newConformanceDynamicConfigBridge("control", "frontend")
	bridge.defaults = make(map[dynamicconfig.Key]any)
	for i := range 32 {
		for j := range 8 {
			bridge.defaults[dynamicconfig.MakeKey(fmt.Sprintf("key.%d.%d", i, j))] = 1
		}
	}
	refused := errors.New("unimplemented")
	bridge.call = func(_, _, body string) error {
		if strings.Contains(body, `"intValue":"2"`) {
			return refused
		}
		return nil
	}
	host := &TemporalImpl{conformanceConfig: bridge, dcClient: dynamicconfig.NewMemoryClient()}
	for i := range 32 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			bridge.applyHarness(t)
			for j := range 8 {
				key := dynamicconfig.MakeKey(fmt.Sprintf("key.%d.%d", i, j))
				host.overrideHarnessDynamicConfig(t, key, 2)()
				require.ErrorIs(t, bridge.harnessError(key), refused)
				host.overrideHarnessDynamicConfig(t, key, 2)()
				bridge.deliver(t, key, 3)()
			}
		})
	}
}

func TestConformanceDynamicConfigCleanupRefusedHarness(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail=%t", fail), func(t *testing.T) {
			bridge := newConformanceDynamicConfigBridge("control", "frontend")
			key := dynamicconfig.WorkflowTaskHeartbeatTimeout.Key()
			bridge.defaults = map[dynamicconfig.Key]any{key: 5 * time.Second}
			var calls []string
			bridge.call = func(_, method, body string) error {
				calls = append(calls, method+" "+body)
				if strings.Contains(body, "durationNanos") {
					return errors.New("connect status invalid_argument: wrong type")
				}
				if method == "ClearDynamicConfigOverride" && fail {
					return errors.New("cleanup unavailable")
				}
				return nil
			}
			recorder := &conformanceConfigRecorder{name: "TestSuite/TestLeaf"}
			bridge.applyHarness(recorder)
			cleanup := bridge.deliver(recorder, key, 1)
			cleanup()
			recorder.finish()
			require.Equal(t, []string{
				fmt.Sprintf(`SetDynamicConfigOverride {"key":%q,"value":{"durationNanos":"5000000000"}}`, key.String()),
				fmt.Sprintf(`SetDynamicConfigOverride {"key":%q,"value":{"intValue":"1"}}`, key.String()),
				fmt.Sprintf(`ClearDynamicConfigOverride {"key":%q}`, key.String()),
			}, calls)
			require.Len(t, recorder.logs, 1)
			if fail {
				require.Len(t, recorder.failures, 1)
				require.Contains(t, recorder.failures[0], "cleanup unavailable")
			} else {
				require.Empty(t, recorder.failures)
			}
		})
	}
}

func TestConformanceDynamicConfigCleanup(t *testing.T) {
	for _, restore := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("restore=%t/fail=%t", restore, fail), func(t *testing.T) {
				bridge := newConformanceDynamicConfigBridge("control", "frontend")
				key := dynamicconfig.EnableActivityEagerExecution.Key()
				value := any(false)
				expectedMethod := "ClearDynamicConfigOverride"
				expectedBody := fmt.Sprintf(`{"key":%q}`, key.String())
				if restore {
					key = dynamicconfig.WorkflowTaskHeartbeatTimeout.Key()
					value = time.Second
					expectedMethod = "SetDynamicConfigOverride"
					expectedBody = fmt.Sprintf(`{"key":%q,"value":{"durationNanos":"5000000000"}}`, key.String())
				}
				calls := 0
				bridge.call = func(_, method, body string) error {
					calls++
					if calls == 1 {
						return nil
					}
					require.Equal(t, expectedMethod, method)
					require.Equal(t, expectedBody, body)
					if fail {
						return errors.New("cleanup unavailable")
					}
					return nil
				}
				recorder := &conformanceConfigRecorder{name: "TestSuite/TestLeaf"}
				cleanup := bridge.deliver(recorder, key, value)
				cleanup()
				recorder.finish()
				require.Equal(t, 2, calls, "early cleanup must be idempotent")
				if fail {
					require.Len(t, recorder.failures, 1)
					require.Contains(t, recorder.failures[0], key.String())
					require.Contains(t, recorder.failures[0], "cleanup unavailable")
				} else {
					require.Empty(t, recorder.failures)
				}
			})
		}
	}
}

func TestConformanceDynamicConfigSuitePerTest(t *testing.T) {
	bridge := newConformanceDynamicConfigBridge("control", "frontend")
	bridge.suiteOverrides[dynamicconfig.MaximumBufferedEventsSizeInBytes.Key()] = 10 * 1024 * 1024
	calls := 0
	bridge.call = func(_, _, _ string) error { calls++; return errors.New("unimplemented") }
	for _, name := range []string{"TestSuite/TestFirst", "TestSuite/TestSibling"} {
		recorder := &conformanceConfigRecorder{name: name}
		bridge.applySuite(recorder)
		recorder.finish()
		require.Empty(t, recorder.skips)
		require.Empty(t, recorder.failures)
		require.Len(t, recorder.logs, 1)
		require.Contains(t, recorder.logs[0], fmt.Sprintf("test=%q", name))
	}
	require.Equal(t, 2, calls)
}

func TestConformanceDynamicConfigSuiteLifetime(t *testing.T) {
	bridge := newConformanceDynamicConfigBridge("control", "frontend")
	bridge.suiteT = t
	host := &TemporalImpl{conformanceConfig: bridge, dcClient: dynamicconfig.NewMemoryClient()}
	key := dynamicconfig.WorkflowTaskHeartbeatTimeout.Key()
	calls := 0
	bridge.call = func(_, _, _ string) error { calls++; return nil }
	cleanup := host.overrideDynamicConfig(t, key, time.Second)
	require.Zero(t, calls, "suite overrides are checked by each test")
	for range 2 {
		recorder := &conformanceConfigRecorder{name: "TestSuite/TestLeaf"}
		bridge.applySuite(recorder)
		recorder.finish()
	}
	require.Equal(t, 4, calls, "each leaf applies its suite value and restores the corpus default")
	cleanup()
	require.Empty(t, bridge.suiteOverrides)
}

func TestConformanceDynamicConfigOptions(t *testing.T) {
	for _, frontend := range []string{"", "frontend"} {
		t.Run("frontend="+frontend, func(t *testing.T) {
			t.Setenv(tokeiraFrontendAddrEnv, frontend)
			var options testOptions
			require.Panics(t, func() {
				WithDynamicConfig(dynamicconfig.WorkflowTaskHeartbeatTimeout, make(chan struct{}))(&options)
			})
			require.Empty(t, options.dynamicConfigSettings)
			require.NotPanics(t, func() {
				WithDynamicConfig(dynamicconfig.WorkflowTaskHeartbeatTimeout, time.Second)(&options)
			})
			require.Len(t, options.dynamicConfigSettings, 1)
			require.Equal(t, dynamicconfig.WorkflowTaskHeartbeatTimeout.Key(), options.dynamicConfigSettings[0].setting.Key())
			require.Equal(t, time.Second, options.dynamicConfigSettings[0].value)
		})
	}
}
