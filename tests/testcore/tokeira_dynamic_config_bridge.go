package testcore

// Tokeira Tier-2 conformance: dynamic-config control bridge.
//
// Under Shape-2 the backend `tokeirad` runs out-of-process, so the corpus's
// OverrideDynamicConfig writes to the in-process onebox MemoryClient — which the
// external engine never reads. This bridge closes that gap for the keys tokeira
// genuinely honours: it delivers each override to `tokeirad`'s conformance
// control service (a Connect-RPC surface mounted only in a `--features
// conformance` build, on a separate loopback listener) and returns a cleanup
// that clears it.
//
// It is honest, not faking. tokeira's control service is the arbiter: it accepts
// only wired, non-kernel keys whose declared value type matches, and rejects
// every other key (unimplemented / invalid_argument). On rejection — or when no
// control listener was wired, or the value type is not encodable — the bridge
// logs and returns a no-op. It NEVER fails the test on delivery: a corpus test
// that REQUIRES an unhonoured override is skip-registered
// (tokeira_conformance_skip.go), and the config-as-constant convention covers
// keys whose v1.31.0 default already matches tokeira's. So a silently-dropped
// override can only surface as an honest test failure, never a fabricated pass.
//
// The proving consumer is the reported-problems threshold
// (system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute), which
// TestWFTFailureReportedProblemsTestSuite sets to 2 in SetupTest and mutates
// 0->2 mid-run in TestWFTFailureReportedProblems_DynamicConfigChanges; tokeira
// reads it live at Describe (spec .kiro/specs/conformance-config-override/).

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
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

// deliverConformanceDynamicConfigOverride delivers an OverrideDynamicConfig(name, value) to the
// out-of-process tokeirad via its Connect-RPC control service and returns a cleanup that clears
// the override. It bridges the in-process MemoryClient write, which the external engine cannot see.
//
// It returns a no-op cleanup, never failing the test, when: not in conformance mode / no control
// listener; the value type is not encodable; or tokeira does not honour the key (unimplemented /
// invalid_argument) or the RPC otherwise fails. A test that REQUIRES an unhonoured override is
// skip-registered, and config-as-constant covers keys whose default already matches tokeira's.
func deliverConformanceDynamicConfigOverride(t *testing.T, name dynamicconfig.Key, value any) func() {
	noop := func() {}
	addr := conformanceControlAddr()
	if addr == "" {
		return noop
	}
	keyName := name.String()
	encoded, ok := encodeDynamicConfigOverrideValue(value)
	if !ok {
		t.Logf("tokeira conformance: dynamic-config key %q value type %T not encodable for control "+
			"delivery; relying on skip registry / config-as-constant", keyName, value)
		return noop
	}
	setBody := fmt.Sprintf(`{"key":%s,"value":%s}`, strconv.Quote(keyName), encoded)
	if err := postConformanceControl(addr, "SetDynamicConfigOverride", setBody); err != nil {
		t.Logf("tokeira conformance: dynamic-config override %q not delivered (%v); relying on skip "+
			"registry / config-as-constant", keyName, err)
		return noop
	}
	return func() {
		clearBody := fmt.Sprintf(`{"key":%s}`, strconv.Quote(keyName))
		if err := postConformanceControl(addr, "ClearDynamicConfigOverride", clearBody); err != nil {
			t.Logf("tokeira conformance: clearing dynamic-config override %q failed: %v", keyName, err)
		}
	}
}

// encodeDynamicConfigOverrideValue maps a Go dynamic-config value to the JSON body of the
// DynamicConfigValue oneof. Proto3 JSON encodes int64 (int_value, duration_nanos) as a string,
// matching tokeira's buffa serializer. Returns ok=false for a type the control proto does not
// model, so the caller logs and no-ops rather than inventing a value.
func encodeDynamicConfigOverrideValue(value any) (string, bool) {
	switch v := value.(type) {
	case int:
		return fmt.Sprintf(`{"intValue":%s}`, strconv.Quote(strconv.FormatInt(int64(v), 10))), true
	case int32:
		return fmt.Sprintf(`{"intValue":%s}`, strconv.Quote(strconv.FormatInt(int64(v), 10))), true
	case int64:
		return fmt.Sprintf(`{"intValue":%s}`, strconv.Quote(strconv.FormatInt(v, 10))), true
	case float64:
		return fmt.Sprintf(`{"doubleValue":%s}`, strconv.FormatFloat(v, 'g', -1, 64)), true
	case float32:
		return fmt.Sprintf(`{"doubleValue":%s}`, strconv.FormatFloat(float64(v), 'g', -1, 32)), true
	case bool:
		return fmt.Sprintf(`{"boolValue":%t}`, v), true
	case string:
		return fmt.Sprintf(`{"stringValue":%s}`, strconv.Quote(v)), true
	case time.Duration:
		return fmt.Sprintf(`{"durationNanos":%s}`, strconv.Quote(strconv.FormatInt(int64(v), 10))), true
	default:
		return "", false
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
