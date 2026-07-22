package testcore

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TokeiradProcess is the lifecycle handle for an externally-launched `tokeirad`
// frontend used by the Tokeira Tier-2 (Shape-2) functional-conformance seam.
//
// Tier 2 runs Temporal's unmodified functional corpus over the real gRPC wire
// against `tokeirad` (see temporal-functional-conformance design). The onebox
// seam in onebox.go short-circuits Temporal's own service boot when
// TOKEIRA_CONFORMANCE_FRONTEND_ADDR points at a running frontend; this harness
// is the glue that produces that running frontend: it starts the process, waits
// until the frontend actually serves, exposes its address, and tears it down.
//
// Design decision — take the binary, do not build it. The harness launches a
// prebuilt `tokeirad` binary located via the TOKEIRA_BIN env var rather than
// invoking the Rust toolchain (cargo) itself. Coupling these Go tests to the
// Rust build would drag the entire Rust workspace into the Go test path, make
// runs slow and non-hermetic, and conflate "did Tokeira build" with "does
// Tokeira conform". Building `tokeirad` is the caller's responsibility; the
// harness only runs it. When TOKEIRA_BIN is unset, StartTokeirad skips the test
// so the default fork checkout stays green without a Tokeira binary present.
type TokeiradProcess struct {
	cmd *exec.Cmd
	// Addr is the 127.0.0.1:<port> frontend gRPC address `tokeirad` was told to
	// serve on. It is the value a test injects into the onebox seam via
	// SetFrontendEnv (TOKEIRA_CONFORMANCE_FRONTEND_ADDR).
	Addr string
	// MetricsAddr is the 127.0.0.1:<port> address `tokeirad` serves its Prometheus
	// /metrics endpoint on (infrastructure.network.metrics_addr). The Tier-2 metrics
	// bridge scrapes it to feed the corpus CaptureMetricsHandler for metric-asserting
	// tests; SetFrontendEnv exports it as TOKEIRA_CONFORMANCE_METRICS_ADDR.
	MetricsAddr string
	// ControlAddr is the 127.0.0.1:<port> address `tokeirad` is told to bind its conformance
	// dynamic-config control service on, via TOKEIRA_CONFORMANCE_CONTROL_ADDR. A listener
	// answers there only when tokeirad was built with the `conformance` feature; the
	// dynamic-config bridge posts OverrideDynamicConfig values to it, and SetFrontendEnv
	// exports it so the corpus can reach it.
	ControlAddr string
	// AuthCallbackURL is served inside the corpus process and lets a
	// conformance-mode tokeirad invoke namespace-scoped SetOnAuthorize hooks.
	AuthCallbackURL string
}

// tokeiradBinEnv names the env var holding the path to a prebuilt `tokeirad`
// binary (e.g. .../target/debug/tokeirad). Unset means "no Tokeira to test".
const tokeiradBinEnv = "TOKEIRA_BIN"

// tokeiraFrontendAddrEnv mirrors the seam env var read by onebox.go. The harness
// honours an externally-pinned address when set, otherwise it picks a free port.
const tokeiraFrontendAddrEnv = "TOKEIRA_CONFORMANCE_FRONTEND_ADDR"

// StartTokeirad launches an external `tokeirad` frontend for the Tier-2 seam and
// returns a handle to manage its lifecycle.
//
// It skips the test (t.Skip) when TOKEIRA_BIN is unset, so a conformance test
// can be written unconditionally and simply no-op on a checkout that has not
// supplied a Tokeira binary. The frontend is bound to a free 127.0.0.1 port
// (chosen by binding :0 and immediately releasing it) unless
// TOKEIRA_CONFORMANCE_FRONTEND_ADDR is already set, so parallel runs do not
// collide on the default 0.0.0.0:7233.
//
// The chosen port is delivered to `tokeirad` through a throwaway TOML config in
// t.TempDir() carrying only `[infrastructure.network] grpc_addr`; every other
// field defaults, which selects in-memory storage (R2.7) — no DSQL is
// configured here (the DSQL variant is task 5.3, out of scope). The process is
// started but NOT waited on for readiness; the caller must invoke WaitReady.
func StartTokeirad(t *testing.T) *TokeiradProcess {
	t.Helper()

	bin := os.Getenv(tokeiradBinEnv)
	if bin == "" {
		t.Skipf("set %s to the tokeirad binary to run Tier-2 conformance", tokeiradBinEnv)
	}

	// Prefer a caller-pinned address, otherwise grab a free loopback port so
	// concurrent test binaries don't fight over the default frontend port.
	addr := os.Getenv(tokeiraFrontendAddrEnv)
	if addr == "" {
		addr = freeLoopbackAddr(t)
	}

	// A second free loopback port for tokeirad's Prometheus /metrics endpoint, so the
	// Tier-2 metrics bridge can scrape it without colliding with the default 0.0.0.0:9090
	// across parallel runs. metrics_enabled defaults true, so binding this port serves
	// /metrics for the renamed-counter scrape.
	metricsAddr := freeLoopbackAddr(t)

	// A third free loopback port for tokeirad's conformance dynamic-config control service,
	// delivered via TOKEIRA_CONFORMANCE_CONTROL_ADDR. tokeirad binds and answers here only when
	// built with the `conformance` feature; the dynamic-config bridge posts overrides to it.
	controlAddr := freeLoopbackAddr(t)
	authCallbackURL := "http://" + freeLoopbackAddr(t) + "/authorize"

	// tokeirad reads its frontend bind address from infrastructure.network.grpc_addr and
	// its metrics bind address from infrastructure.network.metrics_addr. A minimal TOML
	// file is sufficient because every other field has a default, and the default storage
	// is in-memory (ConfigStorageKind::InMemory), which is exactly what the default Tier-2
	// suite wants.
	cfgPath := filepath.Join(t.TempDir(), "tokeirad-conformance.toml")
	cfg := fmt.Sprintf(
		"[infrastructure.network]\ngrpc_addr = %q\nmetrics_addr = %q\n\n"+
			"[policy.http_api]\nadditional_forwarded_headers = [\"this-header-forwarded\", \"this-header-prefix-forwarded-*\"]\n",
		addr, metricsAddr,
	)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("tokeira conformance: write tokeirad config %q: %v", cfgPath, err)
	}

	cmd := exec.Command(bin, "--config", cfgPath)
	// Deliver the control-service bind address to the child. tokeirad reads
	// TOKEIRA_CONFORMANCE_CONTROL_ADDR only in a `conformance` build; any other build ignores
	// it (no control listener), and the dynamic-config bridge then no-ops. os.Environ() is
	// preserved so other harness env (e.g. wire-coverage) still reaches the child.
	cmd.Env = append(
		os.Environ(),
		tokeiraControlAddrEnv+"="+controlAddr,
		tokeiraAuthorizationCallbackURLEnv+"="+authCallbackURL,
	)
	// Surface tokeirad's own logs through the test log so a failed readiness wait
	// is debuggable rather than silent.
	cmd.Stdout = newTestLogWriter(t, "tokeirad stdout")
	cmd.Stderr = newTestLogWriter(t, "tokeirad stderr")

	if err := cmd.Start(); err != nil {
		t.Fatalf("tokeira conformance: start %q: %v", bin, err)
	}

	return &TokeiradProcess{
		cmd: cmd, Addr: addr, MetricsAddr: metricsAddr, ControlAddr: controlAddr,
		AuthCallbackURL: authCallbackURL,
	}
}

// WaitReady blocks until `tokeirad`'s frontend answers a trivial, side-effect-free
// WorkflowService RPC, or fails the test (and tears the process down) on timeout.
//
// Design decision — poll WorkflowService, not a health endpoint (design caveat 1).
// Temporal's onebox/SDKs would naturally probe the standard gRPC Health Checking
// Protocol (grpc.health.v1.Health/Check), but `tokeirad` is NOT confirmed to serve
// that protocol on the frontend port. The authoritative readiness signal for the
// seam is therefore "can a test actually call the WorkflowService surface?" — so
// the poll issues GetSystemInfo, which is read-only and is the same surface every
// functional test reaches through FrontendClient(). A success means the frontend
// is genuinely serving the contract the corpus exercises, not merely that a
// process is listening.
//
// The wait is a short-interval poll against a deadline (process-startup polling,
// not in-test timing), never a fixed long sleep. On timeout the process is killed
// so a failed bring-up never leaks a zombie and never reports false readiness.
func (p *TokeiradProcess) WaitReady(t *testing.T, timeout time.Duration) {
	t.Helper()

	conn, err := grpc.NewClient(p.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		p.Stop(t)
		t.Fatalf("tokeira conformance: dial frontend %q: %v", p.Addr, err)
	}
	defer func() { _ = conn.Close() }()

	client := workflowservice.NewWorkflowServiceClient(conn)

	deadline := time.Now().Add(timeout)
	const pollInterval = 100 * time.Millisecond
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), pollInterval)
		_, lastErr = client.GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(pollInterval)
	}

	p.Stop(t)
	t.Fatalf("tokeira conformance: frontend %q not ready after %s: %v", p.Addr, timeout, lastErr)
}

// Stop terminates the `tokeirad` subprocess, first asking it to exit cleanly with
// SIGTERM and escalating to SIGKILL if it does not exit within a short grace
// window. Teardown is best-effort but reliable: it never leaves a dangling
// process behind, which is essential because the harness picks an ephemeral port
// per run and a leaked process would hold it.
func (p *TokeiradProcess) Stop(t *testing.T) {
	t.Helper()

	if p.cmd == nil || p.cmd.Process == nil {
		return
	}

	// Ask for a graceful shutdown first; tokeirad owns its own drain.
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		// Already gone (or un-signalable) — nothing left to wait on.
		p.cmd = nil
		return
	}

	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
	p.cmd = nil
}

// SetFrontendEnv wires the onebox seam to this process by exporting
// TOKEIRA_CONFORMANCE_FRONTEND_ADDR for the duration of the test. t.Setenv
// automatically restores the prior value when the test ends, so the default
// onebox boot is preserved for every other test. A test calls this after
// WaitReady so that the subsequent TestCluster Start() resolves FrontendClient()
// to this running `tokeirad` rather than booting Temporal's own services.
func (p *TokeiradProcess) SetFrontendEnv(t *testing.T) {
	t.Helper()
	t.Setenv(tokeiraFrontendAddrEnv, p.Addr)
	// Pair the metrics endpoint with the frontend address so newConformanceCluster can
	// stand up the scrape-backed CaptureMetricsHandler for metric-asserting tests.
	if p.MetricsAddr != "" {
		t.Setenv(tokeiraMetricsAddrEnv, p.MetricsAddr)
	}
	// Pair the control endpoint so the dynamic-config bridge can post OverrideDynamicConfig
	// values to tokeirad. Harmless when tokeirad was built without the `conformance` feature:
	// nothing answers there and the bridge no-ops.
	if p.ControlAddr != "" {
		t.Setenv(tokeiraControlAddrEnv, p.ControlAddr)
	}
	if p.AuthCallbackURL != "" {
		t.Setenv(tokeiraAuthorizationCallbackURLEnv, p.AuthCallbackURL)
	}
}

// freeLoopbackAddr returns a currently-free 127.0.0.1:<port> address using the
// standard bind-:0-then-close trick. There is an inherent TOCTOU window between
// releasing the port here and `tokeirad` binding it, but loopback ephemeral-port
// reuse is rare enough in practice and the readiness poll would catch a genuine
// bind failure; this keeps parallel runs off the shared default port.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tokeira conformance: reserve free port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("tokeira conformance: release reserved port %q: %v", addr, err)
	}
	return addr
}

// testLogWriter adapts a *testing.T into an io.Writer so a child process's
// stdout/stderr is captured in the test log, prefixed for provenance.
type testLogWriter struct {
	t      *testing.T
	prefix string
}

func newTestLogWriter(t *testing.T, prefix string) *testLogWriter {
	return &testLogWriter{t: t, prefix: prefix}
}

func (w *testLogWriter) Write(p []byte) (int, error) {
	w.t.Logf("[%s] %s", w.prefix, p)
	return len(p), nil
}
