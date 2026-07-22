// Package runner holds the shared out-of-process tokeirad lifecycle used by the
// standalone conformance runner commands (tokeira_conformance_runall and
// tokeira_conformance_runsuite).
//
// It is a plain (non-test) library so both main commands import it directly
// rather than duplicating the boot-or-reuse machinery. This does NOT reintroduce
// the coupling the run-all command deliberately avoids: that command duplicates
// the *harness* lifecycle (tests/testcore/tokeira_harness.go) only because the
// harness is *testing.T-coupled and belongs to the test binary. Nothing here
// depends on *testing.T, so sharing between two standalone commands is clean.
package runner

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Environment variables shared with the onebox seam (onebox.go), the harness,
// and the corpus bridges. They are duplicated as string consts here rather than
// imported from the test package, which these standalone commands must not
// depend on.
const (
	// TokeiradBinEnv names the path to a prebuilt tokeirad binary. Required only
	// when a runner must boot its own frontend (i.e. when SeamAddrEnv is unset).
	TokeiradBinEnv = "TOKEIRA_BIN"
	// SeamAddrEnv mirrors the onebox seam env read by onebox.go and the harness.
	// When already set, a runner reuses that frontend; otherwise it exports the
	// address of the frontend it boots so the corpus resolves to it.
	SeamAddrEnv = "TOKEIRA_CONFORMANCE_FRONTEND_ADDR"
	// MetricsAddrEnv carries the tokeirad Prometheus /metrics host:port to the
	// corpus (read by the conformance cluster's metrics bridge).
	MetricsAddrEnv = "TOKEIRA_CONFORMANCE_METRICS_ADDR"
	// ControlAddrEnv carries tokeirad's conformance dynamic-config control-service
	// host:port to the corpus (read by the dynamic-config bridge). A listener
	// answers there only when tokeirad was built with the `conformance` feature;
	// absent it, the bridge no-ops.
	ControlAddrEnv = "TOKEIRA_CONFORMANCE_CONTROL_ADDR"
	// AuthCallbackURLEnv carries the corpus process's loopback authorization
	// callback URL to conformance-mode tokeirad.
	AuthCallbackURLEnv = "TOKEIRA_CONFORMANCE_AUTH_CALLBACK_URL"
)

const (
	// GoToolchain pins the Go toolchain every spawned `go test` uses, so the
	// corpus is exercised on the version its go.mod requires (matching
	// TEMPORAL_SERVER_COMPAT's release) rather than whatever `go` is first on
	// PATH. Pinning explicitly makes the version deterministic and visible.
	GoToolchain = "go1.26.2"
	// ReadyTimeout bounds how long BootOrReuse waits for a freshly-booted
	// tokeirad frontend to begin serving before giving up.
	ReadyTimeout = 60 * time.Second
)

// Process is the minimal lifecycle handle for a tokeirad frontend a runner boots
// itself. The address fields are also valid in reuse mode (populated by
// BootOrReuse from the operator's environment) so callers read them uniformly.
type Process struct {
	cmd             *exec.Cmd
	Addr            string
	MetricsAddr     string
	ControlAddr     string
	AuthCallbackURL string
}

// BootOrReuse returns a tokeirad frontend for the corpus to run against.
//
// If SeamAddrEnv is already set the operator manages the tokeirad lifecycle, so
// this reuses that frontend as-is: proc is nil and the metrics/control addresses
// pass through whatever the operator exported (empty disables the respective
// bridge). Otherwise it boots a tokeirad from TokeiradBinEnv on free loopback
// ports with a minimal in-memory TOML and waits until the frontend genuinely
// serves WorkflowService.
//
// The caller owns teardown of a booted process: when proc is non-nil, defer
// proc.Stop() and (for an interactive run) call InstallSignalCleanup(proc).
func BootOrReuse() (proc *Process, addr, metricsAddr, controlAddr, authCallbackURL string, err error) {
	if addr = os.Getenv(SeamAddrEnv); addr != "" {
		return nil, addr, os.Getenv(MetricsAddrEnv), os.Getenv(ControlAddrEnv), os.Getenv(AuthCallbackURLEnv), nil
	}
	bin := os.Getenv(TokeiradBinEnv)
	if bin == "" {
		return nil, "", "", "", "", fmt.Errorf(
			"neither %s nor %s is set: set %s to a prebuilt tokeirad binary to boot one, "+
				"or set %s to an already-running frontend",
			SeamAddrEnv, TokeiradBinEnv, TokeiradBinEnv, SeamAddrEnv)
	}
	proc, err = boot(bin)
	if err != nil {
		return nil, "", "", "", "", err
	}
	// Tear the just-booted process down on a readiness failure so a frontend that
	// never came up does not leak a process holding the ephemeral port.
	if err = WaitReady(proc.Addr, ReadyTimeout); err != nil {
		proc.Stop()
		return nil, "", "", "", "", err
	}
	return proc, proc.Addr, proc.MetricsAddr, proc.ControlAddr, proc.AuthCallbackURL, nil
}

// boot launches tokeirad on free loopback ports with a minimal in-memory TOML
// config (only infrastructure.network addresses are set; every other field
// defaults, which selects in-memory storage). The config lives in a temp dir
// cleaned up when the process stops.
func boot(bin string) (*Process, error) {
	addr, err := freeLoopbackAddr()
	if err != nil {
		return nil, err
	}
	// A second free port for tokeirad's Prometheus /metrics endpoint so the
	// corpus's metrics bridge can scrape it. Distinct from the default
	// 0.0.0.0:9090 to keep parallel runs from colliding.
	metricsAddr, err := freeLoopbackAddr()
	if err != nil {
		return nil, err
	}
	// A third free port for tokeirad's conformance dynamic-config control service.
	// tokeirad binds and answers here only in a `conformance` build; the
	// dynamic-config bridge posts overrides to it.
	controlAddr, err := freeLoopbackAddr()
	if err != nil {
		return nil, err
	}
	authCallbackAddr, err := freeLoopbackAddr()
	if err != nil {
		return nil, err
	}
	authCallbackURL := "http://" + authCallbackAddr + "/authorize"

	dir, err := os.MkdirTemp("", "tokeira-conformance-runner")
	if err != nil {
		return nil, fmt.Errorf("create temp config dir: %w", err)
	}
	cfgPath := filepath.Join(dir, "tokeirad-conformance.toml")
	cfg := fmt.Sprintf(
		"[infrastructure.network]\ngrpc_addr = %q\nmetrics_addr = %q\n\n"+
			"[policy.http_api]\nadditional_forwarded_headers = [\"this-header-forwarded\", \"this-header-prefix-forwarded-*\"]\n",
		addr, metricsAddr,
	)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		return nil, fmt.Errorf("write tokeirad config %q: %w", cfgPath, err)
	}

	cmd := exec.Command(bin, "--config", cfgPath)
	// Deliver the control-service bind address to the child; ignored by a
	// non-conformance build.
	cmd.Env = append(
		os.Environ(),
		ControlAddrEnv+"="+controlAddr,
		AuthCallbackURLEnv+"="+authCallbackURL,
	)
	// tokeirad logs go to stderr so a runner's stdout stays clean for its own output.
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start tokeirad %q: %w", bin, err)
	}

	return &Process{
		cmd: cmd, Addr: addr, MetricsAddr: metricsAddr, ControlAddr: controlAddr,
		AuthCallbackURL: authCallbackURL,
	}, nil
}

// Stop terminates the tokeirad subprocess: SIGTERM first to let it drain, then
// SIGKILL after a short grace window. Best-effort but reliable — it must not
// leak a process holding the ephemeral port.
func (p *Process) Stop() {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
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

// WaitReady polls the frontend's WorkflowService until GetSystemInfo succeeds or
// the timeout elapses. Readiness is "can a caller reach the WorkflowService
// surface the corpus uses?" — GetSystemInfo is read-only and is the same surface
// FrontendClient() reaches — not a gRPC health-protocol probe, which tokeirad is
// not confirmed to serve.
func WaitReady(addr string, timeout time.Duration) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial frontend %q: %w", addr, err)
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
			return nil
		}
		time.Sleep(pollInterval)
	}
	return fmt.Errorf("frontend %q not ready after %s: %w", addr, timeout, lastErr)
}

// InstallSignalCleanup ensures a booted tokeirad is torn down if the operator
// interrupts the run (Ctrl-C / SIGTERM), so an aborted manual run never leaks a
// process holding the ephemeral port.
func InstallSignalCleanup(proc *Process) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		proc.Stop()
		os.Exit(130)
	}()
}

// freeLoopbackAddr returns a currently-free 127.0.0.1:<port> using the standard
// bind-:0-then-close trick. The TOCTOU window before tokeirad binds is benign:
// loopback ephemeral reuse is rare and WaitReady would surface a genuine bind
// failure as a never-ready timeout.
func freeLoopbackAddr() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("reserve free port: %w", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", fmt.Errorf("release reserved port %q: %w", addr, err)
	}
	return addr, nil
}
