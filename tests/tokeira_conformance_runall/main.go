// Command tokeira-conformance-runall is the operator-invokable run-all executor
// for Tokeira Tier-2 functional conformance.
//
// It executes Temporal's entire pinned functional corpus (the `tests/` package)
// over the real gRPC wire against a running `tokeirad`, and is the concrete
// realisation of the design's core posture: run every test, classify in the
// report, never exclude a test from the run (temporal-functional-conformance
// design, "Run-all, classify-in-report").
//
// Process isolation (why per-entrypoint, not one wide `go test`). A single
// `go test ./tests/` invocation runs every test in one OS process, and a Go
// test that panics (e.g. an out-of-public-scope test dereferencing the nil
// persistence test-base under the Option-B shim) crashes that process and
// aborts every test scheduled after it — a de-facto exclusion that violates the
// run-all posture and Property 6. So the runner instead *enumerates* the
// complete set of top-level entrypoints (`go test -list`) and runs each in its
// own `go test -run '^Name$'` process. A panic is then contained to its single
// entrypoint; every other entrypoint still runs. The anti-exclusion guarantee
// shifts from "one wide invocation" to "the complete enumerated set, each
// isolated": `-list` discovers the full corpus and the runner fails loudly if
// that set is empty, so nothing is silently dropped. `-run '^Name$'` is an
// isolation boundary, never a curated subset — the runner has no allow/deny
// list and cannot narrow which entrypoints execute.
//
// Lifecycle. The runner reuses the Shape-2 seam already proven by the harness
// (tests/testcore/tokeira_harness.go): a single `tokeirad` frontend backs the
// whole corpus, and each FunctionalTestBase suite resolves FrontendClient() to
// it via TOKEIRA_CONFORMANCE_FRONTEND_ADDR. Namespaces stay isolated because
// each suite registers its own randomized namespace through the frontend
// RegisterNamespace RPC (the conformanceMetadataManager adapter in
// tokeira_conformance_cluster.go forwards setupCluster's namespace write to that
// RPC). A shared in-memory `tokeirad` is the default for speed and hermeticity
// (design: "in-memory storage for speed/hermeticity"; DSQL is a separate
// opt-in variant).
//
// Boot-or-reuse. If TOKEIRA_CONFORMANCE_FRONTEND_ADDR is already set, the runner
// assumes the operator manages the `tokeirad` lifecycle and runs the corpus
// against that address as-is. Otherwise it boots a `tokeirad` from TOKEIRA_BIN on
// a free loopback port with a minimal in-memory TOML, waits until the frontend
// genuinely serves WorkflowService, runs the corpus, and tears the process down.
// That boot-or-reuse lifecycle is shared with the single-suite runner via the
// tests/tokeira_conformance_runner package, so both agree on how tokeirad is
// stood up and how the bridges and Go toolchain are wired.
//
// This is a standalone `main` in its own package directory, so `go test ./tests/`
// (non-recursive, the corpus) never builds or runs it; it is explicitly invoked
// by an operator (manual initially, per R2.8).
package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"go.temporal.io/server/tests/testcore"
	runner "go.temporal.io/server/tests/tokeira_conformance_runner"
)

const (
	// resultsPathEnv optionally overrides where the machine-readable `go test
	// -json` event stream is written. The report consumes this stream to build
	// the per-test ledger.
	resultsPathEnv = "TOKEIRA_CONFORMANCE_RESULTS"

	// defaultResultsPath is the default destination for the -json event stream.
	defaultResultsPath = "tokeira-conformance-results.json"
	// corpusPattern is the package selector for the pinned corpus. It is
	// deliberately the flat `./tests/` package (non-recursive) — exactly the set
	// of functional test files, with no `...` that would pull in this runner or
	// testcore helpers as test targets.
	corpusPattern = "./tests/"
	// perTestTimeout is the `go test -timeout` applied to each isolated
	// entrypoint process. It bounds a single hung entrypoint without capping the
	// whole corpus; an entrypoint that exceeds it is killed by `go test` and
	// recorded as a failure (data), and the runner moves on to the next.
	perTestTimeout = 5 * time.Minute
	// Versioning3 expands several suite methods into forwarding-mode matrices and
	// makes steady progress well beyond the ordinary entrypoint bound. Keep it in
	// one isolated process, but allow the matrix to finish so the ledger does not
	// misclassify its unvisited leaves as unfinished.
	versioning3Timeout = 30 * time.Minute
)

func timeoutForEntrypoint(name string) time.Duration {
	if name == "TestVersioning3FunctionalSuite" {
		return versioning3Timeout
	}
	return perTestTimeout
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "tokeira-conformance-runall: %v\n", err)
		os.Exit(1)
	}
}

// run boots-or-reuses a `tokeirad` frontend, executes the full corpus against it,
// and tears down anything it started. It returns an error only for harness-level
// failures (no binary, frontend never came up, `go test` could not be launched).
// Test *failures* are not harness failures — they are the expected data of a
// conformance run — so a corpus that runs to completion with failing tests still
// returns nil here; the failing outcomes live in the -json results for the
// report to classify.
func run() error {
	proc, addr, metricsAddr, controlAddr, authCallbackURL, err := runner.BootOrReuse()
	if err != nil {
		return err
	}
	if proc != nil {
		defer proc.Stop()
		// Terminate a booted tokeirad cleanly if the operator interrupts the run.
		runner.InstallSignalCleanup(proc)
		fmt.Printf("tokeira-conformance-runall: tokeirad serving at %s\n", addr)
	} else {
		fmt.Printf("tokeira-conformance-runall: reusing operator-managed frontend at %s\n", addr)
	}

	resultsPath := os.Getenv(resultsPathEnv)
	if resultsPath == "" {
		resultsPath = defaultResultsPath
	}

	return runCorpus(addr, metricsAddr, controlAddr, authCallbackURL, resultsPath)
}

// runCorpus enumerates the complete set of top-level entrypoints in the pinned
// corpus and runs each in its own isolated `go test` process, with the seam
// address exported so every suite resolves to the shared `tokeirad`.
//
// This is the run-all loop. It first discovers the full entrypoint set via
// listEntrypoints (`go test -list`), fails loudly if that set is empty (the
// anti-exclusion guard: an empty corpus is a harness failure, never a silent
// pass), then invokes each entrypoint under `-run '^Name$'` in its own process.
// Per-process isolation is what makes "run everything" robust: a panicking
// entrypoint crashes only its own process, and the loop proceeds to the next, so
// no entrypoint can exclude the ones scheduled after it.
//
// `-run '^Name$'` is an exact anchored match — an isolation boundary, not a
// curated subset. The runner runs *every* discovered name; it has no allow/deny
// list. `-json` makes per-test and `t.Run` sub-test outcomes machine-readable
// for the ledger (task 8.2); `-count=1` defeats the test cache; `-tags test_dep`
// matches the fork's required build tag. Each entrypoint's `-json` events are
// concatenated into resultsPath (one combined stream the report consumes) and
// echoed to stdout for live operator visibility.
//
// Failing or panicking entrypoints are expected (failures are data) and never
// become a harness error; runCorpus returns an error only for harness-level
// faults (enumeration failed, empty corpus, results file unwritable).
func runCorpus(addr, metricsAddr, controlAddr, authCallbackURL, resultsPath string) error {
	entrypoints, err := listEntrypoints(addr)
	if err != nil {
		return err
	}
	// Anti-exclusion guard: a corpus that discovers zero entrypoints is a
	// harness failure, not a clean run. Property 6 requires the full set to
	// execute; an empty set means enumeration is broken (wrong package, build
	// failure), and silently "passing" would hide a total exclusion.
	if len(entrypoints) == 0 {
		return fmt.Errorf("no test entrypoints discovered in %q: refusing to report an empty corpus as a run", corpusPattern)
	}

	results, err := os.Create(resultsPath)
	if err != nil {
		return fmt.Errorf("create results file %q: %w", resultsPath, err)
	}
	defer func() { _ = results.Close() }()

	fmt.Printf("tokeira-conformance-runall: discovered %d entrypoints; running each in isolation -> %s\n",
		len(entrypoints), resultsPath)

	var ran, failed int
	for i, name := range entrypoints {
		fmt.Printf("tokeira-conformance-runall: [%d/%d] %s\n", i+1, len(entrypoints), name)
		if runEntrypoint(addr, metricsAddr, controlAddr, authCallbackURL, name, results) == entrypointFailed {
			failed++
		}
		ran++
	}

	fmt.Printf("tokeira-conformance-runall: corpus complete — %d entrypoints ran, %d failed. results: %s\n",
		ran, failed, resultsPath)
	return nil
}

// entrypointOutcome is the coarse per-process result the run-all loop tracks for
// its operator summary. Fine-grained classification (pass / real-gap /
// deliberate-deviation / out-of-public-scope), including per-`t.Run` sub-tests
// and telling a panic apart from an assertion failure, is the report's job
// (tasks 9–10) reading the -json stream; this is only the harness's running
// tally. A panic and an assertion failure both surface here as a non-zero
// `go test` exit and are tallied identically as entrypointFailed.
type entrypointOutcome int

const (
	entrypointPassed entrypointOutcome = iota
	entrypointFailed
)

// runEntrypoint runs a single top-level entrypoint in its own `go test` process
// under an exact `-run` anchor, appending its -json events to results and
// echoing them to stdout. A non-zero exit (test failure) and a panic-induced
// crash are both expected outcomes recorded as data; only the coarse outcome is
// returned for the operator tally. The process is fully isolated, so a panic
// here cannot affect any other entrypoint.
func runEntrypoint(addr, metricsAddr, controlAddr, authCallbackURL, name string, results io.Writer) entrypointOutcome {
	args := []string{
		"test",
		"-tags", "test_dep",
		"-json",
		"-count=1",
		// One entrypoint may still contain parallel-suite leaves. The shared
		// Tokeira process cannot namespace unlabeled metric counters, whereas
		// Temporal's dedicated clusters isolate them, so preserve that isolation
		// by running one leaf at a time.
		"-parallel=1",
		"-timeout", timeoutForEntrypoint(name).String(),
		"-run", "^" + name + "$",
	}
	// Skip the registered out-of-scope sub-tests under this entrypoint (raw t.Run
	// cases testify's SetupSubTest cannot intercept — e.g. dynamic-config-override
	// assertions). `-skip` excludes only the named leaves, never their siblings;
	// the skipped outcomes still appear in the -json stream for the ledger.
	if skip := testcore.ConformanceSkipRegexp(name); skip != "" {
		args = append(args, "-skip", skip)
	}
	args = append(args, corpusPattern)
	cmd := exec.Command("go", args...)
	// Pin the toolchain to the version the corpus's go.mod requires so runs do not
	// silently use whatever `go` is first on PATH.
	cmd.Env = append(os.Environ(), runner.SeamAddrEnv+"="+addr, "GOTOOLCHAIN="+runner.GoToolchain)
	// Hand the corpus tokeirad's /metrics address so the conformance cluster stands up the
	// scrape-backed CaptureMetricsHandler for metric-asserting tests; omitted when unknown.
	if metricsAddr != "" {
		cmd.Env = append(cmd.Env, runner.MetricsAddrEnv+"="+metricsAddr)
	}
	// Hand the corpus tokeirad's control-service address so the dynamic-config bridge can deliver
	// OverrideDynamicConfig; omitted when unknown (the bridge then no-ops).
	if controlAddr != "" {
		cmd.Env = append(cmd.Env, runner.ControlAddrEnv+"="+controlAddr)
	}
	if authCallbackURL != "" {
		cmd.Env = append(cmd.Env, runner.AuthCallbackURLEnv+"="+authCallbackURL)
	}
	cmd.Stdout = io.MultiWriter(results, os.Stdout)
	cmd.Stderr = os.Stderr

	if runErr := cmd.Run(); runErr != nil {
		if _, ok := runErr.(*exec.ExitError); !ok {
			// Could not launch `go test` at all — surface it but keep going so
			// one launch hiccup does not abort the corpus.
			fmt.Fprintf(os.Stderr, "tokeira-conformance-runall: launch go test for %q: %v\n", name, runErr)
		}
		// Non-zero exit (failure or panic crash) is expected data; the report
		// splits failure-vs-panic from the -json stream.
		return entrypointFailed
	}
	return entrypointPassed
}

// listEntrypoints discovers the complete set of top-level test entrypoints in
// the corpus via `go test -list '.*'`, which prints every matching top-level
// Test/Benchmark/Fuzz function name (one per line) without running any. This is
// the enumeration that the run-all loop iterates: it is the authoritative full
// set, so the loop runs exactly what the corpus declares, nothing curated.
//
// Only lines that are valid Go test identifiers beginning with "Test" are kept;
// `go test -list` also prints a trailing "ok <pkg> <elapsed>" summary line and
// (potentially) benchmark/fuzz names, which are filtered out so the loop runs
// the functional Test entrypoints. The seam env is exported here too so listing
// builds against the same configuration the run uses.
func listEntrypoints(addr string) ([]string, error) {
	cmd := exec.Command("go", "test", "-tags", "test_dep", "-list", ".*", corpusPattern)
	cmd.Env = append(os.Environ(), runner.SeamAddrEnv+"="+addr, "GOTOOLCHAIN="+runner.GoToolchain)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("enumerate entrypoints via `go test -list`: %w", err)
	}

	var names []string
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		// Keep only top-level Test entrypoints; skip the trailing "ok ..."
		// summary line and any Benchmark/Fuzz/Example names.
		if strings.HasPrefix(line, "Test") {
			names = append(names, line)
		}
	}
	return names, nil
}
