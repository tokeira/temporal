// Command tokeira-conformance-runsuite is the operator-invokable single-suite
// runner for Tokeira Tier-2 functional conformance — the iteration counterpart
// to the run-all executor (tokeira_conformance_runall).
//
// Where run-all executes the entire pinned corpus for the baseline (and, by
// design, refuses to subset), this command runs ONE suite (or an arbitrary
// -run regexp) against a booted-or-reused tokeirad and prints a per-leaf
// PASS/FAIL/SKIP tally — the fast inner loop while driving a single tier green.
// It replaces the throwaway run_suite.sh bash harness with a first-class,
// committed Go entrypoint that shares the exact boot lifecycle run-all uses
// (tests/tokeira_conformance_runner), so the two agree on how tokeirad is stood
// up, how the metrics/control bridges are wired, and which Go toolchain the
// corpus runs under.
//
// It is a standalone main in its own package directory, so `go test ./tests/`
// (the corpus) never builds or runs it; it is explicitly invoked by an operator.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"go.temporal.io/server/tests/testcore"
	runner "go.temporal.io/server/tests/tokeira_conformance_runner"
)

// corpusPattern is the flat, non-recursive corpus package selector — exactly the
// functional test files, with no `...` that would pull in this runner or the
// testcore helpers as test targets.
const corpusPattern = "./tests/"

func main() {
	code, err := run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "tokeira-conformance-runsuite: %v\n", err)
	}
	// os.Exit only after run() has returned, so its deferred tokeirad teardown
	// runs (os.Exit does not honour defers).
	os.Exit(code)
}

// run boots-or-reuses a tokeirad frontend, runs the corpus filtered to the
// operator's -run pattern once, prints a per-leaf tally, and returns the process
// exit code plus any harness-level error. A failing suite is data, not a harness
// error: it yields exit code 1 with a nil error (the tally is still printed),
// while a harness fault (no binary/frontend, bad usage) returns a non-nil error.
func run() (int, error) {
	timeout := flag.Duration("timeout", 8*time.Minute, "per-run `go test -timeout` applied to the suite")
	flag.Parse()
	pattern := flag.Arg(0)
	if pattern == "" {
		return 2, fmt.Errorf(
			"usage: tokeira-conformance-runsuite [-timeout DUR] '<go-test-run-regexp>' " +
				"(e.g. '^TestWFTFailureReportedProblemsTestSuite$')")
	}

	proc, addr, metricsAddr, controlAddr, authCallbackURL, err := runner.BootOrReuse()
	if err != nil {
		return 1, err
	}
	if proc != nil {
		defer proc.Stop()
		runner.InstallSignalCleanup(proc)
		fmt.Printf("tokeira-conformance-runsuite: tokeirad serving at %s\n", addr)
	} else {
		fmt.Printf("tokeira-conformance-runsuite: reusing operator-managed frontend at %s\n", addr)
	}

	outcome := runSuite(addr, metricsAddr, controlAddr, authCallbackURL, pattern, *timeout)
	printSummary(pattern, outcome)
	if !outcome.ok() {
		return 1, nil
	}
	return 0, nil
}

// suiteOutcome is the per-leaf tally scraped from a single suite's `go test -v`
// stream, plus whether the `go test` process itself exited non-zero.
type suiteOutcome struct {
	pass, fail, skip int
	failLeaves       []string
	testExitErr      bool
}

// ok reports a clean run: the process exited zero and no leaf failed. Both are
// checked because a suite can fail while the top-level tally shows no `--- FAIL`
// (e.g. a build/setup error), and a leaf can fail without the process crashing.
func (o suiteOutcome) ok() bool { return !o.testExitErr && o.fail == 0 }

// runSuite runs the corpus filtered to `pattern` in a single `go test` process
// against the shared frontend, teeing the -v stream to stdout for live
// visibility while scraping the per-leaf tally from it.
//
// The out-of-scope raw t.Run leaves for the pattern's top-level entrypoint are
// excluded via the skip registry — the same -skip run-all applies — so a
// single-suite run classifies identically to the baseline; -skip excludes only
// the named leaves, never their siblings.
func runSuite(addr, metricsAddr, controlAddr, authCallbackURL, pattern string, timeout time.Duration) suiteOutcome {
	args := []string{
		"test", corpusPattern,
		"-tags", "test_dep",
		"-count=1",
		// Temporal's dedicated clusters isolate unlabeled metrics. Shape-2 shares
		// one tokeirad, so serialize parallel-suite leaves to preserve that same
		// observable capture boundary without inventing metric labels.
		"-parallel=1",
		"-timeout", timeout.String(),
		"-v",
		"-run", pattern,
	}
	if skip := testcore.ConformanceSkipRegexp(entrypointOf(pattern)); skip != "" {
		args = append(args, "-skip", skip)
	}
	cmd := exec.Command("go", args...)
	// Pin the toolchain and hand the corpus the shared frontend + bridge
	// addresses, exactly as run-all does for each entrypoint.
	cmd.Env = append(os.Environ(), runner.SeamAddrEnv+"="+addr, "GOTOOLCHAIN="+runner.GoToolchain)
	if metricsAddr != "" {
		cmd.Env = append(cmd.Env, runner.MetricsAddrEnv+"="+metricsAddr)
	}
	if controlAddr != "" {
		cmd.Env = append(cmd.Env, runner.ControlAddrEnv+"="+controlAddr)
	}
	if authCallbackURL != "" {
		cmd.Env = append(cmd.Env, runner.AuthCallbackURLEnv+"="+authCallbackURL)
	}

	var buf bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &buf)
	cmd.Stderr = os.Stderr

	var outcome suiteOutcome
	if runErr := cmd.Run(); runErr != nil {
		// A non-zero exit (a failing or panicking suite) is expected data, not a
		// harness fault; record it so the runner's exit code reflects it.
		outcome.testExitErr = true
	}
	outcome.pass, outcome.fail, outcome.skip, outcome.failLeaves = tally(&buf)
	return outcome
}

// tally scrapes the `go test -v` stream for leaf-level outcomes. It counts the
// testing package's "--- PASS/FAIL/SKIP:" markers (which carry optional leading
// indentation for sub-tests) and collects the FAIL leaf names.
func tally(r io.Reader) (pass, fail, skip int, failLeaves []string) {
	sc := bufio.NewScanner(r)
	// Suite -v output can carry long lines (panics, encoded payloads); raise the
	// scanner's line cap well above the 64 KiB default so scraping never stalls.
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case strings.HasPrefix(line, "--- PASS:"):
			pass++
		case strings.HasPrefix(line, "--- FAIL:"):
			fail++
			failLeaves = append(failLeaves, strings.TrimSpace(strings.TrimPrefix(line, "--- FAIL:")))
		case strings.HasPrefix(line, "--- SKIP:"):
			skip++
		}
	}
	return pass, fail, skip, failLeaves
}

// printSummary renders the run_suite.sh-equivalent tally block: PASS/FAIL/SKIP
// counts, the FAIL leaf names, and an overall result line.
func printSummary(pattern string, o suiteOutcome) {
	fmt.Printf("\n===== tokeira-conformance-runsuite tally (%s) =====\n", pattern)
	fmt.Printf("PASS %d\nFAIL %d\nSKIP %d\n", o.pass, o.fail, o.skip)
	if len(o.failLeaves) > 0 {
		fmt.Println("----- FAIL leaves -----")
		for _, leaf := range o.failLeaves {
			fmt.Printf("  %s\n", leaf)
		}
	}
	if o.ok() {
		fmt.Println("result: PASS")
	} else {
		fmt.Println("result: FAIL")
	}
}

// entrypointOf extracts the top-level Test entrypoint name from a `go test -run`
// regexp so the skip registry can be consulted for it: strip a leading '^', then
// take everything up to the first '/' (sub-test separator) or '$' (anchor). A
// pattern that does not name a concrete entrypoint yields no registered skip,
// which is safe (the harness's SetupSubTest hooks still apply testify skips).
func entrypointOf(pattern string) string {
	p := strings.TrimPrefix(pattern, "^")
	if i := strings.IndexAny(p, "/$"); i >= 0 {
		p = p[:i]
	}
	return p
}
