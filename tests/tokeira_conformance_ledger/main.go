// Command tokeira-conformance-ledger distills a Go `test2json` event stream
// into the per-test outcome capture that seeds the Tokeira Tier-2 ledger.
//
// The run-all executor (tests/tokeira_conformance_runall) emits a combined
// `go test -json` stream over the whole pinned corpus. That stream is faithful
// but verbose (one event per line, many per test). This command reduces it to
// exactly one outcome row per test — at per-test granularity *including every
// `t.Run` sub-test* — which is the shape the Rust report joins against the
// compatibility matrix and the per-test ledger (temporal-functional-conformance
// tasks 1.2, 8.2, 9.2).
//
// Capture vs. classification. This tool performs *capture* only: it records what
// happened (pass / fail / skip / unfinished), never *why* or into which ledger
// category. Classification into {pass, real-gap, deliberate-deviation,
// out-of-public-scope} is the report's job (task 9.2) and an operator-authored
// classification source; conflating the two here would bake interpretation into
// the raw evidence. Keeping capture mechanical is what lets the same outcome
// document be re-classified without re-running the corpus.
//
// Per-test granularity is load-bearing. The design keys the ledger at package +
// full test name including `t.Run` sub-test names, so a single failing sub-test
// in an otherwise-passing file is classified on its own and a real-gap fix flips
// exactly the tests it resolves. `test2json`'s `Test` field already carries the
// full slash-separated sub-test path (e.g. `TestSuite/TestCase`), so each
// sub-test is a distinct key here with its own outcome; this tool does not roll
// sub-tests up into their parent.
//
// The `unfinished` outcome and why it exists. Under the run-all's per-entrypoint
// process isolation a panicking test crashes its own `go test` process. The
// panicking test itself gets a terminal `fail` from `test2json`, but sibling
// sub-tests that had started (`run`) and not yet reported are left with no
// terminal event. Recording those as `unfinished` (rather than silently dropping
// them) preserves the run-all posture that every test that *ran* is accounted
// for — an unfinished test is data the report must classify, not an absence.
//
// Standalone `main` in its own package directory so `go test ./tests/` never
// builds it. It reads the stream from a file (first CLI arg) or stdin, and
// writes the outcome document to a file (second CLI arg) or stdout.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

// testEvent is the subset of the `go test -json` (test2json) event schema this
// tool needs. The full schema carries more fields (Output, Elapsed at package
// scope, etc.); only these four bear on per-test outcome capture, and unknown
// fields are ignored by encoding/json.
type testEvent struct {
	// Action is the event kind: run, pause, cont, pass, fail, skip, output, ...
	Action string `json:"Action"`
	// Package is the Go import path under test, e.g. go.temporal.io/server/tests.
	Package string `json:"Package"`
	// Test is the test name, including the full slash-separated `t.Run` sub-test
	// path. Empty for package-scoped events (which this tool ignores for
	// per-test capture).
	Test string `json:"Test"`
	// Elapsed is the test's wall-clock seconds, present on terminal events.
	Elapsed float64 `json:"Elapsed"`
}

// outcome is the captured terminal result of a single test or sub-test.
type outcome string

const (
	// outcomePass / outcomeFail / outcomeSkip mirror test2json's terminal
	// actions verbatim.
	outcomePass outcome = "pass"
	outcomeFail outcome = "fail"
	outcomeSkip outcome = "skip"
	// outcomeUnfinished marks a test that emitted `run` but no terminal action —
	// the signature of a sibling lost to a panic-induced process crash under the
	// run-all's per-entrypoint isolation. It is captured, never dropped.
	outcomeUnfinished outcome = "unfinished"
)

// testOutcome is one row of the capture document: a per-test-granular identity
// and its terminal outcome. This is the shape the Rust report (task 9.2)
// deserializes and joins.
type testOutcome struct {
	// TestID is `<package>/<Test>` — the package import path joined to the full
	// sub-test path. This is the per-test key the ledger is keyed on.
	TestID string `json:"test_id"`
	// Outcome is the captured terminal result.
	Outcome outcome `json:"outcome"`
	// ElapsedSeconds is the test's wall-clock duration from its terminal event,
	// or 0 for an unfinished test.
	ElapsedSeconds float64 `json:"elapsed_seconds"`
}

// outcomeDocument is the top-level capture artifact: every test that ran,
// exactly once, with its outcome. It is intentionally a thin container — run
// metadata (pins, wire coverage) lives in other Tier-2 artifacts and is joined
// in the report, not duplicated here.
type outcomeDocument struct {
	Outcomes []testOutcome `json:"outcomes"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "tokeira-conformance-ledger: %v\n", err)
		os.Exit(1)
	}
}

// run wires input/output (file args or stdin/stdout) and drives the distillation.
func run(args []string) error {
	in := os.Stdin
	if len(args) >= 1 && args[0] != "-" {
		f, err := os.Open(args[0])
		if err != nil {
			return fmt.Errorf("open input %q: %w", args[0], err)
		}
		defer func() { _ = f.Close() }()
		in = f
	}

	doc, err := distill(in)
	if err != nil {
		return err
	}

	out := os.Stdout
	if len(args) >= 2 && args[1] != "-" {
		f, err := os.Create(args[1])
		if err != nil {
			return fmt.Errorf("create output %q: %w", args[1], err)
		}
		defer func() { _ = f.Close() }()
		out = f
	}

	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return fmt.Errorf("encode outcome document: %w", err)
	}
	fmt.Fprintf(os.Stderr, "tokeira-conformance-ledger: captured %d test outcomes\n", len(doc.Outcomes))
	return nil
}

// distill reads a test2json stream and reduces it to one outcome per test.
//
// It tracks, per test key, whether the test was seen `run`ning and its latest
// terminal action. A terminal action (pass/fail/skip) sets the final outcome; a
// test seen running but never terminated is captured as `unfinished`. The last
// terminal action wins, which is correct for `-count=1` (a single execution per
// test); the tool does not attempt to reconcile multiple counts because the
// run-all always runs `-count=1`.
//
// Malformed lines (not valid JSON objects) are skipped rather than aborting:
// the stream is tee'd through other writers during a run and may carry the
// occasional non-event line, and capture must be robust to that without losing
// the well-formed majority.
func distill(r io.Reader) (outcomeDocument, error) {
	// state per test key: did we see it run, and what terminal outcome (if any).
	type state struct {
		seenRun  bool
		terminal outcome // "" until a terminal action is observed
		elapsed  float64
	}
	states := make(map[string]*state)

	scanner := bufio.NewScanner(r)
	// test2json lines can be long (they embed test output); raise the buffer
	// cap well above bufio's 64KiB default so a long line is not a read error.
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var ev testEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}
		// Per-test capture ignores package-scoped events (empty Test).
		if ev.Test == "" {
			continue
		}

		key := ev.Package + "/" + ev.Test
		st := states[key]
		if st == nil {
			st = &state{}
			states[key] = st
		}

		switch ev.Action {
		case "run":
			st.seenRun = true
		case "pass":
			st.terminal = outcomePass
			st.elapsed = ev.Elapsed
		case "fail":
			st.terminal = outcomeFail
			st.elapsed = ev.Elapsed
		case "skip":
			st.terminal = outcomeSkip
			st.elapsed = ev.Elapsed
		default:
			// pause / cont / output and any future action: not terminal, ignored.
		}
	}
	if err := scanner.Err(); err != nil {
		return outcomeDocument{}, fmt.Errorf("read test2json stream: %w", err)
	}

	outcomes := make([]testOutcome, 0, len(states))
	for key, st := range states {
		o := st.terminal
		if o == "" {
			// Seen running (or only referenced) but never terminated: the
			// panic-crash signature. Captured as unfinished so it is accounted
			// for, never silently dropped.
			o = outcomeUnfinished
		}
		outcomes = append(outcomes, testOutcome{
			TestID:         key,
			Outcome:        o,
			ElapsedSeconds: st.elapsed,
		})
	}
	// Deterministic order so the document is stable across runs (diffable,
	// reviewable) regardless of map iteration order.
	sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].TestID < outcomes[j].TestID })

	return outcomeDocument{Outcomes: outcomes}, nil
}
