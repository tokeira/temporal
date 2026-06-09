package main

import (
	"strings"
	"testing"
)

// TestDistill_CapturesTerminalOutcomes verifies the core reduction: a test2json
// stream collapses to exactly one outcome per per-test key, terminal actions are
// captured verbatim, and a test seen running but never terminated (the
// panic-crash sibling signature) is captured as `unfinished` rather than dropped.
func TestDistill_CapturesTerminalOutcomes(t *testing.T) {
	// A representative stream: a passing sub-test, a failing sub-test, a skip,
	// and an "unfinished" sub-test that ran but whose process died before a
	// terminal event (no pass/fail/skip line follows its run).
	stream := strings.Join([]string{
		`{"Action":"run","Package":"go.temporal.io/server/tests","Test":"TestSuite/TestPasses"}`,
		`{"Action":"output","Package":"go.temporal.io/server/tests","Test":"TestSuite/TestPasses","Output":"ok\n"}`,
		`{"Action":"pass","Package":"go.temporal.io/server/tests","Test":"TestSuite/TestPasses","Elapsed":0.5}`,
		`{"Action":"run","Package":"go.temporal.io/server/tests","Test":"TestSuite/TestFails"}`,
		`{"Action":"fail","Package":"go.temporal.io/server/tests","Test":"TestSuite/TestFails","Elapsed":0.1}`,
		`{"Action":"run","Package":"go.temporal.io/server/tests","Test":"TestSuite/TestSkipped"}`,
		`{"Action":"skip","Package":"go.temporal.io/server/tests","Test":"TestSuite/TestSkipped","Elapsed":0.0}`,
		`{"Action":"run","Package":"go.temporal.io/server/tests","Test":"TestSuite/TestUnfinished"}`,
		// package-scoped event (empty Test) must be ignored for per-test capture
		`{"Action":"fail","Package":"go.temporal.io/server/tests","Elapsed":1.2}`,
		// a malformed line must be skipped, not abort the parse
		`this is not json`,
	}, "\n")

	doc, err := distill(strings.NewReader(stream))
	if err != nil {
		t.Fatalf("distill: %v", err)
	}

	got := make(map[string]outcome, len(doc.Outcomes))
	for _, o := range doc.Outcomes {
		got[o.TestID] = o.Outcome
	}

	want := map[string]outcome{
		"go.temporal.io/server/tests/TestSuite/TestPasses":     outcomePass,
		"go.temporal.io/server/tests/TestSuite/TestFails":      outcomeFail,
		"go.temporal.io/server/tests/TestSuite/TestSkipped":    outcomeSkip,
		"go.temporal.io/server/tests/TestSuite/TestUnfinished": outcomeUnfinished,
	}

	if len(got) != len(want) {
		t.Fatalf("captured %d outcomes, want %d: %#v", len(got), len(want), got)
	}
	for id, wantOutcome := range want {
		if got[id] != wantOutcome {
			t.Errorf("test_id %q: got %q, want %q", id, got[id], wantOutcome)
		}
	}
}

// TestDistill_SubTestsAreIndependentKeys verifies per-test granularity: a parent
// suite entrypoint and each of its `t.Run` sub-tests are distinct keys with
// independent outcomes, so a single failing sub-test never tars a passing
// sibling.
func TestDistill_SubTestsAreIndependentKeys(t *testing.T) {
	stream := strings.Join([]string{
		`{"Action":"run","Package":"pkg","Test":"TestParent"}`,
		`{"Action":"run","Package":"pkg","Test":"TestParent/SubA"}`,
		`{"Action":"pass","Package":"pkg","Test":"TestParent/SubA","Elapsed":0.1}`,
		`{"Action":"run","Package":"pkg","Test":"TestParent/SubB"}`,
		`{"Action":"fail","Package":"pkg","Test":"TestParent/SubB","Elapsed":0.2}`,
		`{"Action":"fail","Package":"pkg","Test":"TestParent","Elapsed":0.3}`,
	}, "\n")

	doc, err := distill(strings.NewReader(stream))
	if err != nil {
		t.Fatalf("distill: %v", err)
	}

	got := make(map[string]outcome, len(doc.Outcomes))
	for _, o := range doc.Outcomes {
		got[o.TestID] = o.Outcome
	}

	if got["pkg/TestParent/SubA"] != outcomePass {
		t.Errorf("SubA: got %q, want pass", got["pkg/TestParent/SubA"])
	}
	if got["pkg/TestParent/SubB"] != outcomeFail {
		t.Errorf("SubB: got %q, want fail", got["pkg/TestParent/SubB"])
	}
	if got["pkg/TestParent"] != outcomeFail {
		t.Errorf("parent: got %q, want fail", got["pkg/TestParent"])
	}
}
