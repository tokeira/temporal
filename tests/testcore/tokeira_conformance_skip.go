package testcore

import (
	"regexp"
	"strings"
)

// Conformance skip registry.
//
// The Tier-2 functional conformance harness replays Temporal's UNMODIFIED Go
// corpus over the real gRPC wire against an external `tokeirad`. A small number
// of corpus tests cannot run in that mode because they depend on
// `OverrideDynamicConfig`, which the harness cannot deliver to an out-of-process
// server (it writes to the in-process onebox MemoryClient). Rather than edit the
// corpus, we skip these by name in the shared SetupTest/SetupSubTest hooks, and
// ONLY when conformance mode is active — a normal Temporal test run is
// unaffected.
//
// Each entry records WHY the test is out of scope so the skip is auditable.
// Keys are matched as substrings of `t.Name()` (which is
// `Suite/Test[/subtest]`), so a parent name skips its subtests too.

type conformanceSkip struct {
	// nameContains matches against the full `t.Name()`.
	nameContains string
	// reason is surfaced in the skip message.
	reason string
}

var conformanceSkips = []conformanceSkip{
	{
		nameContains: "TestWorkerDeploymentSuite/TestDeploymentVersionLimits",
		reason: "requires OverrideDynamicConfig(MatchingMaxVersionsInDeployment=1); " +
			"tokeira does not support dynamic-config injection over the wire and its " +
			"default already matches v1.31.0 (100)",
	},
	{
		nameContains: "TestWorkerDeploymentSuite/TestDeleteVersion_ServerDeleteMaxVersionsReached",
		reason: "requires OverrideDynamicConfig(MatchingMaxVersionsInDeployment=1); " +
			"tokeira does not support dynamic-config injection over the wire and its " +
			"default already matches v1.31.0 (100)",
	},
	{
		nameContains: "TestWorkerDeploymentSuite/TestSetRampingVersion_AfterDrained",
		reason: "depends on suite-level OverrideDynamicConfig of " +
			"VersionDrainageStatusRefreshInterval / VisibilityGracePeriod (defaults 3m) to " +
			"drain a demoted version within the 10s assertion window; tokeira does not " +
			"support dynamic-config injection over the wire",
	},
	{
		nameContains: "TestWorkerDeploymentSuite/TestDrainRollbackedVersion",
		reason: "depends on suite-level OverrideDynamicConfig of " +
			"VersionDrainageStatusRefreshInterval / VisibilityGracePeriod (defaults 3m) to " +
			"drive Draining→Drained within the assertion window; tokeira does not " +
			"support dynamic-config injection over the wire",
	},
	{
		nameContains: "TestWorkerDeploymentSuite/TestForceCAN_WithOverrideState",
		reason: "injects the server's internal deployment entity-workflow state " +
			"(deploymentspb.ForceCANDeploymentSignalArgs.OverrideState, a WorkerDeploymentLocalState) " +
			"via signal — an internal-surface representation tokeira does not model, not a " +
			"public API behaviour",
	},
	{
		nameContains: "TestStandaloneActivityTestSuite/TestStart/RequestValidations/InputTooLarge",
		reason: "asserts at an OverrideDynamicConfig(BlobSizeLimitError=1000) value; tokeira " +
			"represents the limit as the pinned-release constant and does not accept dynamic-config " +
			"injection over the wire, so the 1001-byte input cannot trip the constant default",
	},
	{
		nameContains: "TestStandaloneActivityTestSuite/TestRequestCancel/RequestValidations/ReasonTooLong",
		reason: "asserts at an OverrideDynamicConfig(BlobSizeLimitError=1000) value; tokeira " +
			"represents the limit as the pinned-release constant and does not accept dynamic-config " +
			"injection over the wire, so the 1001-byte reason cannot trip the constant default",
	},
	{
		nameContains: "TestStandaloneActivityTestSuite/TestTerminate/RequestValidations/ReasonTooLong",
		reason: "asserts at an OverrideDynamicConfig(BlobSizeLimitError=1000) value; tokeira " +
			"represents the limit as the pinned-release constant and does not accept dynamic-config " +
			"injection over the wire, so the 1001-byte reason cannot trip the constant default",
	},
}

// conformanceSkipReason returns the skip reason for a test name when one is
// registered, and whether a match was found.
func conformanceSkipReason(testName string) (string, bool) {
	for _, skip := range conformanceSkips {
		if strings.Contains(testName, skip.nameContains) {
			return skip.reason, true
		}
	}
	return "", false
}

// maybeSkipForConformance skips the current test when conformance mode is active
// and the test is in the skip registry. A no-op outside conformance mode.
func (s *FunctionalTestBase) maybeSkipForConformance() {
	if conformanceFrontendAddr() == "" {
		return
	}
	if reason, ok := conformanceSkipReason(s.T().Name()); ok {
		s.T().Skipf("tokeira conformance: skipping %s — %s", s.T().Name(), reason)
	}
}

// ConformanceSkipRegexp builds the `go test -skip` regular expression that skips
// every registered out-of-scope test under the given top-level entrypoint (the
// `^Name$` the run-all runner isolates per process). It returns "" when no
// registered skip targets that entrypoint.
//
// Why a `-skip` regexp and not the SetupTest/SetupSubTest hook above: testify
// only invokes SetupSubTest from suite.Run, but most corpus suites nest their
// cases with the standard library's raw t.Run, which testify cannot intercept —
// so maybeSkipForConformance never fires inside such a sub-test. `go test -skip`
// (Go 1.20+) skips by name at the testing layer regardless of how the sub-test
// was started, and — verified — skips only the matched leaf, never its parent or
// siblings. The two mechanisms are complementary: the hook gives a per-test Skip
// message for whole methods / s.Run sub-tests, the regexp reaches raw t.Run ones.
//
// Positional matching. `go test` splits both the test identifier and the skip
// regexp on unbracketed '/', matching element-by-element. Entries under one
// entrypoint are therefore merged into a per-position alternation (each element
// anchored and de-duplicated). When entries differ in two or more positions this
// admits phantom cross-products (A/x + B/y also matches A/y), but those name a
// test that does not exist, so they are inert — and a wrongly-skipped *real* test
// would surface as a `skip` outcome the ledger gate must classify, so the curated
// registry cannot silently over-skip. Entries under one entrypoint are assumed to
// share depth (they do today: per-method or per-leaf, never mixed).
func ConformanceSkipRegexp(entrypoint string) string {
	var positions [][]string
	seen := []map[string]bool{}
	for _, skip := range conformanceSkips {
		parts := strings.Split(skip.nameContains, "/")
		if len(parts) == 0 || parts[0] != entrypoint {
			continue
		}
		for i, part := range parts {
			for len(positions) <= i {
				positions = append(positions, nil)
				seen = append(seen, map[string]bool{})
			}
			elem := "^" + regexp.QuoteMeta(part) + "$"
			if !seen[i][elem] {
				seen[i][elem] = true
				positions[i] = append(positions[i], elem)
			}
		}
	}
	if len(positions) == 0 {
		return ""
	}
	elems := make([]string, len(positions))
	for i, alts := range positions {
		if len(alts) == 1 {
			elems[i] = alts[0]
		} else {
			elems[i] = "(?:" + strings.Join(alts, "|") + ")"
		}
	}
	return strings.Join(elems, "/")
}
