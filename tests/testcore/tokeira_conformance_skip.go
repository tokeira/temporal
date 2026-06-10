package testcore

import "strings"

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
