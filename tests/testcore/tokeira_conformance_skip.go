package testcore

import (
	"regexp"
	"strings"
	"testing"
)

// Conformance skip registry.
//
// The Tier-2 functional conformance harness replays Temporal's UNMODIFIED Go
// corpus over the real gRPC wire against an external `tokeirad`. A small number
// of corpus tests cannot run in that mode because they require in-process
// internals, excluded implementation modes, or an override key Tokeira cannot
// honour without violating the kernel boundary. Supported runtime/edge overrides
// are delivered through the conformance control bridge. Rather than edit the
// corpus, we skip the remaining exclusions by name in the shared
// SetupTest/SetupSubTest hooks, and ONLY when conformance mode is active — a
// normal Temporal test run is unaffected.
//
// Each entry records WHY the test is out of scope so the skip is auditable.
// `nameContains` matches `t.Name()` on path-segment boundaries (see
// conformanceSkipReason): the exact test, or any sub-test beneath it
// (`Name/...`). A parent name therefore skips its sub-tests, but a leaf never
// skips a sibling that merely shares a textual prefix.

type conformanceSkip struct {
	// nameContains matches against the full `t.Name()`.
	nameContains string
	// reason is surfaced in the skip message.
	reason string
}

const workerVersioningEnabledPathReason = "requires the suite's non-default " +
	"frontend.workerVersioningDataAPIs=true V1 enabled path (and, for versioning rules, " +
	"frontend.workerVersioningRuleAPIs=true); the Tokeira v1.31.0 conformance decision " +
	"targets the stock-default PERMISSION_DENIED behavior and excludes deprecated V1/V2 " +
	"version sets, rules, reachability, and scavenging semantics."

const versioning3InternalRoutingMutationReason = "drives the scenario by calling the internal " +
	"MatchingService SyncDeploymentUserData/GetTaskQueueUserData path to install or roll back exact " +
	"per-task-queue routing revisions (tests/versioning_3_test.go @ v1.31.0). That topology-control " +
	"surface is not a public Temporal API and has no equivalent in Tokeira's runtime-owned Worker " +
	"Deployment registry; the externally observable V3 routing paths remain active."

var conformanceSkips = []conformanceSkip{
	{
		nameContains: "TestVersioning3FunctionalSuite/TestUnpinnedTask_OldDeployment",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromWft_Sticky",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromWft_NoSticky",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromWft_Sticky_ToUnversioned",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromWft_NoSticky_ToUnversioned",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestDoubleTransition",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestDoubleTransition_WithSignal",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestDoubleTransitionFromUnversioned",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestDoubleTransitionFromUnversioned_WithSignal",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestEagerActivity",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromActivity_Sticky",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromActivity_NoSticky",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestIndependentVersionedActivity_Pinned",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestIndependentVersionedActivity_Unpinned",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestIndependentUnversionedActivity_Pinned",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestIndependentUnversionedActivity_Unpinned",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestChildWorkflowInheritance_PinnedParent",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestChildWorkflowInheritance_ParentPinnedByOverride",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestChildWorkflowInheritance_CrossTQ_Inherit",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestDescribeTaskQueueVersioningInfo",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestSyncDeploymentUserDataWithRoutingConfig_Update",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestAutoUpgradeWorkflows_NoBouncingBetweenVersions",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestWorkflowTQLags_DependentActivityStartsTransition",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestActivityTQLags_DependentActivityCompletesOnTheNewVersion",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestChildStartsWithParentRevision_SameTQ_TQLags",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestContinueAsNewOfAutoUpgradeWorkflow_RevisionNumberMechanics",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestWorkflowRetry_AutoUpgrade_NoBounceBack",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestWorkflowRetry_AutoUpgrade_AfterCAN_NoBounceBack",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestWorkflowRetry_AutoUpgrade_ChildNoBounceBack",
		reason:       versioning3InternalRoutingMutationReason,
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestNexusTask_StaysOnCurrentDeployment",
		reason: "dispatches the task through internal MatchingService.DispatchNexusTask and mutates " +
			"Nexus queue routing through SyncDeploymentUserData (tests/versioning_3_test.go @ " +
			"v1.31.0); neither internal service RPC is part of the public compatibility surface.",
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestCheckTaskQueueVersionMembership",
		reason: "directly tests internal MatchingService.CheckTaskQueueVersionMembership " +
			"(tests/versioning_3_test.go @ v1.31.0), an internal history-to-matching validation RPC " +
			"rather than a public WorkflowService contract.",
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestMaxVersionsInTaskQueue",
		reason: "injects versions with internal MatchingService.SyncDeploymentUserData and asserts " +
			"the matching.maxDeployments internal cache limit (tests/versioning_3_test.go and " +
			"common/dynamicconfig/constants.go @ v1.31.0); neither surface is public.",
	},
	{
		nameContains: "TestVersioning3FunctionalSuite/TestVersionedQueueUnload",
		reason: "asserts internal Matching task-queue partition unload/reload and repeatedly reads " +
			"GetTaskQueueUserData (tests/versioning_3_test.go @ v1.31.0); Tokeira has no matching " +
			"partition/cache lifecycle and exposes no public equivalent.",
	},
	{
		nameContains: "TestCallbacksSuiteCHASM",
		reason: "runs the callbacks corpus with EnableChasm and EnableCHASMCallbacks enabled " +
			"(tests/callbacks_test.go SetupSuite @ v1.31.0); CHASM framework internals are " +
			"outside Tokeira's default v1.31.0 compatibility gate " +
			"(docs/conformance/v1.31.0/excluded.md). The HSM sibling remains active.",
	},
	{
		nameContains: "TestNexusApiTestSuiteWithLegacyErrorPaths/TestNexusStartOperation_WithNamespaceAndTaskQueue_SupportsVersioning",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestNexusApiTestSuiteWithTemporalFailures/TestNexusStartOperation_WithNamespaceAndTaskQueue_SupportsVersioning",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuite/Test_BuildIdIndexedOnCompletion_VersionedWorker",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuiteLegacy/Test_BuildIdIndexedOnCompletion_VersionedWorker",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuite/Test_BuildIdIndexedOnReset",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuiteLegacy/Test_BuildIdIndexedOnReset",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuite/Test_BuildIdIndexedOnRetry",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuiteLegacy/Test_BuildIdIndexedOnRetry",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_ByBuildId",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_ByBuildId",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_ByBuildId_NotInNamespace",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_ByBuildId_NotInNamespace",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_ByBuildId_NotInTaskQueue",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_ByBuildId_NotInTaskQueue",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_EmptyBuildIds",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_EmptyBuildIds",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_TooManyBuildIds",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_TooManyBuildIds",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_Unversioned_InNamespace",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_Unversioned_InNamespace",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_Unversioned_InTaskQueue",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_Unversioned_InTaskQueue",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuite/TestBuildIdScavenger_DeletesUnusedBuildId",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestAdvancedVisibilitySuiteLegacy/TestBuildIdScavenger_DeletesUnusedBuildId",
		reason:       workerVersioningEnabledPathReason,
	},
	{
		nameContains: "TestWorkflowUpdateSuite/TestFirstNormalWorkflowTask_UpdateResurrectedAfterRegistryCleared",
		reason: "calls clearUpdateRegistryAndAbortPendingUpdates -> FunctionalTestBase.CloseShard, an " +
			"in-process history-service admin poke that simulates volatile update-registry loss; " +
			"tokeira runs out-of-process with no shard-close surface (the nil in-process host " +
			"SIGSEGVs the harness). CloseShard-class skip.",
	},
	{
		nameContains: "TestWorkflowUpdateSuite/TestCompletedSpeculativeWorkflowTask_DeduplicateID",
		reason: "calls closeShard to evict mutable state between update completions, exercising " +
			"registry-rebuild dedupe; requires in-process CloseShard, impossible against the " +
			"out-of-process engine (CloseShard-class skip).",
	},
	{
		nameContains: "TestWorkflowUpdateSuite/TestStaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartedId",
		reason: "calls clearUpdateRegistryAndAbortPendingUpdates (CloseShard) to force a stale " +
			"speculative WFT with a divergent started id; requires in-process CloseShard, " +
			"impossible against the out-of-process engine (CloseShard-class skip).",
	},
	{
		nameContains: "TestWorkflowUpdateSuite/TestStaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartTime",
		reason: "calls clearUpdateRegistryAndAbortPendingUpdates (CloseShard) to force a stale " +
			"speculative WFT with a divergent start time; requires in-process CloseShard, " +
			"impossible against the out-of-process engine (CloseShard-class skip).",
	},
	{
		nameContains: "TestWorkflowUpdateSuite/TestStaleSpeculativeWorkflowTask_Fail_NewWorkflowTaskWith2Updates",
		reason: "calls clearUpdateRegistryAndAbortPendingUpdates (CloseShard) to strand a stale " +
			"speculative WFT before delivering two fresh updates; requires in-process CloseShard, " +
			"impossible against the out-of-process engine (CloseShard-class skip).",
	},
	{
		nameContains: "TestWorkflowUpdateSuite/TestScheduledSpeculativeWorkflowTask_LostUpdate",
		reason: "calls loseUpdateRegistryAndAbandonPendingUpdates -> CloseShard to drop a scheduled " +
			"speculative WFT's update from the volatile registry; requires in-process CloseShard, " +
			"impossible against the out-of-process engine (CloseShard-class skip).",
	},
	{
		nameContains: "TestWorkflowUpdateSuite/TestStartedSpeculativeWorkflowTask_LostUpdate",
		reason: "calls loseUpdateRegistryAndAbandonPendingUpdates -> CloseShard to drop a started " +
			"speculative WFT's update from the volatile registry; requires in-process CloseShard, " +
			"impossible against the out-of-process engine (CloseShard-class skip).",
	},
	{
		nameContains: "TestWorkflowUpdateSuite/TestStartedSpeculativeWorkflowTask_TerminateWorkflow",
		reason: "ends with env.AdminClient().DescribeMutableState asserting CompletionEventBatchId; " +
			"the conformance shim has no admin client (nil-interface panic aborts the parallel " +
			"suite). The public abort-on-terminate surface stays covered by " +
			"TestUpdateWorkflowSdkSuite/TestTerminateWorkflowAfterUpdateAccepted. " +
			"AdminService/DescribeMutableState-class skip.",
	},
	{
		nameContains: "TestWorkflowUpdateSuite/TestScheduledSpeculativeWorkflowTask_TerminateWorkflow",
		reason: "ends with env.AdminClient().DescribeMutableState asserting CompletionEventBatchId; " +
			"the conformance shim has no admin client (nil-interface panic aborts the parallel " +
			"suite). The public abort-on-terminate surface stays covered by " +
			"TestUpdateWorkflowSdkSuite/TestTerminateWorkflowAfterUpdateAdmitted. " +
			"AdminService/DescribeMutableState-class skip.",
	},
	{
		nameContains: "TestWorkflowUpdateSuite/TestContinueAsNew_Suggestion",
		reason: "requires OverrideDynamicConfig(WorkflowExecutionMaxTotalUpdates=3, " +
			"SuggestContinueAsNewThreshold=0.5) vs v1.31.0 defaults 2000/0.9 — the asserted " +
			"SuggestContinueAsNew flip on the second update is unreachable at defaults (needs " +
			"1800 updates); tokeira does not support dynamic-config injection over the wire " +
			"(established OverrideDynamicConfig-class skip)",
	},
	{
		nameContains: "TestUpdateWithStartSuite/TestUpdateIsAbortedByClosingWorkflow",
		reason: "closing-workflow retry-once + NotFound->Aborted conversion is a deliberate spec " +
			"deferral (api-conformance-multi-operation task 6.1: primary UWS paths land first, " +
			"retry in the wave-8 follow-up task 6.2); the return_retryable_error_after_retry " +
			"sub-case additionally requires the in-process testhook " +
			"UpdateWithStartOnClosingWorkflowRetry to force a second abort, which never fires " +
			"against the out-of-process engine.",
	},
	{
		nameContains: "TestUpdateWithStartSuite/TestReturnUpdateRateLimitError",
		reason: "requires OverrideDynamicConfig(WorkflowExecutionMaxTotalUpdates=1) vs the v1.31.0 " +
			"default 2000 to trip the total-updates FailedPrecondition on the second update; " +
			"unreachable at the default; tokeira does not support dynamic-config injection over " +
			"the wire (established OverrideDynamicConfig-class skip)",
	},
	{
		nameContains: "TestUpdateWithStartSuite/TestReturnUpdateInFlightLimitError",
		reason: "requires OverrideDynamicConfig(WorkflowExecutionMaxInFlightUpdates=1) vs the " +
			"v1.31.0 default 10 to trip the in-flight ResourceExhausted on the second concurrent " +
			"update; unreachable at the default; tokeira does not support dynamic-config " +
			"injection over the wire (established OverrideDynamicConfig-class skip)",
	},
	{
		nameContains: "TestTransientTaskSuite/TestTransientWorkflowTaskHistorySize",
		reason: "requires OverrideDynamicConfig(HistorySizeSuggestContinueAsNew=20KB) to drive " +
			"SuggestContinueAsNew at a test-sized threshold; tokeira does not support " +
			"dynamic-config injection over the wire (established OverrideDynamicConfig-class skip)",
	},
	{
		nameContains: "TestMaxBufferedEventSuite/TestBufferedEventsMutableStateSizeLimit",
		reason: "requires OverrideDynamicConfig(MutableStateSizeLimitError=410KB) so 100KB signals " +
			"exhaust the mutable-state size at a test-sized threshold; unreachable at the v1.31.0 " +
			"default 8MB, and tokeira does not support dynamic-config injection over the wire. The " +
			"sibling TestMaxBufferedEventsLimit (count>100 force-close) passes because it relies on " +
			"the DEFAULT MaximumBufferedEventsBatch=100, not an override (established " +
			"OverrideDynamicConfig-class skip).",
	},
	{
		nameContains: "TestSizeLimitFunctionalSuite/TestTerminateWorkflowCausedByHistoryCountLimit",
		reason: "requires HistoryCountLimitError=20 (and warn=10) vs the v1.31.0 default " +
			"50*1024 events so a short activity/signal history force-terminates; the corpus " +
			"delivers those values only through WithDynamicConfig, which cannot reach an " +
			"out-of-process tokeirad (tests/sizelimit_test.go:43-51 and " +
			"common/dynamicconfig/constants.go:376-384 @ v1.31.0; established " +
			"OverrideDynamicConfig-class skip)",
	},
	{
		nameContains: "TestSizeLimitFunctionalSuite/TestWorkflowFailed_PayloadSizeTooLarge",
		reason: "requires BlobSizeLimitError=1000 vs the v1.31.0 default 2MiB so a 1001-byte " +
			"marker fails its workflow task; the corpus delivers that value only through " +
			"WithDynamicConfig, which cannot reach an out-of-process tokeirad " +
			"(tests/sizelimit_test.go:229-230 and common/dynamicconfig/constants.go:316-324 " +
			"@ v1.31.0; established OverrideDynamicConfig-class skip)",
	},
	{
		nameContains: "TestSizeLimitFunctionalSuite/TestTerminateWorkflowCausedByMsSizeLimit",
		reason: "requires MutableStateSizeLimitError=1100 bytes vs the v1.31.0 default 8MiB " +
			"so four small pending activities force-terminate; the corpus delivers that value " +
			"only through WithDynamicConfig, which cannot reach an out-of-process tokeirad " +
			"(tests/sizelimit_test.go:326-327 and common/dynamicconfig/constants.go:397-405 " +
			"@ v1.31.0; established OverrideDynamicConfig-class skip)",
	},
	{
		nameContains: "TestSizeLimitFunctionalSuite/TestTerminateWorkflowCausedByHistorySizeLimit",
		reason: "requires HistorySizeLimitError=9000 bytes vs the v1.31.0 default 50MiB so " +
			"ten 900-byte signals force-terminate; the corpus delivers that value only through " +
			"WithDynamicConfig, which cannot reach an out-of-process tokeirad " +
			"(tests/sizelimit_test.go:460-461 and common/dynamicconfig/constants.go:360-368 " +
			"@ v1.31.0; established OverrideDynamicConfig-class skip)",
	},
	{
		nameContains: "TestQueryWorkflowSuite/TestQueryWorkflow_NonStickyMultiPageHistory",
		reason: "requires OverrideDynamicConfig(MatchingHistoryMaxPageSize=2) to force a multi-page " +
			"query-task history (the leaf asserts a non-empty NextPageToken, unreachable at any " +
			"realistic default page size); tokeira does not support dynamic-config injection over " +
			"the wire (established OverrideDynamicConfig-class skip)",
	},
	{
		nameContains: "TestEagerWorkflowTestSuite/TestEagerWorkflowStart_TerminateDuplicate",
		reason: "requires OverrideDynamicConfig(WorkflowIdReuseMinimalInterval=0) to allow an " +
			"immediate TerminateIfRunning restart; v1.31.0 migrates that reuse policy to " +
			"TERMINATE_EXISTING and applies the default 1s minimal-reuse gate, while tokeira " +
			"cannot receive dynamic-config injection over the wire (tests/eager_workflow_start_test.go, " +
			"common/dynamicconfig/constants.go, service/history/api/workflow_id_dedup.go @ v1.31.0; " +
			"established OverrideDynamicConfig-class skip)",
	},
	{
		nameContains: "TestRawHistorySuite/TestGetWorkflowExecutionHistory_GetRawHistoryData",
		reason: "requires suite-level dynamic config SendRawWorkflowHistory=true (default false); " +
			"the raw path REPLACES parsed History with RawHistory blobs " +
			"(getworkflowexecutionhistory/api.go:101 @ v1.31.0), so honoring it unconditionally " +
			"would break every parsed-history consumer, and tokeira does not support " +
			"dynamic-config injection over the wire (established OverrideDynamicConfig-class skip)",
	},
	{
		nameContains: "TestDeploymentVersionSuite/TestForceCAN_WithOverrideState",
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

	// --- TestNexusWorkflowTestSuite: Nexus metric leaves.
	//
	// The scrape-backed metrics bridge (tokeira_metrics_bridge.go) now feeds tokeira's
	// genuine out-of-process metric emissions into the corpus CaptureMetricsHandler under
	// Temporal's metric names, so these leaves no longer panic on a nil handler. They remain
	// skipped for reasons the bridge does NOT address:
	//
	//   - OUTBOUND-metric leaves have all landed: SyncNexusFailure, SyncOperationErrorRehydration
	//     (via the kernel Nexus-op invocation-retry state machine, .kiro/specs/nexus-retry-policy),
	//     and AsyncOperationErrorRehydration (via the async error-rehydration + cancel-resolution
	//     decoupling, .kiro/specs/nexus-async-completion). The bridge supplies
	//     nexus_outbound_requests, scoped per namespace so parallel sub-cases don't cross-count.
	//   - COMPLETION-HANDLER metric leaves: a DELIBERATE DEVIATION (Temporal's internal
	//     callback-token wire format + StateMachineRef staleness; see each reason).
	//   - AUTH leaves: need the in-process Host().SetOnAuthorize hook, absent out-of-process.
	{
		nameContains: "TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletion",
		reason: "Asserts Temporal's internal callback-token wire format (CallbackTokenGenerator / " +
			"NexusOperationCompletion proto) and StateMachineRef.MachineInitialVersionedTransition " +
			"staleness — an internal representation tokeira deliberately does not adopt (opaque " +
			"versioned token + op-fencing, nexus.rs:523). The observable contract is covered by " +
			"tokeira-owned behavioural tests.",
	},
	{
		nameContains: "TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionAfterReset",
		reason: "Asserts Temporal's internal callback-token wire format (CallbackTokenGenerator / " +
			"NexusOperationCompletion proto) via sendNexusCompletionRequest — same DELIBERATE " +
			"DEVIATION as TestNexusOperationAsyncCompletion (opaque versioned token + op-fencing, " +
			"nexus.rs:523). The observable contract is covered by tokeira-owned behavioural tests.",
	},
	{
		nameContains: "TestNexusWorkflowTestSuite/TestNexusOperationAsyncFailure",
		reason: "Asserts Temporal's internal callback-token wire format (CallbackTokenGenerator / " +
			"NexusOperationCompletion proto) and StateMachineRef.MachineInitialVersionedTransition " +
			"staleness — an internal representation tokeira deliberately does not adopt (opaque " +
			"versioned token + op-fencing, nexus.rs:523). The observable contract is covered by " +
			"tokeira-owned behavioural tests.",
	},
	{
		nameContains: "TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionErrors",
		reason: "Asserts Temporal's internal callback-token wire format (CallbackTokenGenerator / " +
			"NexusOperationCompletion proto) and StateMachineRef.MachineInitialVersionedTransition " +
			"staleness — an internal representation tokeira deliberately does not adopt (opaque " +
			"versioned token + op-fencing, nexus.rs:523). The observable contract is covered by " +
			"tokeira-owned behavioural tests.",
	},
	{
		nameContains: "TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionAuthErrors",
		reason: "uses the in-process auth hook Host().SetOnAuthorize and the in-process metrics " +
			"CaptureHandler — neither exists against an out-of-process tokeirad",
	},
	{
		nameContains: "TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionAuthErrorsNoIdentifier",
		reason: "uses the in-process auth hook Host().SetOnAuthorize and the in-process metrics " +
			"CaptureHandler — neither exists against an out-of-process tokeirad",
	},
	{
		nameContains: "TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionInternalAuth",
		reason: "requires OverrideDynamicConfig; tokeira does not accept dynamic-config injection " +
			"over the wire (config-as-constant)",
	},
	{
		nameContains: "TestNexusWorkflowTestSuite/TestNexusCallbackAfterCallerComplete",
		reason: "DEFERRED tokeira behaviour gap (NOT a metric test — it makes no nexus_outbound_requests " +
			"assertion; the prior 'metrics CaptureHandler' reason was inaccurate): asserts " +
			"DescribeWorkflowExecution.Callbacks[0].State == CALLBACK_STATE_FAILED and LastAttemptFailure " +
			"(nexus_workflow_test.go:2455-2458), the completion-callback Describe surface tokeira does not " +
			"yet populate (UNSUPPORTED_FIELDS.md: callbacks Empty). Callback-lifecycle Describe work, " +
			"tracked; remove when it lands.",
	},

	// --- TestNexusWorkflowTestSuite: DEFERRED tokeira conformance GAP (not a harness
	// limitation). These exercise the inbound async Nexus completion-callback surface:
	// the server must attach a real callback URL + callback_header + links to the
	// worker-dispatched StartOperation request, and invoke that callback when the
	// handler-side workflow completes to resolve the caller's Nexus operation (with
	// completion links). tokeira ships empty callback/header/links on dispatch
	// (translate/nexus.rs `nexus_task_to_proto_request`) and does not yet invoke
	// completion callbacks on workflow close. Tracked as the C4b "inbound Nexus
	// completion-callback surface" gap; skipped so the panic (nil-map assignment on the
	// absent CallbackHeader) does not abort the whole parallel suite and mask the other
	// leaves' real results. Remove these entries when the surface lands.
	{
		nameContains: "TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionBeforeStart",
		reason: "DEFERRED GAP: requires the inbound async Nexus completion-callback surface " +
			"(callback URL/header/links on the dispatched StartOperation + callback invocation on " +
			"handler-workflow close); tokeira ships empty callback fields and does not invoke " +
			"completion callbacks yet — tracked C4b gap, not a conformance claim",
	},

	// --- TestNexusWorkflowTestSuite: assertions on tokeira-internal / out-of-claim
	// surfaces, not public Nexus behaviour.
	{
		nameContains: "TestNexusWorkflowTestSuite/TestNexusOperationSyncCompletion",
		reason: "the PUBLIC sync-completion behaviour PASSES (NexusOperationCompleted event, the " +
			"handler Nexus-Link carried onto the event with its event_type, and the result " +
			"round-trip to \"result\"); the test THEN white-box-inspects HSM SubStateMachinesByType " +
			"deletion via the internal temporal.server.api.adminservice.v1.AdminService/" +
			"DescribeMutableState (with chasm.WorkflowArchetype). tokeira does not serve that " +
			"internal AdminService (its own coverage classifies AdminService as beyond-claim) and " +
			"has no HSM sub-state-machine model, so the response is absent; s.NoError is non-fatal, " +
			"so the next line nil-derefs and PANICS, aborting the parallel suite. Skipped so the " +
			"rest of the suite runs; the assertion is outside tokeira's public v1.31.0 claim. " +
			"Segment-boundary matching keeps this from skipping TestNexusOperationSyncCompletion_LargePayload.",
	},
	{
		nameContains: "TestNexusWorkflowTestSuite/TestNexusOperationSystemEndpoint",
		reason: "DEFERRED GAP: exercises the __temporal_system internal endpoint, which v1.31.0 " +
			"dispatches in-process via startOnHistoryService (NOT over HTTP). tokeira's outbound " +
			"Nexus client covers External HTTP endpoints only; the internal system-endpoint surface " +
			"is deferred and tracked separately — not a public-HTTP-Nexus conformance claim",
	},
	{
		nameContains: "TestWorkflowTestSuite/TestStartWorkflowExecution_InternalTaskQueue/multiOp",
		reason: "DEFERRED GAP: requires ExecuteMultiOperation (Update-with-Start) to return a typed " +
			"serviceerror.MultiOperationExecution wrapping per-operation validation errors. tokeira " +
			"does not implement the MultiOperation feature yet (ExecuteMultiOperation returns " +
			"Unimplemented); tracked under the api-conformance-multi-operation spec. The sibling " +
			"per-NS-task-queue validation itself PASSES via StartWorkflowExecution/" +
			"SignalWithStartWorkflowExecution in this same test.",
	},
	{
		nameContains: "TestWorkflowTaskTestSuite/TestWorkflowTaskHeartbeatingWithEmptyResult",
		reason: "OUT OF SCOPE (owner decision 2026-07-03): depends on " +
			"OverrideDynamicConfig(WorkflowTaskHeartbeatTimeout=5s) vs the 30m default " +
			"(respondworkflowtaskcompleted/api.go:298, constants.go:2427). Per the conformance " +
			"config-as-constant convention the 30m default is not operationally wrong for tokeira, so the " +
			"heartbeat timeout does not earn a deployment knob merely to pass a test — same class as the " +
			"MaxCallbacksPerWorkflow OverrideDynamicConfig skip. Permanent skip, not implemented (no " +
			"config knob, no PendingWorkflowTask.original_scheduled_at). Spec .kiro/specs/transient-wft/ " +
			"Item C; raised in docs/HANDOVER-transient-wft.md (C).",
	},
	{
		nameContains: "TestWorkflowTestSuite/TestStartWorkflowExecution_UseExisting_OnConflictOptions/" +
			"OnConflictOptions_failed_max_callbacks_per_workflow",
		reason: "requires OverrideDynamicConfig(MaxCallbacksPerWorkflow=1); tokeira does not support " +
			"dynamic-config injection over the wire (MaxCallbacksPerWorkflow is a pinned constant), " +
			"so the leaf's premise — lowering the limit to 1 mid-test — cannot reach an " +
			"out-of-process tokeirad. Same class as the other OverrideDynamicConfig skips in this " +
			"registry.",
	},
	{
		nameContains: "TestWorkflowResetTestSuite/TestResetWorkflowWithOptionsUpdate",
		reason: "reset-with-worker-deployment-versioning: startVersionedPollerAndValidate drives a " +
			"VERSIONED poller and asserts task-queue version membership via the matching-service RPC " +
			"CheckTaskQueueVersionMembership. tokeira's conformance cluster exposes no standalone " +
			"MatchingClient (the engine is a single edge process), so GetTestCluster().MatchingClient() " +
			"is nil and the versioned-poller goroutine SIGSEGVs, aborting the whole binary. Worker " +
			"deployment versioning is Tier-4+ scope; the reset behaviour itself is covered by the " +
			"non-versioned reset leaves. Matching-service/versioning-class skip.",
	},
	{
		nameContains: "TestWorkflowResetTestSuite/TestBatchResetWithOptionsUpdate",
		reason: "reset-with-worker-deployment-versioning: same startVersionedPollerAndValidate + " +
			"CheckTaskQueueVersionMembership matching-RPC dependency as TestResetWorkflowWithOptionsUpdate " +
			"(nil MatchingClient SIGSEGVs the versioned-poller goroutine). Worker deployment versioning " +
			"is Tier-4+ scope. Matching-service/versioning-class skip.",
	},
	{
		nameContains: "TestNamespaceSuite/Test_NamespaceDelete_WithMissingWorkflows",
		reason: "directly calls GetTestCluster().ExecutionManager().DeleteWorkflowExecution to " +
			"remove mutable state while deliberately leaving visibility rows behind " +
			"(tests/namespace_delete_test.go @ v1.31.0). Shape-2 has no in-process Temporal " +
			"ExecutionManager; namespace deletion over the public OperatorService is covered by " +
			"the sibling namespace-delete leaves. ExecutionManager-class skip.",
	},
	{
		nameContains: "TestNamespaceSuite/Test_NamespaceDelete_Protected",
		reason: "requires a per-test OverrideDynamicConfig(worker.protectedNamespaces, []string{random " +
			"namespace}) and asserts that policy's FailedPrecondition response " +
			"(tests/namespace_delete_test.go and common/dynamicconfig/constants.go @ v1.31.0). " +
			"The conformance override protocol has no string-list value kind and tokeira does not " +
			"implement this deployment-policy key; the public deletion path remains covered by the " +
			"sibling leaves. OverrideDynamicConfig-class skip.",
	},
	{
		nameContains: "TestTaskQueueSuite/TestTaskDispatchLatencyMetric_WorkflowAndActivity",
		reason: "requires the in-process MatchingClient.DescribeTaskQueuePartition RPC, " +
			"CaptureMetricsHandler, MatchingForwardTaskDelay test hook, and constrained " +
			"WithDynamicConfig values (tests/task_queue_test.go @ v1.31.0); Shape-2 fronts " +
			"the public WorkflowService only, so these matching-internal metric assertions " +
			"cannot execute against an out-of-process tokeirad.",
	},
	{
		nameContains: "TestTaskQueueSuite/TestTaskDispatchLatencyMetric_Query",
		reason: "requires the in-process MatchingClient.DescribeTaskQueuePartition RPC, " +
			"CaptureMetricsHandler, MatchingForwardTaskDelay test hook, and constrained " +
			"WithDynamicConfig values (tests/task_queue_test.go @ v1.31.0); Shape-2 fronts " +
			"the public WorkflowService only, so these matching-internal metric assertions " +
			"cannot execute against an out-of-process tokeirad.",
	},
	{
		nameContains: "TestTaskQueueSuite/TestTaskDispatchLatencyMetric_Nexus",
		reason: "requires the in-process MatchingClient DescribeTaskQueuePartition and " +
			"DispatchNexusTask RPCs, CaptureMetricsHandler, MatchingForwardTaskDelay test " +
			"hook, and constrained WithDynamicConfig values (tests/task_queue_test.go @ " +
			"v1.31.0); Shape-2 fronts the public WorkflowService only, so the leaf otherwise " +
			"nil-dereferences the unavailable matching client and aborts its process.",
	},
	{
		nameContains: "TestTaskQueueSuite/TestTaskQueueRateLimit",
		reason: "tests Temporal matching implementation modes and partition forwarding by " +
			"mutating MatchingUseNewMatcher, read/write partition counts, forwarder rate, and " +
			"AdminMatchingNamespaceTaskqueueToPartitionDispatchRate, then comparing four " +
			"mode/partition drain budgets (tests/task_queue_test.go:57-160 @ v1.31.0). Tokeira " +
			"has one matching partition and no old/new matcher mode, so the test premise is " +
			"outside the public Shape-2 contract; public queue stats and API/worker rate limits " +
			"remain covered by active sibling leaves.",
	},
	{
		nameContains: "TestScheduleV1/TestRefresh",
		reason: "directly reads and signals Temporal's internal scheduler workflow " +
			"(temporal-sys-scheduler:<schedule-id>) to validate its refresh implementation " +
			"(tests/schedule_test.go @ v1.31.0). tokeira implements schedules as a native " +
			"runtime store and engine, so no internal scheduler workflow exists; public " +
			"Describe/Update/List behavior remains covered by active sibling leaves.",
	},
	{
		nameContains: "TestScheduleV1/TestNextTimeCache",
		reason: "white-box inspects Temporal's internal scheduler workflow history, SideEffect " +
			"marker count, and serialized NextTimeCache (tests/schedule_test.go @ v1.31.0). " +
			"Those are implementation details of Temporal's workflow-backed scheduler; " +
			"tokeira's native schedule engine has no equivalent internal history.",
	},
	{
		nameContains: "TestScheduleV1/TestCreatesCHASMSentinel",
		reason: "directly calls the in-process SchedulerClient to inspect CHASM sentinel " +
			"reservation for a V1 scheduler workflow (tests/schedule_test.go @ v1.31.0). " +
			"Shape-2 exposes the public WorkflowService only; SchedulerClient is unavailable " +
			"and otherwise nil-dereferences, while CHASM migration sentinels are outside the " +
			"v1.31.0 public compatibility claim.",
	},
	{
		nameContains: "TestScheduleV1/TestSkipsCHASMSentinelWhenDisabled",
		reason: "directly calls the in-process SchedulerClient and toggles " +
			"EnableCHASMSchedulerSentinels to inspect absence of an internal migration sentinel " +
			"(tests/schedule_test.go @ v1.31.0). Shape-2 has no SchedulerClient and tokeira's " +
			"native V1 schedule engine does not model CHASM migration sentinels.",
	},
}

// conformanceSkipReason returns the skip reason for a test name when one is
// registered, and whether a match was found. A registered name matches the test
// itself (exact) or any of its sub-tests (the registered name followed by "/...").
// Matching is on path-segment boundaries, NOT raw substring, so a registered leaf
// never accidentally skips a sibling whose name merely shares a textual prefix
// (e.g. registering `…AsyncCompletion` must not skip `…AsyncCompletionBeforeStart`).
func conformanceSkipReason(testName string) (string, bool) {
	for _, skip := range conformanceSkips {
		if testName == skip.nameContains || strings.HasPrefix(testName, skip.nameContains+"/") {
			return skip.reason, true
		}
	}
	return "", false
}

// maybeSkipForConformance skips the current test when conformance mode is active
// and the test is in the skip registry. A no-op outside conformance mode.
func (s *FunctionalTestBase) maybeSkipForConformance() {
	maybeSkipTestForConformance(s.T())
}

// maybeSkipTestForConformance is the `testing.TB`-based skip used by entry points
// that are not `FunctionalTestBase` methods — notably [NewEnv], which every
// `testEnv`/`parallelsuite` test calls first. That path matters because
// `parallelsuite.Run` invokes test methods directly and never calls `SetupTest`,
// so the method hook above never fires for those suites; gating in `NewEnv`
// ensures a registered skip takes effect on a plain `go test` invocation (not only
// under the run-all runner's `-skip`). A no-op outside conformance mode.
func maybeSkipTestForConformance(t testing.TB) {
	if conformanceFrontendAddr() == "" {
		return
	}
	if reason, ok := conformanceSkipReason(t.Name()); ok {
		t.Skipf("tokeira conformance: skipping %s — %s", t.Name(), reason)
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
