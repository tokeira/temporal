package testcore

import (
	"regexp"
	"strings"
	"testing"
)

// Conformance skip registry.
//
// The Tier-2 functional conformance harness replays Temporal's Go corpus, with
// the sole HTTP synchronization disclosed in README.md, against an external
// `tokeirad` over the real gRPC wire. A small number
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

var conformanceSkips = []conformanceSkip{
	{nameContains: "TestActivityApiPause_AttributesToActivityInContextMetadata", reason: "Enables the unwired startup-only frontend.contextMetadataSetTrailer flag, whose stock default is false; the frontend captures it when constructing the context-metadata interceptor. (tests/activity_api_pause_test.go:990; service/frontend/fx.go:525; common/dynamicconfig/constants.go:869 @ v1.32.0)"},
	{nameContains: "TestTaskQueueStats_Pri_Suite/TestAddMultipleTasks_ValidateStats_Cached", reason: "Sets matching.TaskQueueInfoByBuildIdTTL to one hour and requires stale cached backlog statistics after draining tasks; this tests Temporal's internal statistics-cache lifetime. (tests/task_queue_stats_test.go:137-183 @ v1.32.0)"},
	{nameContains: "TestTaskQueueStats_Pri_Suite/TestVersioningSuite/NoTaskForwardNoPollForwardForceAsyncSuite", reason: "Forces asynchronous matching with MatchingDisableSyncMatch; the nested statistics cases depend on an in-process matching hook. (tests/task_queue_stats_test.go:189-197; tests/testcore/matching_behavior.go:58-73 @ v1.32.0)"},
	{nameContains: "TestTaskQueueStats_Pri_Suite/TestVersioningSuite/ForceTaskForwardNoPollForwardAllowSyncSuite", reason: "Forces writes to matching partition 11 with MatchingLBForceWritePartition in a 13-partition topology; the nested statistics cases depend on internal routing hooks. (tests/task_queue_stats_test.go:189-197; tests/testcore/matching_behavior.go:39-73 @ v1.32.0)"},
	{nameContains: "TestTaskQueueStats_Pri_Suite/TestVersioningSuite/ForceTaskForwardNoPollForwardForceAsyncSuite", reason: "Forces writes to matching partition 11 and disables synchronous matching with in-process hooks in a 13-partition topology. (tests/task_queue_stats_test.go:189-197; tests/testcore/matching_behavior.go:39-73 @ v1.32.0)"},
	{nameContains: "TestTaskQueueStats_Pri_Suite/TestVersioningSuite/NoTaskForwardForcePollForwardAllowSyncSuite", reason: "Forces polls to matching partition 5 with MatchingLBForceReadPartition in a 13-partition topology; the nested statistics cases depend on internal routing hooks. (tests/task_queue_stats_test.go:189-197; tests/testcore/matching_behavior.go:39-73 @ v1.32.0)"},
	{nameContains: "TestTaskQueueStats_Pri_Suite/TestVersioningSuite/NoTaskForwardForcePollForwardForceAsyncSuite", reason: "Forces polls to matching partition 5 and disables synchronous matching with in-process hooks in a 13-partition topology. (tests/task_queue_stats_test.go:189-197; tests/testcore/matching_behavior.go:39-73 @ v1.32.0)"},
	{nameContains: "TestTaskQueueStats_Pri_Suite/TestVersioningSuite/ForceTaskForwardForcePollForwardAllowSyncSuite", reason: "Forces writes to matching partition 11 and polls to partition 5 with in-process routing hooks in a 13-partition topology. (tests/task_queue_stats_test.go:189-197; tests/testcore/matching_behavior.go:39-73 @ v1.32.0)"},
	{nameContains: "TestTaskQueueStats_Pri_Suite/TestVersioningSuite/ForceTaskForwardForcePollForwardForceAsyncSuite", reason: "Forces writes to matching partition 11, polls to partition 5, and asynchronous matching with in-process hooks in a 13-partition topology. (tests/task_queue_stats_test.go:189-197; tests/testcore/matching_behavior.go:39-73 @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestUnpinnedTask_OldDeployment", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromWft_Sticky", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromWft_NoSticky", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromWft_Sticky_ToUnversioned", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromWft_NoSticky_ToUnversioned", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestDoubleTransition", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestDoubleTransition_WithSignal", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestDoubleTransitionFromUnversioned", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestDoubleTransitionFromUnversioned_WithSignal", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestEagerActivity", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromActivity_Sticky", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestTransitionFromActivity_NoSticky", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestIndependentVersionedActivity_Pinned", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestIndependentVersionedActivity_Unpinned", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestIndependentUnversionedActivity_Pinned", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestIndependentUnversionedActivity_Unpinned", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestChildWorkflowInheritance_PinnedParent", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestChildWorkflowInheritance_ParentPinnedByOverride", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestChildWorkflowInheritance_CrossTQ_Inherit", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestDescribeTaskQueueVersioningInfo", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestSyncDeploymentUserDataWithRoutingConfig_Update", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestAutoUpgradeWorkflows_NoBouncingBetweenVersions", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestWorkflowTQLags_DependentActivityStartsTransition", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestActivityTQLags_DependentActivityCompletesOnTheNewVersion", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestChildStartsWithParentRevision_SameTQ_TQLags", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestContinueAsNewOfAutoUpgradeWorkflow_RevisionNumberMechanics", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestWorkflowRetry_AutoUpgrade_NoBounceBack", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestWorkflowRetry_AutoUpgrade_AfterCAN_NoBounceBack", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestWorkflowRetry_AutoUpgrade_ChildNoBounceBack", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestNexusTask_StaysOnCurrentDeployment", reason: "Dispatches through internal MatchingService.DispatchNexusTask and installs routing state through SyncDeploymentUserData. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestCheckTaskQueueVersionMembership", reason: "Directly tests the internal MatchingService.CheckTaskQueueVersionMembership contract, outside public WorkflowService behavior. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestMaxVersionsInTaskQueue", reason: "Installs or rolls back internal task-queue routing revisions through SyncDeploymentUserData/GetTaskQueueUserData, including helpers in tests/versioning_test_env.go; these topology mutations have no public API equivalent. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestVersioning3FunctionalSuite/TestVersionedQueueUnload", reason: "Asserts internal matching partition unload/reload via GetTaskQueueUserData; Tokeira has no Temporal partition-cache lifecycle. (tests/versioning_3_test.go @ v1.32.0)"},
	{nameContains: "TestNexusApiTestSuiteWithLegacyErrorPaths/TestNexusStartOperation_WithNamespaceAndTaskQueue_SupportsVersioning", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/nexus_api_test.go @ v1.32.0)"},
	{nameContains: "TestNexusApiTestSuiteWithTemporalFailures/TestNexusStartOperation_WithNamespaceAndTaskQueue_SupportsVersioning", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/nexus_api_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuite/Test_BuildIdIndexedOnCompletion_VersionedWorker", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuiteLegacy/Test_BuildIdIndexedOnCompletion_VersionedWorker", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuite/Test_BuildIdIndexedOnReset", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuiteLegacy/Test_BuildIdIndexedOnReset", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuite/Test_BuildIdIndexedOnRetry", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuiteLegacy/Test_BuildIdIndexedOnRetry", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_ByBuildId", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_ByBuildId", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_ByBuildId_NotInNamespace", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_ByBuildId_NotInNamespace", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_ByBuildId_NotInTaskQueue", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_ByBuildId_NotInTaskQueue", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_EmptyBuildIds", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_EmptyBuildIds", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_TooManyBuildIds", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_TooManyBuildIds", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_Unversioned_InNamespace", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_Unversioned_InNamespace", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuite/TestWorkerTaskReachability_Unversioned_InTaskQueue", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuiteLegacy/TestWorkerTaskReachability_Unversioned_InTaskQueue", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuite/TestBuildIdScavenger_DeletesUnusedBuildId", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestAdvancedVisibilitySuiteLegacy/TestBuildIdScavenger_DeletesUnusedBuildId", reason: "Requires deprecated worker-versioning V1/V2 enabled behavior. frontend.workerVersioningDataAPIs and frontend.workerVersioningRuleAPIs remain false by default in common/dynamicconfig/constants.go @ v1.32.0; the compatibility scope retains stock rejection. (tests/advanced_visibility_test.go @ v1.32.0)"},
	{nameContains: "TestWorkflowUpdateSuite/TestFirstNormalWorkflowTask_UpdateResurrectedAfterRegistryCleared", reason: "Uses CloseShard through clearUpdateRegistryAndAbortPendingUpdates, loseUpdateRegistryAndAbandonPendingUpdates, or closeShard to evict Temporal mutable state. The external engine has no equivalent shard-cache mutation. (tests/update_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestWorkflowUpdateSuite/TestCompletedSpeculativeWorkflowTask_DeduplicateID", reason: "Uses CloseShard through clearUpdateRegistryAndAbortPendingUpdates, loseUpdateRegistryAndAbandonPendingUpdates, or closeShard to evict Temporal mutable state. The external engine has no equivalent shard-cache mutation. (tests/update_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestWorkflowUpdateSuite/TestStaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartedId", reason: "Uses CloseShard through clearUpdateRegistryAndAbortPendingUpdates, loseUpdateRegistryAndAbandonPendingUpdates, or closeShard to evict Temporal mutable state. The external engine has no equivalent shard-cache mutation. (tests/update_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestWorkflowUpdateSuite/TestStaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartTime", reason: "Uses CloseShard through clearUpdateRegistryAndAbortPendingUpdates, loseUpdateRegistryAndAbandonPendingUpdates, or closeShard to evict Temporal mutable state. The external engine has no equivalent shard-cache mutation. (tests/update_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestWorkflowUpdateSuite/TestStaleSpeculativeWorkflowTask_Fail_NewWorkflowTaskWith2Updates", reason: "Uses CloseShard through clearUpdateRegistryAndAbortPendingUpdates, loseUpdateRegistryAndAbandonPendingUpdates, or closeShard to evict Temporal mutable state. The external engine has no equivalent shard-cache mutation. (tests/update_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestWorkflowUpdateSuite/TestScheduledSpeculativeWorkflowTask_LostUpdate", reason: "Uses CloseShard through clearUpdateRegistryAndAbortPendingUpdates, loseUpdateRegistryAndAbandonPendingUpdates, or closeShard to evict Temporal mutable state. The external engine has no equivalent shard-cache mutation. (tests/update_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestWorkflowUpdateSuite/TestStartedSpeculativeWorkflowTask_LostUpdate", reason: "Uses CloseShard through clearUpdateRegistryAndAbortPendingUpdates, loseUpdateRegistryAndAbandonPendingUpdates, or closeShard to evict Temporal mutable state. The external engine has no equivalent shard-cache mutation. (tests/update_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestWorkflowUpdateSuite/TestStartedSpeculativeWorkflowTask_TerminateWorkflow", reason: "Asserts internal DescribeMutableState.ExecutionInfo.CompletionEventBatchId. The engine admin projection does not expose Temporal event-batch persistence metadata. (tests/update_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestWorkflowUpdateSuite/TestScheduledSpeculativeWorkflowTask_TerminateWorkflow", reason: "Asserts internal DescribeMutableState.ExecutionInfo.CompletionEventBatchId. The engine admin projection does not expose Temporal event-batch persistence metadata. (tests/update_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestUpdateWithStartSuite/TestUpdateIsAbortedByClosingWorkflow/return_retryable_error_after_retry", reason: "This subcase requires the in-process UpdateWithStartOnClosingWorkflowRetry hook to start another workflow during the server retry. The public retry-once sibling now runs. (tests/update_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestUpdateWithStartSuite/TestReturnUpdateInFlightLimitError", reason: "Requires non-default history.maxInFlightUpdates; the unchanged engine conformance KEY_CLASSIFICATION marks it unwired. The override bridge cannot honor this setting. (tests/update_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestMaxBufferedEventSuite/TestBufferedEventsMutableStateSizeLimit", reason: "Requires non-default limit.mutableStateSize.error; the unchanged engine conformance KEY_CLASSIFICATION marks it NotEnforced. The override bridge cannot honor this setting. (tests/max_buffered_event_test.go @ v1.32.0)"},
	{nameContains: "TestSizeLimitFunctionalSuite/TestTerminateWorkflowCausedByHistoryCountLimit", reason: "Requires non-default limit.historyCount.error; the unchanged engine conformance KEY_CLASSIFICATION marks it NotEnforced. The override bridge cannot honor this setting. (tests/sizelimit_test.go @ v1.32.0)"},
	{nameContains: "TestSizeLimitFunctionalSuite/TestWorkflowFailed_PayloadSizeTooLarge", reason: "Requires non-default limit.blobSize.error; the unchanged engine conformance KEY_CLASSIFICATION marks it NotEnforced. The override bridge cannot honor this setting. (tests/sizelimit_test.go @ v1.32.0)"},
	{nameContains: "TestSizeLimitFunctionalSuite/TestTerminateWorkflowCausedByMsSizeLimit", reason: "Requires non-default limit.mutableStateSize.error; the unchanged engine conformance KEY_CLASSIFICATION marks it NotEnforced. The override bridge cannot honor this setting. (tests/sizelimit_test.go @ v1.32.0)"},
	{nameContains: "TestSizeLimitFunctionalSuite/TestTerminateWorkflowCausedByHistorySizeLimit", reason: "Requires non-default limit.historySize.error; the unchanged engine conformance KEY_CLASSIFICATION marks it NotEnforced. The override bridge cannot honor this setting. (tests/sizelimit_test.go @ v1.32.0)"},
	{nameContains: "TestQueryWorkflowSuite/TestQueryWorkflow_NonStickyMultiPageHistory", reason: "Requires non-default matching.historyMaxPageSize; the unchanged engine conformance KEY_CLASSIFICATION marks it unwired. The override bridge cannot honor this setting. (tests/query_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestEagerWorkflowTestSuite/TestEagerWorkflowStart_TerminateDuplicate", reason: "Requires non-default history.workflowIdReuseMinimalInterval; the unchanged engine conformance KEY_CLASSIFICATION marks it KernelExcluded. The override bridge cannot honor this setting. (tests/eager_workflow_start_test.go @ v1.32.0)"},
	{nameContains: "TestRawHistorySuite/TestGetWorkflowExecutionHistory_GetRawHistoryData", reason: "Requires non-default frontend.sendRawWorkflowHistory; the unchanged engine conformance KEY_CLASSIFICATION marks it unwired. The override bridge cannot honor this setting. (tests/gethistory_test.go @ v1.32.0)"},
	{nameContains: "TestDeploymentVersionSuite/TestForceCAN_WithOverrideState", reason: "Injects serialized internal worker-deployment entity-workflow state through ForceCANDeploymentSignalArgs.OverrideState; Tokeira owns a native deployment registry. (tests/worker_deployment_version_test.go @ v1.32.0)"},
	{nameContains: "TestWorkerDeploymentSuite/TestForceCAN_WithOverrideState", reason: "Injects serialized internal worker-deployment entity-workflow state through ForceCANDeploymentSignalArgs.OverrideState; Tokeira owns a native deployment registry. (tests/worker_deployment_test.go @ v1.32.0)"},
	{nameContains: "TestWorkerDeploymentSuite/TestSetManagerIdentity_WithDeleteVersion", reason: "Assumes two manager-identity operations consume the 500ms PollerHistoryTTL before deletion. The unchanged corpus has no wait for that TTL; fast native-registry operations can correctly reject deletion while the poller remains active. (tests/worker_deployment_test.go @ v1.32.0)"},
	{nameContains: "TestStandaloneActivityTestSuite/TestStart/RequestValidations/InputTooLarge", reason: "Requires limit.blobSize.error=1000; the unchanged engine conformance KEY_CLASSIFICATION marks this setting NotEnforced. (tests/activity_standalone_test.go @ v1.32.0)"},
	{nameContains: "TestStandaloneActivityTestSuite/TestRequestCancel/RequestValidations/ReasonTooLong", reason: "Requires limit.blobSize.error=1000; the unchanged engine conformance KEY_CLASSIFICATION marks this setting NotEnforced. (tests/activity_standalone_test.go @ v1.32.0)"},
	{nameContains: "TestStandaloneActivityTestSuite/TestTerminate/RequestValidations/ReasonTooLong", reason: "Requires limit.blobSize.error=1000; the unchanged engine conformance KEY_CLASSIFICATION marks this setting NotEnforced. (tests/activity_standalone_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteHSM/TestNexusOperationAsyncCompletion", reason: "Decodes or manufactures Temporal CallbackTokenGenerator/NexusOperationCompletion tokens (directly or via generateValidCallbackToken), including internal HSM/CHASM references; the engine uses opaque operation-fenced tokens. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteCHASM/TestNexusOperationAsyncCompletion", reason: "Decodes or manufactures Temporal CallbackTokenGenerator/NexusOperationCompletion tokens (directly or via generateValidCallbackToken), including internal HSM/CHASM references; the engine uses opaque operation-fenced tokens. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteHSM/TestNexusOperationAsyncCompletionErrors", reason: "Decodes or manufactures Temporal CallbackTokenGenerator/NexusOperationCompletion tokens (directly or via generateValidCallbackToken), including internal HSM/CHASM references; the engine uses opaque operation-fenced tokens. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteCHASM/TestNexusOperationAsyncCompletionErrors", reason: "Decodes or manufactures Temporal CallbackTokenGenerator/NexusOperationCompletion tokens (directly or via generateValidCallbackToken), including internal HSM/CHASM references; the engine uses opaque operation-fenced tokens. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteHSM/TestNexusOperationAsyncCompletionAuthErrors", reason: "Decodes or manufactures Temporal CallbackTokenGenerator/NexusOperationCompletion tokens (directly or via generateValidCallbackToken), including internal HSM/CHASM references; the engine uses opaque operation-fenced tokens. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteCHASM/TestNexusOperationAsyncCompletionAuthErrors", reason: "Decodes or manufactures Temporal CallbackTokenGenerator/NexusOperationCompletion tokens (directly or via generateValidCallbackToken), including internal HSM/CHASM references; the engine uses opaque operation-fenced tokens. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteHSM/TestNexusOperationAsyncCompletionAuthErrorsNoIdentifier", reason: "Decodes or manufactures Temporal CallbackTokenGenerator/NexusOperationCompletion tokens (directly or via generateValidCallbackToken), including internal HSM/CHASM references; the engine uses opaque operation-fenced tokens. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteCHASM/TestNexusOperationAsyncCompletionAuthErrorsNoIdentifier", reason: "Decodes or manufactures Temporal CallbackTokenGenerator/NexusOperationCompletion tokens (directly or via generateValidCallbackToken), including internal HSM/CHASM references; the engine uses opaque operation-fenced tokens. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteHSM/TestNexusOperationAsyncCompletionInternalAuth", reason: "Requires the unwired component.nexusoperations.callbackURLTemplate override to install an internal callback host; the engine cannot deliver that topology override. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteCHASM/TestNexusOperationAsyncCompletionInternalAuth", reason: "Requires the unwired component.nexusoperations.callbackURLTemplate override to install an internal callback host; the engine cannot deliver that topology override. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteHSM/TestNexusOperationSyncCompletion", reason: "Inspects internal DescribeMutableState.SubStateMachinesByType to verify HSM deletion; Tokeira has no Temporal HSM persistence representation. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestNexusWorkflowTestSuiteCHASM/TestNexusOperationSyncCompletion", reason: "Inspects internal DescribeMutableState.SubStateMachinesByType to verify HSM deletion; Tokeira has no Temporal HSM persistence representation. (tests/nexus_workflow_test.go @ v1.32.0)"},
	{nameContains: "TestWorkflowTaskTestSuite/TestWorkflowTaskHeartbeatingWithEmptyResult", reason: "Requires non-default history.workflowTaskHeartbeatTimeout; the unchanged engine conformance KEY_CLASSIFICATION marks it unwired. The override bridge cannot honor this setting. (tests/workflow_task_test.go @ v1.32.0)"},
	{nameContains: "TestNamespaceSuite/Test_NamespaceDelete_WithMissingWorkflows", reason: "Calls ExecutionManager.DeleteWorkflowExecution to remove mutable state while retaining stale visibility rows; this persistence-corruption setup is unavailable through the public API. (tests/namespace_test.go @ v1.32.0)"},
	{nameContains: "TestNamespaceSuite/Test_NamespaceDelete_Protected", reason: "Requires non-default worker.protectedNamespaces; the unchanged engine conformance KEY_CLASSIFICATION marks it unwired. The override bridge cannot honor this setting. (tests/namespace_test.go @ v1.32.0)"},
	{nameContains: "TestTaskQueueSuite/TestTaskDispatchLatencyMetric_WorkflowAndActivity", reason: "Requires internal matching partition/forwarding state and hooks (DescribeTaskQueuePartition, MatchingForwardTaskDelay, or per-partition dispatch-rate overrides); these Temporal topology controls are not public API behavior. (tests/task_queue_test.go @ v1.32.0)"},
	{nameContains: "TestTaskQueueSuite/TestTaskDispatchLatencyMetric_Query", reason: "Requires internal matching partition/forwarding state and hooks (DescribeTaskQueuePartition, MatchingForwardTaskDelay, or per-partition dispatch-rate overrides); these Temporal topology controls are not public API behavior. (tests/task_queue_test.go @ v1.32.0)"},
	{nameContains: "TestTaskQueueSuite/TestTaskDispatchLatencyMetric_Nexus", reason: "Requires internal matching partition/forwarding state and hooks (DescribeTaskQueuePartition, MatchingForwardTaskDelay, or per-partition dispatch-rate overrides); these Temporal topology controls are not public API behavior. (tests/task_queue_test.go @ v1.32.0)"},
	{nameContains: "TestTaskQueueSuite/TestTaskQueueRateLimit", reason: "Requires internal matching partition/forwarding state and hooks (DescribeTaskQueuePartition, MatchingForwardTaskDelay, or per-partition dispatch-rate overrides); these Temporal topology controls are not public API behavior. (tests/task_queue_test.go @ v1.32.0)"},
	{nameContains: "TestFairnessSuite/TestMigration_FromClassic", reason: "Asserts draining/active physical-queue status during Temporal classic/new/fair matcher persistence migration. Tokeira delivery modes do not implement that internal migration topology. (tests/priority_fairness_test.go @ v1.32.0)"},
	{nameContains: "TestFairnessSuite/TestMigration_FromPri", reason: "Asserts draining/active physical-queue status during Temporal classic/new/fair matcher persistence migration. Tokeira delivery modes do not implement that internal migration topology. (tests/priority_fairness_test.go @ v1.32.0)"},
	{nameContains: "TestFairnessSuite/TestMigration_FromFair", reason: "Asserts draining/active physical-queue status during Temporal classic/new/fair matcher persistence migration. Tokeira delivery modes do not implement that internal migration topology. (tests/priority_fairness_test.go @ v1.32.0)"},
	{nameContains: "TestFairnessAutoEnableSuite/TestMigration_FromClassic", reason: "Asserts draining/active physical-queue status during Temporal classic/new/fair matcher persistence migration. Tokeira delivery modes do not implement that internal migration topology. (tests/priority_fairness_test.go @ v1.32.0)"},
	{nameContains: "TestFairnessAutoEnableSuite/TestMigration_FromPri", reason: "Asserts draining/active physical-queue status during Temporal classic/new/fair matcher persistence migration. Tokeira delivery modes do not implement that internal migration topology. (tests/priority_fairness_test.go @ v1.32.0)"},
	{nameContains: "TestFairnessAutoEnableSuite/TestMigration_FromFair", reason: "Asserts draining/active physical-queue status during Temporal classic/new/fair matcher persistence migration. Tokeira delivery modes do not implement that internal migration topology. (tests/priority_fairness_test.go @ v1.32.0)"},
	{nameContains: "TestFairnessSuite/TestUpdateWorkflowExecutionOptions_InvalidatesPendingTask", reason: "Asserts internal matching-client request/failure metrics and a Go concrete error type; an external engine cannot emit Temporal in-process client calls. (tests/priority_fairness_test.go @ v1.32.0)"},
	{nameContains: "TestScheduleV1/TestRefresh", reason: "Reads or signals the internal temporal-sys-scheduler workflow, including history markers/NextTimeCache; Tokeira owns a native schedule store. (tests/schedule_test.go @ v1.32.0)"},
	{nameContains: "TestScheduleV1/TestNextTimeCache", reason: "Reads or signals the internal temporal-sys-scheduler workflow, including history markers/NextTimeCache; Tokeira owns a native schedule store. (tests/schedule_test.go @ v1.32.0)"},
	{nameContains: "TestScheduleV1/TestCreatesCHASMSentinel", reason: "Uses the internal SchedulerClient to inspect CHASM migration sentinels; Tokeira schedules have no Temporal entity-workflow/sentinel representation. (tests/schedule_test.go @ v1.32.0)"},
	{nameContains: "TestScheduleV1/TestSkipsCHASMSentinelWhenDisabled", reason: "Uses the internal SchedulerClient to inspect CHASM migration sentinels; Tokeira schedules have no Temporal entity-workflow/sentinel representation. (tests/schedule_test.go @ v1.32.0)"},
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
// Go splits -skip at path separators. Mixing method and nested-leaf entries
// creates unwanted cross-products. Only the deepest group goes into -skip;
// shallower method exclusions are enforced by NewEnv/SetupTest. Every deeper
// entry in this registry has matching depth within its entrypoint.
func ConformanceSkipRegexp(entrypoint string) string {
	depth := 0
	for _, skip := range conformanceSkips {
		if strings.Split(skip.nameContains, "/")[0] == entrypoint {
			depth = max(depth, len(strings.Split(skip.nameContains, "/")))
		}
	}
	var positions [][]string
	seen := []map[string]bool{}
	for _, skip := range conformanceSkips {
		parts := strings.Split(skip.nameContains, "/")
		if len(parts) != depth || parts[0] != entrypoint {
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

// ConformanceExclusions returns exact registry identities and reasons. Go's
// -skip emits no test2json event, so the runner records these separately as
// explicitly sourced registry exclusions rather than losing them from coverage.
func ConformanceExclusions(entrypoint string) map[string]string {
	out := make(map[string]string)
	for _, skip := range conformanceSkips {
		if strings.Split(skip.nameContains, "/")[0] == entrypoint {
			out[skip.nameContains] = skip.reason
		}
	}
	return out
}
