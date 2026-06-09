package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestTokeiraConformance_BasicWorkflowLifecycle is the Tier-2 (Shape-2) first
// milestone: it proves the onebox seam's wire path end-to-end by driving one
// basic WorkflowService lifecycle against a real, externally-launched `tokeirad`
// frontend BEFORE the full Temporal corpus or large files like
// versioning_3_test.go are attempted (see temporal-functional-conformance design,
// "Initial bring-up runs a small, high-signal subset").
//
// Deliberately standalone — NOT a FunctionalTestBase suite. FunctionalTestBase's
// setupCluster registers its namespace by writing directly to
// testCluster.testBase.MetadataManager.CreateNamespace
// (functional_test_base.go RegisterNamespace), i.e. Temporal's own persistence
// layer. Under Shape-2 `tokeirad` is the backend over the wire and the onebox
// boots no Temporal persistence, so that direct write is invisible to `tokeirad`.
// This test therefore registers its namespace through `tokeirad`'s frontend
// RegisterNamespace RPC and talks to the frontend WorkflowService client directly,
// which is exactly the surface the corpus reaches through FrontendClient().
//
// The test t.Skip's when TOKEIRA_BIN is unset (via StartTokeirad), so it is safe
// to commit and no-ops on a checkout without a `tokeirad` binary present.
func TestTokeiraConformance_BasicWorkflowLifecycle(t *testing.T) {
	proc := testcore.StartTokeirad(t)
	proc.WaitReady(t, 30*time.Second)
	defer proc.Stop(t)

	// Talk to tokeirad directly via the frontend WorkflowService client. This
	// milestone proves the wire path to tokeirad; the onebox seam itself is
	// exercised by the run-all harness later, so SetFrontendEnv is not required
	// here.
	conn, err := grpc.NewClient(proc.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "dial tokeirad frontend %q", proc.Addr)
	defer func() { _ = conn.Close() }()
	client := workflowservice.NewWorkflowServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := testcore.RandomizeStr("tokeira-conformance-ns")

	// Register the namespace through the frontend RPC (not a direct persistence
	// write). Tolerate AlreadyExists so re-runs against a pinned address are idempotent.
	_, err = client.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		require.NoError(t, err, "RegisterNamespace")
	}

	// Namespace registration may propagate asynchronously; poll DescribeNamespace
	// (bounded) until the frontend can resolve it before driving any workflow.
	require.Eventually(t, func() bool {
		_, descErr := client.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: namespace,
		})
		return descErr == nil
	}, 15*time.Second, 200*time.Millisecond, "namespace %q never became visible via DescribeNamespace", namespace)

	const (
		workflowID   = "tokeira-conformance-basic-lifecycle"
		workflowType = "tokeira-conformance-basic-lifecycle-type"
		taskQueue    = "tokeira-conformance-basic-lifecycle-tq"
		identity     = "tokeira-conformance-worker"
	)

	// StartWorkflowExecution: the workflow is started over the frontend.
	startResp, err := client.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(30 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	})
	require.NoError(t, err, "StartWorkflowExecution")
	require.NotEmpty(t, startResp.GetRunId(), "StartWorkflowExecution returned empty RunId")

	// PollWorkflowTaskQueue: a worker picks up the first workflow task.
	pollResp, err := client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	require.NoError(t, err, "PollWorkflowTaskQueue")
	require.NotEmpty(t, pollResp.GetTaskToken(), "PollWorkflowTaskQueue returned no task")

	// RespondWorkflowTaskCompleted: a single COMPLETE_WORKFLOW_EXECUTION command
	// drives the workflow to completion — the minimal possible lifecycle.
	_, err = client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: namespace,
		TaskToken: pollResp.GetTaskToken(),
		Identity:  identity,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	require.NoError(t, err, "RespondWorkflowTaskCompleted")

	// GetWorkflowExecutionHistory: the recorded history must contain the canonical
	// open/close pair for a workflow that started and completed cleanly.
	histResp, err := client.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      startResp.GetRunId(),
		},
	})
	require.NoError(t, err, "GetWorkflowExecutionHistory")

	var sawStarted, sawCompleted bool
	for _, event := range histResp.GetHistory().GetEvents() {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
			sawStarted = true
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
			sawCompleted = true
		}
	}
	require.True(t, sawStarted, "history missing WorkflowExecutionStarted event")
	require.True(t, sawCompleted, "history missing WorkflowExecutionCompleted event")
}
