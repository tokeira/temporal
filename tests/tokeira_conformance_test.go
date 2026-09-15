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
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestTokeiraConformance_BasicWorkflowLifecycle proves namespace registration and
// workflow completion over the external frontend. The test environment owns the
// authorization callback listener required by conformance-mode engines, including
// when the run-all executor supplies an existing engine. Without that listener,
// an unavailable authorization bridge correctly makes namespace registration fail
// closed before the lifecycle can be exercised.
func TestTokeiraConformance_BasicWorkflowLifecycle(t *testing.T) {
	proc := testcore.StartTokeirad(t)
	t.Cleanup(func() { proc.Stop(t) })
	proc.WaitReady(t, 30*time.Second)
	proc.SetFrontendEnv(t)
	client := testcore.NewEnv(t).FrontendClient()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := testcore.RandomizeStr("tokeira-conformance-ns")

	// Register the namespace through the frontend RPC (not a direct persistence
	// write). Tolerate AlreadyExists so re-runs against a pinned address are idempotent.
	_, err := client.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		require.NoError(t, err, "RegisterNamespace")
	}

	// Namespace registration may propagate asynchronously; poll DescribeNamespace
	// (bounded) until the frontend can resolve it before driving any workflow.
	await.RequireTruef(t, func() bool {
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
		default:
			// Intermediate events do not satisfy either lifecycle boundary.
		}
	}
	require.True(t, sawStarted, "history missing WorkflowExecutionStarted event")
	require.True(t, sawCompleted, "history missing WorkflowExecutionCompleted event")
}
