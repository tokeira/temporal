package testcore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"google.golang.org/grpc"
)

type conformanceMatchingFrontend struct {
	workflowservice.WorkflowServiceClient
	response *workflowservice.DescribeWorkerDeploymentVersionResponse
	request  *workflowservice.DescribeWorkerDeploymentVersionRequest
}

func (f *conformanceMatchingFrontend) DescribeWorkerDeploymentVersion(
	_ context.Context,
	request *workflowservice.DescribeWorkerDeploymentVersionRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DescribeWorkerDeploymentVersionResponse, error) {
	f.request = request
	return f.response, nil
}

func TestConformanceMatchingClientChecksPublicDeploymentProjection(t *testing.T) {
	namespaces := newConformanceNamespaceSet()
	namespaces.addID("namespace-id", "namespace-name")
	frontend := &conformanceMatchingFrontend{
		response: &workflowservice.DescribeWorkerDeploymentVersionResponse{
			VersionTaskQueues: []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue{
				{
					Name: "workflow-queue",
					Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				},
			},
		},
	}
	client := &conformanceMatchingClient{frontend: frontend, namespaces: namespaces}

	response, err := client.CheckTaskQueueVersionMembership(
		context.Background(),
		&matchingservice.CheckTaskQueueVersionMembershipRequest{
			NamespaceId:   "namespace-id",
			TaskQueue:     "workflow-queue",
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: "deployment",
				BuildId:        "build-id",
			},
		},
	)
	require.NoError(t, err)
	require.True(t, response.GetIsMember())
	require.Equal(t, "namespace-name", frontend.request.GetNamespace())
	require.Equal(t, &deploymentpb.WorkerDeploymentVersion{
		DeploymentName: "deployment",
		BuildId:        "build-id",
	}, frontend.request.GetDeploymentVersion())
}

func TestConformanceMatchingClientRejectsUnknownNamespaceID(t *testing.T) {
	client := &conformanceMatchingClient{
		frontend:   &conformanceMatchingFrontend{},
		namespaces: newConformanceNamespaceSet(),
	}

	response, err := client.CheckTaskQueueVersionMembership(
		context.Background(),
		&matchingservice.CheckTaskQueueVersionMembershipRequest{NamespaceId: "unknown"},
	)
	require.ErrorContains(t, err, "namespace id \"unknown\" is not registered")
	require.Nil(t, response)
}
