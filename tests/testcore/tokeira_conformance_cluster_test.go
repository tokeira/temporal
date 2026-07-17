package testcore

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/authorization"
	"google.golang.org/grpc"
)

func TestConformanceAuthorizationBridgeRoutesByExactNamespace(t *testing.T) {
	hostA := &TemporalImpl{}
	hostB := &TemporalImpl{}
	namespacesA := newConformanceNamespaceSet()
	namespacesA.add("namespace-a")
	namespacesB := newConformanceNamespaceSet()
	namespacesB.add("namespace-b")
	hostA.SetOnAuthorize(func(
		context.Context,
		*authorization.Claims,
		*authorization.CallTarget,
	) (authorization.Result, error) {
		return authorization.Result{Decision: authorization.DecisionDeny, Reason: "host-a"}, nil
	})
	hostB.SetOnAuthorize(func(
		context.Context,
		*authorization.Claims,
		*authorization.CallTarget,
	) (authorization.Result, error) {
		return authorization.Result{Decision: authorization.DecisionDeny, Reason: "host-b"}, nil
	})
	registerConformanceAuthorizationHost(hostA, namespacesA)
	registerConformanceAuthorizationHost(hostB, namespacesB)
	t.Cleanup(func() {
		unregisterConformanceAuthorizationHost(hostA)
		unregisterConformanceAuthorizationHost(hostB)
	})

	call := func(namespace string) (int, conformanceAuthorizeResponse) {
		body := `{"api_name":"api","namespace":"` + namespace + `"}`
		request := httptest.NewRequest(http.MethodPost, "/authorize", strings.NewReader(body))
		response := httptest.NewRecorder()
		handleConformanceAuthorize(response, request)
		var decoded conformanceAuthorizeResponse
		if response.Code == http.StatusOK {
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
		}
		return response.Code, decoded
	}

	status, response := call("namespace-a")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "deny", response.Decision)
	require.Equal(t, "host-a", *response.Reason)
	status, response = call("namespace-b")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "host-b", *response.Reason)
	status, _ = call("unknown")
	require.Equal(t, http.StatusNotFound, status)

	unregisterConformanceAuthorizationHost(hostA)
	status, _ = call("namespace-a")
	require.Equal(t, http.StatusNotFound, status)
}

type conformanceNexusOperator struct {
	operatorservice.OperatorServiceClient
	endpoints []*nexuspb.Endpoint
}

func (o *conformanceNexusOperator) ListNexusEndpoints(
	context.Context,
	*operatorservice.ListNexusEndpointsRequest,
	...grpc.CallOption,
) (*operatorservice.ListNexusEndpointsResponse, error) {
	return &operatorservice.ListNexusEndpointsResponse{Endpoints: o.endpoints}, nil
}

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

func TestConformanceNexusStatePreservesInternalListContract(t *testing.T) {
	// A previous functional cluster can own the namespace while endpoints remain visible
	// cluster-wide to this cluster. The process catalog must preserve the stored ID.
	registeredNamespaces := newConformanceNamespaceSet()
	registeredNamespaces.addID("namespace-id", "namespace-name")
	currentNamespaces := newConformanceNamespaceSet()
	workerTarget := func() *nexuspb.EndpointTarget {
		return &nexuspb.EndpointTarget{
			Variant: &nexuspb.EndpointTarget_Worker_{
				Worker: &nexuspb.EndpointTarget_Worker{
					Namespace: "namespace-name",
					TaskQueue: "task-queue",
				},
			},
		}
	}
	operator := &conformanceNexusOperator{
		endpoints: []*nexuspb.Endpoint{
			{Id: "c", Version: 1, Spec: &nexuspb.EndpointSpec{Name: "c", Target: workerTarget()}},
			{Id: "a", Version: 1, Spec: &nexuspb.EndpointSpec{Name: "a", Target: workerTarget()}},
			{Id: "b", Version: 1, Spec: &nexuspb.EndpointSpec{Name: "b", Target: workerTarget()}},
		},
	}
	state := newConformanceNexusEndpointState(operator, &conformanceMatchingFrontend{}, currentNamespaces)
	state.advanceTableVersion()

	tableVersion, nextPageToken, entries, err := state.list(
		context.Background(),
		1,
		nil,
		2,
		false,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), tableVersion)
	require.Equal(t, []byte("c"), nextPageToken)
	require.Equal(t, []string{"a", "b"}, []string{entries[0].GetId(), entries[1].GetId()})
	require.Equal(t, "namespace-id", entries[0].GetEndpoint().GetSpec().GetTarget().GetWorker().GetNamespaceId())
	require.NotNil(t, entries[0].GetEndpoint().GetClock())

	_, _, entries, err = state.list(context.Background(), 1, nextPageToken, 2, false)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "c", entries[0].GetId())

	_, _, _, err = state.list(context.Background(), 2, nil, 2, false)
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)

	type listResult struct {
		tableVersion int64
		entries      int
		err          error
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan listResult, 1)
	go func() {
		version, _, waitedEntries, waitErr := state.list(ctx, 1, nil, 3, true)
		result <- listResult{tableVersion: version, entries: len(waitedEntries), err: waitErr}
	}()
	state.advanceTableVersion()
	waited := <-result
	require.NoError(t, waited.err)
	require.Equal(t, int64(2), waited.tableVersion)
	require.Equal(t, 3, waited.entries)
}
