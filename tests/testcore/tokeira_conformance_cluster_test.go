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
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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
	hostA.SetOnGetClaims(func(info *authorization.AuthInfo) (*authorization.Claims, error) {
		require.Equal(t, "Bearer test", info.AuthToken)
		require.Equal(t, "extra", info.ExtraData)
		return &authorization.Claims{Subject: "mapped-subject"}, nil
	})
	hostA.SetOnAuthorize(func(
		_ context.Context,
		claims *authorization.Claims,
		_ *authorization.CallTarget,
	) (authorization.Result, error) {
		if claims != nil {
			return authorization.Result{Decision: authorization.DecisionDeny, Reason: claims.Subject}, nil
		}
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

	call := func(namespace, authToken, extraData string) (int, conformanceAuthorizeResponse) {
		body := `{"api_name":"api","namespace":"` + namespace +
			`","auth_token":"` + authToken + `","extra_data":"` + extraData + `"}`
		request := httptest.NewRequest(http.MethodPost, "/authorize", strings.NewReader(body))
		response := httptest.NewRecorder()
		handleConformanceAuthorize(response, request)
		var decoded conformanceAuthorizeResponse
		if response.Code == http.StatusOK {
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
		}
		return response.Code, decoded
	}

	status, response := call("namespace-a", "", "")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "deny", response.Decision)
	require.Equal(t, "host-a", *response.Reason)
	status, response = call("namespace-a", "Bearer test", "extra")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "mapped-subject", *response.Reason)
	status, response = call("namespace-b", "", "")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "host-b", *response.Reason)
	status, _ = call("unknown", "", "")
	require.Equal(t, http.StatusNotFound, status)

	unregisterConformanceAuthorizationHost(hostA)
	status, _ = call("namespace-a", "", "")
	require.Equal(t, http.StatusNotFound, status)
}

type conformanceNexusOperator struct {
	operatorservice.OperatorServiceClient
	endpoints []*nexuspb.Endpoint
	deleted   []string
}

func (o *conformanceNexusOperator) DeleteNexusEndpoint(
	_ context.Context,
	request *operatorservice.DeleteNexusEndpointRequest,
	_ ...grpc.CallOption,
) (*operatorservice.DeleteNexusEndpointResponse, error) {
	o.deleted = append(o.deleted, request.GetId())
	return &operatorservice.DeleteNexusEndpointResponse{}, nil
}

func TestCleanupConformanceNexusEndpointsForNamespaceIsExact(t *testing.T) {
	workerTarget := func(namespace string) *nexuspb.EndpointTarget {
		return &nexuspb.EndpointTarget{
			Variant: &nexuspb.EndpointTarget_Worker_{
				Worker: &nexuspb.EndpointTarget_Worker{
					Namespace: namespace,
					TaskQueue: "task-queue",
				},
			},
		}
	}
	operator := &conformanceNexusOperator{endpoints: []*nexuspb.Endpoint{
		{Id: "exact", Version: 1, Spec: &nexuspb.EndpointSpec{Target: workerTarget("namespace-a")}},
		{Id: "prefix", Version: 1, Spec: &nexuspb.EndpointSpec{Target: workerTarget("namespace-a-sibling")}},
		{Id: "foreign", Version: 1, Spec: &nexuspb.EndpointSpec{Target: workerTarget("namespace-b")}},
	}}

	require.NoError(t, cleanupConformanceNexusEndpointsForNamespace(
		context.Background(),
		operator,
		"namespace-a",
	))
	require.Equal(t, []string{"exact"}, operator.deleted)
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
	response                   *workflowservice.DescribeWorkerDeploymentVersionResponse
	request                    *workflowservice.DescribeWorkerDeploymentVersionRequest
	listResponse               *workflowservice.ListWorkerDeploymentsResponse
	describeDeploymentResponse *workflowservice.DescribeWorkerDeploymentResponse
}

func (f *conformanceMatchingFrontend) ListWorkerDeployments(
	_ context.Context,
	_ *workflowservice.ListWorkerDeploymentsRequest,
	_ ...grpc.CallOption,
) (*workflowservice.ListWorkerDeploymentsResponse, error) {
	return f.listResponse, nil
}

func (f *conformanceMatchingFrontend) DescribeWorkerDeployment(
	_ context.Context,
	_ *workflowservice.DescribeWorkerDeploymentRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DescribeWorkerDeploymentResponse, error) {
	return f.describeDeploymentResponse, nil
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

func TestConformanceMatchingClientProjectsTaskQueueUserDataFromPublicDeploymentAPIs(t *testing.T) {
	namespaces := newConformanceNamespaceSet()
	namespaces.addID("namespace-id", "namespace-name")
	version := &deploymentpb.WorkerDeploymentVersion{
		DeploymentName: "deployment",
		BuildId:        "build-id",
	}
	frontend := &conformanceMatchingFrontend{
		listResponse: &workflowservice.ListWorkerDeploymentsResponse{
			WorkerDeployments: []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
				{Name: "deployment"},
			},
		},
		describeDeploymentResponse: &workflowservice.DescribeWorkerDeploymentResponse{
			WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
				Name: "deployment",
				VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
					{DeploymentVersion: version},
				},
			},
		},
		response: &workflowservice.DescribeWorkerDeploymentVersionResponse{
			WorkerDeploymentVersionInfo: &deploymentpb.WorkerDeploymentVersionInfo{
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			},
			VersionTaskQueues: []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue{
				{Name: "workflow-queue", Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			},
		},
	}
	client := &conformanceMatchingClient{frontend: frontend, namespaces: namespaces}

	response, err := client.GetTaskQueueUserData(
		context.Background(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   "namespace-id",
			TaskQueue:     "/_sys/workflow-queue/3",
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		},
	)
	require.NoError(t, err)
	versionData := response.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].
		GetDeploymentData().GetDeploymentsData()["deployment"].GetVersions()["build-id"]
	require.NotNil(t, versionData)
	require.Zero(t, versionData.GetRevisionNumber())
	require.False(t, versionData.GetDeleted())
	require.Equal(t, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, versionData.GetStatus())
}

type conformanceHistoryAdmin struct {
	adminservice.AdminServiceClient
	request  *adminservice.DescribeMutableStateRequest
	response *adminservice.DescribeMutableStateResponse
}

func (a *conformanceHistoryAdmin) DescribeMutableState(
	_ context.Context,
	request *adminservice.DescribeMutableStateRequest,
	_ ...grpc.CallOption,
) (*adminservice.DescribeMutableStateResponse, error) {
	a.request = request
	return a.response, nil
}

func TestConformanceHistoryClientProjectsStickyQueueFromAdminService(t *testing.T) {
	namespaces := newConformanceNamespaceSet()
	namespaces.addID("namespace-id", "namespace-name")
	execution := &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "run-id"}
	admin := &conformanceHistoryAdmin{
		response: &adminservice.DescribeMutableStateResponse{
			DatabaseMutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					StickyTaskQueue: "sticky-queue",
				},
			},
		},
	}
	client := &conformanceHistoryClient{admin: admin, namespaces: namespaces}

	response, err := client.GetMutableState(
		context.Background(),
		&historyservice.GetMutableStateRequest{
			NamespaceId: "namespace-id",
			Execution:   execution,
		},
	)
	require.NoError(t, err)
	require.Equal(t, "namespace-name", admin.request.GetNamespace())
	require.Equal(t, execution, admin.request.GetExecution())
	require.Equal(t, "sticky-queue", response.GetStickyTaskQueue().GetName())
	require.True(t, response.GetIsStickyTaskQueueEnabled())
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
