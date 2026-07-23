package testcore

// Tokeira Tier-2 functional-conformance cluster (Shape-2, Option B).
//
// This file is a PARALLEL construction path for `TestCluster`, used only when the
// conformance seam env (TOKEIRA_CONFORMANCE_FRONTEND_ADDR) points at an externally
// running `tokeirad`. It deliberately does NOT touch the standard
// `newClusterWithPersistenceTestBaseFactory` path: Temporal's `test_cluster.go` and
// `functional_test_base.go` are hot upstream files that churn between releases, so
// threading conformance branches through them would conflict on every rebase. Keeping
// the conformance construction in its own file is the low-maintenance posture for a
// pinned fork — the only upstream touch is one env-aware line in NewTestClusterFactory.
//
// Why a client-shim cluster (Option B): under Shape-2, `tokeirad` is the backend and is
// reachable only over the gRPC wire. The standard path stands up a full Temporal
// persistence test-base (SQLite/Cassandra/ES) and fx-boots the services before
// `Start()` — none of which `tokeirad` uses. Option B skips all of it: the shim
// `TestCluster` builds no persistence and boots no services; its `host` is just the
// external-frontend WorkflowService client. 74 of 77 functional suites talk only over
// FrontendClient(), so they flow through this shim unchanged. The handful that inspect
// Temporal's own persistence directly (archival, namespace_delete, client_misc) are
// out-of-public-scope and are skipped, not supported.
//
// Why a MetadataManager adapter: FunctionalTestBase.RegisterNamespace registers by
// calling `testBase.MetadataManager.CreateNamespace` directly (not over the wire). To
// keep that method UNMODIFIED, the shim builds a minimal `*persistencetests.TestBase`
// whose `MetadataManager` is an adapter that forwards CreateNamespace to `tokeirad`'s
// frontend RegisterNamespace RPC, and whose `ClusterMetadata` provides the cluster name
// RegisterNamespace reads. Both fields are interface-typed, so the adapter drops in with
// zero edits to functional_test_base.go.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/grpcinject"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// errConformanceUnsupported is returned by adapter methods that are not part of the
// Tier-2 conformance surface. Only CreateNamespace is forwarded to the frontend; any
// other MetadataManager call indicates a test reaching past the public wire contract
// (out-of-public-scope), and is surfaced as a clear error rather than a nil panic.
var errConformanceUnsupported = errors.New(
	"tokeira conformance: persistence MetadataManager operation is not supported in Shape-2 mode " +
		"(only CreateNamespace, forwarded to the frontend RegisterNamespace RPC, is available)",
)

// conformanceClusterFactory is the TestClusterFactory used when the conformance seam is
// active. It builds the client-shim cluster instead of the persistence-backed one.
type conformanceClusterFactory struct{}

func (f *conformanceClusterFactory) NewCluster(
	t *testing.T,
	clusterConfig *TestClusterConfig,
	logger log.Logger,
) (*TestCluster, error) {
	return newConformanceCluster(t, clusterConfig, logger)
}

// conformanceServiceErrorInterceptor mirrors Temporal's client-side `errorInterceptor`
// (`common/rpc/grpc.go @ v1.31.0`): it converts the raw gRPC status returned by the
// invoker into the corresponding typed `*serviceerror.*`, so the unmodified functional
// corpus's `ErrorAs(err, &*serviceerror.X)` assertions resolve as they do against a real
// Temporal frontend.
func conformanceServiceErrorInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err == nil {
		return nil
	}
	return serviceerror.FromStatus(status.Convert(err))
}

// newConformanceCluster builds a client-shim TestCluster fronting an external `tokeirad`.
//
// It dials the address from TOKEIRA_CONFORMANCE_FRONTEND_ADDR (set by the harness via
// StartTokeirad + SetFrontendEnv), wires a WorkflowService/OperatorService client over
// that single connection, and assembles a minimal TestBase carrying only the two fields
// RegisterNamespace touches. No persistence store, no fx services, no archival.
func newConformanceCluster(
	t *testing.T,
	clusterConfig *TestClusterConfig,
	logger log.Logger,
) (*TestCluster, error) {
	addr := conformanceFrontendAddr()
	if addr == "" {
		return nil, errors.New(
			"tokeira conformance: newConformanceCluster called without TOKEIRA_CONFORMANCE_FRONTEND_ADDR set",
		)
	}

	// Convert gRPC statuses into typed `*serviceerror.*` on the client, exactly as
	// Temporal's real client does (`errorInterceptor` in common/rpc/grpc.go @ v1.31.0:
	// `serviceerror.FromStatus(status.Convert(err))`). Without this the shim returns raw
	// `*status.Error`, so functional tests doing `ErrorAs(err, &*serviceerror.X)` fail on
	// typing even when the server returns the correct code and message. Matching the real
	// client's behaviour keeps the shim transparent to the unmodified corpus.
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(conformanceServiceErrorInterceptor),
	)
	if err != nil {
		return nil, fmt.Errorf("tokeira conformance: dial frontend %q: %w", addr, err)
	}
	frontendClient := workflowservice.NewWorkflowServiceClient(conn)
	operatorClient := operatorservice.NewOperatorServiceClient(conn)
	adminClient := adminservice.NewAdminServiceClient(conn)

	clusterMetadataConfig := cluster.NewTestClusterMetadataConfig(
		clusterConfig.ClusterMetadata.EnableGlobalNamespace,
		clusterConfig.IsMasterCluster,
	)
	clusterMetadata := cluster.NewMetadataFromConfig(
		clusterMetadataConfig,
		nil, // clusterMetadataStore: nil — the shim never reads cluster metadata from persistence.
		dynamicconfig.NewNoopCollection(),
		logger,
	)

	// Minimal TestBase: only MetadataManager + ClusterMetadata are set, which is exactly
	// what FunctionalTestBase.RegisterNamespace reads. Every other field stays nil; a test
	// touching them is out-of-public-scope and is skipped (see tokeira_conformance_skip.go).
	// Shared between the metadata manager (records each registered namespace) and the
	// metrics bridge (scopes its scrape to them), so a capture only sees this cluster's
	// own namespace series on the shared tokeirad /metrics.
	namespaces := newConformanceNamespaceSet()
	nexusEndpoints := newConformanceNexusEndpointState(operatorClient, frontendClient, namespaces)
	testBase := &persistencetests.TestBase{
		MetadataManager:      &conformanceMetadataManager{frontend: frontendClient, namespaces: namespaces},
		NexusEndpointManager: &conformanceNexusEndpointManager{state: nexusEndpoints},
		ClusterMetadata:      clusterMetadata,
	}

	host := &TemporalImpl{
		logger:         logger,
		frontendClient: frontendClient,
		operatorClient: operatorClient,
		executionManager: &conformanceExecutionManager{
			admin:      adminClient,
			namespaces: namespaces,
		},
		matchingClient: &conformanceMatchingClient{
			frontend:   frontendClient,
			namespaces: namespaces,
			nexus:      nexusEndpoints,
		},
		historyClient: &conformanceHistoryClient{
			admin:      adminClient,
			namespaces: namespaces,
		},
		// tokeirad serves a minimal AdminService (DescribeMutableState) on the same
		// port; the reset suite reads a run's ResetRunId/status through it.
		adminClient:           adminClient,
		clusterMetadataConfig: clusterMetadataConfig,
		dcClient:              dynamicconfig.NewMemoryClient(),
		// SetupTest/TearDownTest call host.grpcClientInterceptor.Set(...) unconditionally to
		// annotate outgoing RPCs with the test name; setupSdk also reads it for dial options.
		// The shim must supply a real (initially no-op) interceptor or those calls nil-panic.
		grpcClientInterceptor: grpcinject.NewInterceptor(),
		// InjectHook (used by *_Batching tests) stores into this in-process registry via
		// `c.testHooks.data.Store`; the zero-value `TestHooks` has a nil map and would
		// nil-panic. A real registry makes InjectHook a harmless no-op: the hook is an
		// in-process server knob (e.g. `TaskQueuesInDeploymentSyncBatchSize`) that cannot
		// reach the out-of-process `tokeirad`, so the test still verifies observable
		// behaviour while the batch-size override is simply ignored (same posture as
		// dynamic-config overrides against an external server).
		testHooks: testhooks.NewTestHooks(),
		hostsByProtocolByService: map[transferProtocol]map[primitives.ServiceName]static.Hosts{
			grpcProtocol: {primitives.FrontendService: {All: []string{addr}, Self: addr}},
			httpProtocol: {primitives.FrontendService: {All: []string{addr}, Self: addr}},
		},
		frontendMembershipAddress: addr,
	}
	registerConformanceAuthorizationHost(host, namespaces)

	// The standard onebox constructor applies the corpus-wide ClientSuiteLimit
	// defaults while assembling its TemporalImpl. Shape-2 bypasses that
	// constructor, so deliver the four pending-command limits that the external
	// server actually supports. Forwarding every onebox default would only
	// generate unsupported-key noise for unrelated in-process service knobs.
	clientSuiteLimitKeys := []dynamicconfig.Key{
		dynamicconfig.NumPendingChildExecutionsLimitError.Key(),
		dynamicconfig.NumPendingActivitiesLimitError.Key(),
		dynamicconfig.NumPendingCancelRequestsLimitError.Key(),
		dynamicconfig.NumPendingSignalsLimitError.Key(),
	}
	for _, key := range clientSuiteLimitKeys {
		host.overrideDynamicConfig(t, key, dynamicConfigOverrides[key])
	}
	// Suite-scoped overrides remain independent of those corpus-wide defaults.
	// Cleanup is tied to the suite's testing.T, preserving the ordinary onebox
	// lifetime.
	for key, value := range clusterConfig.DynamicConfigOverrides {
		host.overrideDynamicConfig(t, key, value)
	}

	// Tier-2 metrics bridge: when the harness exported tokeirad's /metrics address, install
	// a CaptureMetricsHandler backed by a scrape-and-diff source so metric-asserting corpus
	// tests (e.g. the Nexus outbound-request tests) observe tokeira's genuine emissions
	// under Temporal metric names. Absent the env (no metrics port, or a pinned external
	// frontend without one), captureMetricsHandler stays nil exactly as before.
	if metricsAddr := os.Getenv(tokeiraMetricsAddrEnv); metricsAddr != "" {
		host.captureMetricsHandler = metricstest.NewCaptureHandlerWithSource(
			newTokeiraMetricsScrapeSource("http://"+metricsAddr+"/metrics", namespaces),
		)
	}

	return &TestCluster{testBase: testBase, host: host}, nil
}

// cleanupConformanceNexusEndpointsForNamespace restores per-TestEnv endpoint-catalog
// isolation before a pooled dedicated cluster is reused by a sibling corpus leaf.
func cleanupConformanceNexusEndpointsForNamespace(
	ctx context.Context,
	operatorClient operatorservice.OperatorServiceClient,
	targetNamespace string,
) error {
	if operatorClient == nil {
		return nil
	}
	var endpoints []*nexuspb.Endpoint
	var pageToken []byte
	for {
		response, err := operatorClient.ListNexusEndpoints(
			ctx,
			&operatorservice.ListNexusEndpointsRequest{
				PageSize:      1000,
				NextPageToken: pageToken,
			},
		)
		if err != nil {
			return fmt.Errorf("tokeira conformance: list Nexus endpoints during teardown: %w", err)
		}
		endpoints = append(endpoints, response.GetEndpoints()...)
		if len(response.GetNextPageToken()) == 0 {
			break
		}
		pageToken = response.GetNextPageToken()
	}
	for _, endpoint := range endpoints {
		worker := endpoint.GetSpec().GetTarget().GetWorker()
		if worker == nil || worker.GetNamespace() != targetNamespace {
			continue
		}
		_, err := operatorClient.DeleteNexusEndpoint(
			ctx,
			&operatorservice.DeleteNexusEndpointRequest{
				Id:      endpoint.GetId(),
				Version: endpoint.GetVersion(),
			},
		)
		if err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf(
				"tokeira conformance: delete Nexus endpoint %q during teardown: %w",
				endpoint.GetId(),
				err,
			)
		}
	}
	return nil
}

// conformanceMetadataManager forwards namespace registration to `tokeirad`'s frontend so
// FunctionalTestBase.RegisterNamespace runs unmodified over the wire. Only CreateNamespace
// is implemented; the rest of the MetadataManager surface returns errConformanceUnsupported.
type conformanceMetadataManager struct {
	frontend workflowservice.WorkflowServiceClient
	// namespaces records every namespace registered through THIS cluster, so the
	// metrics bridge can scope its scrape to them. Under Shape-2 all dedicated
	// clusters share one out-of-process tokeirad and one /metrics, so without this
	// scope a capture window would pick up concurrently-running sibling sub-tests'
	// namespace-labelled series (the per-cluster metric isolation the in-process
	// server gets from a separate registry per cluster). May be nil when no metrics
	// bridge is installed.
	namespaces *conformanceNamespaceSet
}

// conformanceNamespaceSet is a thread-safe set of namespace names registered through a
// single conformance cluster, shared between its metadata manager (writer) and its metrics
// bridge source (reader).
type conformanceNamespaceSet struct {
	mu    sync.Mutex
	names map[string]struct{}
	ids   map[string]string
}

// Nexus endpoints are cluster-global and retain the namespace ID stored at creation even
// after the functional suite that registered that namespace tears down. Keep that identity
// catalog process-wide while leaving each cluster's metrics scope local to its own set.
var conformanceNamespaceIDsByName sync.Map

func newConformanceNamespaceSet() *conformanceNamespaceSet {
	return &conformanceNamespaceSet{
		names: map[string]struct{}{},
		ids:   map[string]string{},
	}
}

func (s *conformanceNamespaceSet) add(name string) {
	if name == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.names[name] = struct{}{}
}

func (s *conformanceNamespaceSet) addID(id, name string) {
	if id == "" || name == "" {
		return
	}
	s.mu.Lock()
	s.ids[id] = name
	s.mu.Unlock()
	conformanceNamespaceIDsByName.Store(name, id)
}

func (s *conformanceNamespaceSet) nameForID(id string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name, ok := s.ids[id]
	return name, ok
}

func (s *conformanceNamespaceSet) idForName(name string) (string, bool) {
	s.mu.Lock()
	for id, registeredName := range s.ids {
		if registeredName == name {
			s.mu.Unlock()
			return id, true
		}
	}
	s.mu.Unlock()
	id, ok := conformanceNamespaceIDsByName.Load(name)
	if !ok {
		return "", false
	}
	registeredID, ok := id.(string)
	return registeredID, ok
}

// contains reports whether name was registered. It also reports true when the set is empty
// (fail open): a bridge with no registered namespaces yet must not silently drop everything.
func (s *conformanceNamespaceSet) contains(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.names) == 0 {
		return true
	}
	_, ok := s.names[name]
	return ok
}

// containsExact is the authorization bridge's strict namespace lookup. Unlike
// metrics filtering, an empty set must not fail open: that could route one
// dedicated cluster's authorization callback into another cluster.
func (s *conformanceNamespaceSet) containsExact(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.names[name]
	return ok
}

func (m *conformanceMetadataManager) GetName() string { return "tokeira-conformance" }

func (m *conformanceMetadataManager) Close() {}

// CreateNamespace translates the persistence request into a frontend RegisterNamespace
// RPC. AlreadyExists is tolerated so repeated registrations against a long-lived tokeirad
// are idempotent. The persistence response's ID is not meaningful here (tokeirad assigns
// the real ID); tests that depend on the internal ID are out-of-public-scope.
func (m *conformanceMetadataManager) CreateNamespace(
	ctx context.Context,
	request *persistence.CreateNamespaceRequest,
) (*persistence.CreateNamespaceResponse, error) {
	info := request.Namespace.GetInfo()
	cfg := request.Namespace.GetConfig()

	// Record the namespace so the metrics bridge scopes its scrape to this cluster.
	if m.namespaces != nil {
		m.namespaces.add(info.GetName())
	}

	req := &workflowservice.RegisterNamespaceRequest{
		Namespace:   info.GetName(),
		Description: info.GetDescription(),
	}
	if cfg != nil {
		req.WorkflowExecutionRetentionPeriod = cfg.GetRetention()
	}
	if req.WorkflowExecutionRetentionPeriod == nil {
		req.WorkflowExecutionRetentionPeriod = durationpb.New(0)
	}

	_, err := m.frontend.RegisterNamespace(ctx, req)
	if err != nil && status.Code(err) != codes.AlreadyExists {
		if _, ok := err.(*serviceerror.NamespaceAlreadyExists); !ok {
			return nil, fmt.Errorf("tokeira conformance: RegisterNamespace %q: %w", info.GetName(), err)
		}
	}

	// tokeirad assigns its own namespace id (a hash of the name), which is what
	// every history event carries. The harness picked an arbitrary id in the
	// CreateNamespaceRequest, but suites compare event `NamespaceId` against
	// s.NamespaceID(), so return tokeirad's real id — read back via
	// DescribeNamespace — rather than the harness's throwaway one.
	id := info.GetId()
	if desc, descErr := m.frontend.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: info.GetName(),
	}); descErr == nil && desc.GetNamespaceInfo().GetId() != "" {
		id = desc.GetNamespaceInfo().GetId()
	}
	if m.namespaces != nil {
		m.namespaces.addID(id, info.GetName())
	}
	return &persistence.CreateNamespaceResponse{ID: id}, nil
}

// conformanceMatchingClient adapts the internal matching-service reads used by public-behaviour
// corpus tests to equivalent public service projections. Embedding the generated client keeps every
// other internal method unavailable rather than accidentally fabricating server state.
type conformanceMatchingClient struct {
	matchingservice.MatchingServiceClient
	frontend   workflowservice.WorkflowServiceClient
	namespaces *conformanceNamespaceSet
	nexus      *conformanceNexusEndpointState
}

// conformanceHistoryClient projects the one read-only HistoryService observation used by
// the V3 corpus through tokeirad's minimal AdminService. The adapter exposes the current
// sticky queue only; every other HistoryService method remains unavailable through the
// embedded nil generated client. This preserves the unmodified corpus without inventing
// a Temporal history-service topology inside Tokeira.
type conformanceHistoryClient struct {
	historyservice.HistoryServiceClient
	admin      adminservice.AdminServiceClient
	namespaces *conformanceNamespaceSet
}

// conformanceExecutionManager projects the one read-only persistence observation in
// ClientMisc through tokeirad's scoped AdminService response. Embedding the interface
// leaves every mutating or unrelated persistence method unavailable; this adapter does
// not introduce an in-process persistence topology.
type conformanceExecutionManager struct {
	persistence.ExecutionManager
	admin      adminservice.AdminServiceClient
	namespaces *conformanceNamespaceSet
}

func (c *conformanceExecutionManager) GetWorkflowExecution(
	ctx context.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.GetWorkflowExecutionResponse, error) {
	namespace, ok := c.namespaces.nameForID(request.NamespaceID)
	if !ok {
		return nil, status.Errorf(
			codes.NotFound,
			"tokeira conformance: namespace id %q is not registered",
			request.NamespaceID,
		)
	}
	response, err := c.admin.DescribeMutableState(
		ctx,
		&adminservice.DescribeMutableStateRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: request.WorkflowID,
				RunId:      request.RunID,
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return &persistence.GetWorkflowExecutionResponse{
		State: response.GetDatabaseMutableState(),
	}, nil
}

func (c *conformanceHistoryClient) GetMutableState(
	ctx context.Context,
	request *historyservice.GetMutableStateRequest,
	_ ...grpc.CallOption,
) (*historyservice.GetMutableStateResponse, error) {
	namespace, ok := c.namespaces.nameForID(request.GetNamespaceId())
	if !ok {
		return nil, status.Errorf(
			codes.NotFound,
			"tokeira conformance: namespace id %q is not registered",
			request.GetNamespaceId(),
		)
	}
	response, err := c.admin.DescribeMutableState(
		ctx,
		&adminservice.DescribeMutableStateRequest{
			Namespace: namespace,
			Execution: request.GetExecution(),
		},
	)
	if err != nil {
		return nil, err
	}
	stickyName := response.GetDatabaseMutableState().GetExecutionInfo().GetStickyTaskQueue()
	return &historyservice.GetMutableStateResponse{
		Execution:                request.GetExecution(),
		StickyTaskQueue:          &taskqueuepb.TaskQueue{Name: stickyName},
		IsStickyTaskQueueEnabled: stickyName != "",
	}, nil
}

func (c *conformanceMatchingClient) CheckTaskQueueVersionMembership(
	ctx context.Context,
	request *matchingservice.CheckTaskQueueVersionMembershipRequest,
	_ ...grpc.CallOption,
) (*matchingservice.CheckTaskQueueVersionMembershipResponse, error) {
	namespace, ok := c.namespaces.nameForID(request.GetNamespaceId())
	if !ok {
		return nil, fmt.Errorf(
			"tokeira conformance: namespace id %q is not registered in this cluster",
			request.GetNamespaceId(),
		)
	}
	version := request.GetVersion()
	if version == nil {
		return &matchingservice.CheckTaskQueueVersionMembershipResponse{}, nil
	}

	response, err := c.frontend.DescribeWorkerDeploymentVersion(
		ctx,
		&workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: namespace,
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: version.GetDeploymentName(),
				BuildId:        version.GetBuildId(),
			},
		},
	)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return &matchingservice.CheckTaskQueueVersionMembershipResponse{}, nil
		}
		return nil, err
	}

	for _, taskQueue := range response.GetVersionTaskQueues() {
		if taskQueue.GetName() == request.GetTaskQueue() && taskQueue.GetType() == request.GetTaskQueueType() {
			return &matchingservice.CheckTaskQueueVersionMembershipResponse{IsMember: true}, nil
		}
	}
	return &matchingservice.CheckTaskQueueVersionMembershipResponse{}, nil
}

func (c *conformanceMatchingClient) GetTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.GetTaskQueueUserDataRequest,
	_ ...grpc.CallOption,
) (*matchingservice.GetTaskQueueUserDataResponse, error) {
	namespace, ok := c.namespaces.nameForID(request.GetNamespaceId())
	if !ok {
		return nil, fmt.Errorf(
			"tokeira conformance: namespace id %q is not registered in this cluster",
			request.GetNamespaceId(),
		)
	}
	partition, err := tqid.NormalPartitionFromRpcName(
		request.GetTaskQueue(),
		request.GetNamespaceId(),
		request.GetTaskQueueType(),
	)
	if err != nil {
		return nil, err
	}
	taskQueueFamily := partition.TaskQueue().Name()

	deploymentData := &persistencespb.DeploymentData{
		DeploymentsData: make(map[string]*persistencespb.WorkerDeploymentData),
	}
	var pageToken []byte
	for {
		page, err := c.frontend.ListWorkerDeployments(
			ctx,
			&workflowservice.ListWorkerDeploymentsRequest{
				Namespace:     namespace,
				PageSize:      100,
				NextPageToken: pageToken,
			},
		)
		if err != nil {
			return nil, err
		}
		for _, summary := range page.GetWorkerDeployments() {
			deployment, err := c.frontend.DescribeWorkerDeployment(
				ctx,
				&workflowservice.DescribeWorkerDeploymentRequest{
					Namespace:      namespace,
					DeploymentName: summary.GetName(),
				},
			)
			if err != nil {
				if status.Code(err) == codes.NotFound {
					continue
				}
				return nil, err
			}
			info := deployment.GetWorkerDeploymentInfo()
			for _, versionSummary := range info.GetVersionSummaries() {
				version := versionSummary.GetDeploymentVersion()
				if version == nil {
					continue
				}
				versionInfo, err := c.frontend.DescribeWorkerDeploymentVersion(
					ctx,
					&workflowservice.DescribeWorkerDeploymentVersionRequest{
						Namespace:         namespace,
						DeploymentVersion: version,
					},
				)
				if err != nil {
					if status.Code(err) == codes.NotFound {
						continue
					}
					return nil, err
				}
				for _, taskQueue := range versionInfo.GetVersionTaskQueues() {
					if taskQueue.GetName() != taskQueueFamily ||
						taskQueue.GetType() != request.GetTaskQueueType() {
						continue
					}
					entry := deploymentData.DeploymentsData[version.GetDeploymentName()]
					if entry == nil {
						entry = &persistencespb.WorkerDeploymentData{
							RoutingConfig: info.GetRoutingConfig(),
							Versions:      make(map[string]*deploymentspb.WorkerDeploymentVersionData),
						}
						deploymentData.DeploymentsData[version.GetDeploymentName()] = entry
					}
					// The leaf uses this internal view only to observe public membership. A
					// live public Version corresponds to revision zero and deleted=false,
					// which are the v1.31.0 defaults after poll-driven recreation.
					entry.Versions[version.GetBuildId()] = &deploymentspb.WorkerDeploymentVersionData{
						Status: versionInfo.GetWorkerDeploymentVersionInfo().GetStatus(),
					}
				}
			}
		}
		pageToken = page.GetNextPageToken()
		if len(pageToken) == 0 {
			break
		}
	}

	return &matchingservice.GetTaskQueueUserDataResponse{
		UserData: &persistencespb.VersionedTaskQueueUserData{
			Data: &persistencespb.TaskQueueUserData{
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
					int32(request.GetTaskQueueType()): {DeploymentData: deploymentData},
				},
			},
		},
	}, nil
}

// conformanceNexusEndpointState maps the corpus's internal Matching/Persistence endpoint views onto
// the real OperatorService registry. Endpoint records always come from tokeirad; only Temporal's
// matching-owner coordination metadata (table version and long-poll notification) lives in-process,
// because that metadata has no public RPC representation.
type conformanceNexusEndpointState struct {
	operator   operatorservice.OperatorServiceClient
	frontend   workflowservice.WorkflowServiceClient
	namespaces *conformanceNamespaceSet

	mu           sync.Mutex
	tableVersion int64
	changed      chan struct{}
}

func newConformanceNexusEndpointState(
	operator operatorservice.OperatorServiceClient,
	frontend workflowservice.WorkflowServiceClient,
	namespaces *conformanceNamespaceSet,
) *conformanceNexusEndpointState {
	return &conformanceNexusEndpointState{
		operator:   operator,
		frontend:   frontend,
		namespaces: namespaces,
		changed:    make(chan struct{}),
	}
}

func (s *conformanceNexusEndpointState) advanceTableVersion() {
	s.mu.Lock()
	s.tableVersion++
	changed := s.changed
	s.changed = make(chan struct{})
	s.mu.Unlock()
	close(changed)
}

func (s *conformanceNexusEndpointState) persistenceSpecToAPI(
	spec *persistencespb.NexusEndpointSpec,
) (*nexuspb.EndpointSpec, error) {
	if spec == nil {
		return nil, nil
	}

	var target *nexuspb.EndpointTarget
	switch variant := spec.GetTarget().GetVariant().(type) {
	case *persistencespb.NexusEndpointTarget_Worker_:
		namespaceName, ok := s.namespaces.nameForID(variant.Worker.GetNamespaceId())
		if !ok {
			return nil, fmt.Errorf(
				"tokeira conformance: namespace id %q is not registered in this cluster",
				variant.Worker.GetNamespaceId(),
			)
		}
		target = &nexuspb.EndpointTarget{
			Variant: &nexuspb.EndpointTarget_Worker_{
				Worker: &nexuspb.EndpointTarget_Worker{
					Namespace: namespaceName,
					TaskQueue: variant.Worker.GetTaskQueue(),
				},
			},
		}
	case *persistencespb.NexusEndpointTarget_External_:
		target = &nexuspb.EndpointTarget{
			Variant: &nexuspb.EndpointTarget_External_{
				External: &nexuspb.EndpointTarget_External{Url: variant.External.GetUrl()},
			},
		}
	}

	return &nexuspb.EndpointSpec{
		Name:        spec.GetName(),
		Description: spec.GetDescription(),
		Target:      target,
	}, nil
}

func (s *conformanceNexusEndpointState) apiEndpointToPersistence(
	ctx context.Context,
	endpoint *nexuspb.Endpoint,
) (*persistencespb.NexusEndpointEntry, error) {
	if endpoint == nil {
		return nil, errors.New("tokeira conformance: OperatorService returned an empty Nexus endpoint")
	}

	publicSpec := endpoint.GetSpec()
	var target *persistencespb.NexusEndpointTarget
	switch variant := publicSpec.GetTarget().GetVariant().(type) {
	case *nexuspb.EndpointTarget_Worker_:
		namespaceID, ok := s.namespaces.idForName(variant.Worker.GetNamespace())
		if !ok {
			response, err := s.frontend.DescribeNamespace(
				ctx,
				&workflowservice.DescribeNamespaceRequest{Namespace: variant.Worker.GetNamespace()},
			)
			if err != nil {
				return nil, err
			}
			namespaceID = response.GetNamespaceInfo().GetId()
			if namespaceID == "" {
				return nil, fmt.Errorf(
					"tokeira conformance: DescribeNamespace returned no id for %q",
					variant.Worker.GetNamespace(),
				)
			}
			s.namespaces.addID(namespaceID, variant.Worker.GetNamespace())
		}
		target = &persistencespb.NexusEndpointTarget{
			Variant: &persistencespb.NexusEndpointTarget_Worker_{
				Worker: &persistencespb.NexusEndpointTarget_Worker{
					NamespaceId: namespaceID,
					TaskQueue:   variant.Worker.GetTaskQueue(),
				},
			},
		}
	case *nexuspb.EndpointTarget_External_:
		target = &persistencespb.NexusEndpointTarget{
			Variant: &persistencespb.NexusEndpointTarget_External_{
				External: &persistencespb.NexusEndpointTarget_External{Url: variant.External.GetUrl()},
			},
		}
	}

	clock := &clockspb.HybridLogicalClock{}
	// v1.31.0 seeds creates with hlc.Zero and advances the clock on updates. The
	// public projection omits the cluster-id/version components, but its modification
	// timestamp preserves the observable wall-clock component needed by this corpus.
	if endpoint.GetVersion() > 1 && endpoint.GetLastModifiedTime() != nil {
		clock.WallClock = endpoint.GetLastModifiedTime().AsTime().UnixMilli()
	}

	return &persistencespb.NexusEndpointEntry{
		Version: endpoint.GetVersion(),
		Id:      endpoint.GetId(),
		Endpoint: &persistencespb.NexusEndpoint{
			Clock: clock,
			Spec: &persistencespb.NexusEndpointSpec{
				Name:        publicSpec.GetName(),
				Description: publicSpec.GetDescription(),
				Target:      target,
			},
			CreatedTime: endpoint.GetCreatedTime(),
		},
	}, nil
}

func (s *conformanceNexusEndpointState) listAll(
	ctx context.Context,
) ([]*persistencespb.NexusEndpointEntry, error) {
	var endpoints []*nexuspb.Endpoint
	var pageToken []byte
	for {
		response, err := s.operator.ListNexusEndpoints(
			ctx,
			&operatorservice.ListNexusEndpointsRequest{
				PageSize:      1000,
				NextPageToken: pageToken,
			},
		)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, response.GetEndpoints()...)
		if len(response.GetNextPageToken()) == 0 {
			break
		}
		if bytes.Equal(pageToken, response.GetNextPageToken()) {
			return nil, errors.New("tokeira conformance: Nexus endpoint pagination token did not advance")
		}
		pageToken = response.GetNextPageToken()
	}

	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].GetId() < endpoints[j].GetId() })
	entries := make([]*persistencespb.NexusEndpointEntry, len(endpoints))
	for index, endpoint := range endpoints {
		entry, err := s.apiEndpointToPersistence(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		entries[index] = entry
	}
	return entries, nil
}

func (s *conformanceNexusEndpointState) list(
	ctx context.Context,
	lastKnownTableVersion int64,
	nextPageToken []byte,
	pageSize int,
	wait bool,
) (int64, []byte, []*persistencespb.NexusEndpointEntry, error) {
	if pageSize < 0 {
		return 0, nil, nil, serviceerror.NewInvalidArgument("page_size is negative")
	}
	if wait && len(nextPageToken) != 0 {
		return 0, nil, nil, serviceerror.NewInvalidArgument(
			"request Wait=true and NextPageToken!=nil on ListNexusEndpoints request. waiting is only allowed on first page",
		)
	}

	if wait && lastKnownTableVersion != 0 {
		for {
			s.mu.Lock()
			currentVersion := s.tableVersion
			changed := s.changed
			s.mu.Unlock()
			if currentVersion != lastKnownTableVersion {
				break
			}
			select {
			case <-ctx.Done():
				return currentVersion, nil, nil, ctx.Err()
			case <-changed:
			}
		}
	}

	s.mu.Lock()
	currentVersion := s.tableVersion
	s.mu.Unlock()
	if !wait && lastKnownTableVersion != 0 && lastKnownTableVersion != currentVersion {
		return currentVersion, nil, nil, serviceerror.NewFailedPreconditionf(
			"nexus endpoints table version mismatch. received: %v expected %v",
			lastKnownTableVersion,
			currentVersion,
		)
	}

	entries, err := s.listAll(ctx)
	if err != nil {
		return currentVersion, nil, nil, err
	}
	start := 0
	if len(nextPageToken) != 0 {
		token := string(nextPageToken)
		start = sort.Search(len(entries), func(index int) bool { return entries[index].GetId() >= token })
		if start == len(entries) || entries[start].GetId() != token {
			return currentVersion, nil, nil, serviceerror.NewFailedPrecondition(
				"could not find endpoint indicated by nexus list endpoints next page token",
			)
		}
	}

	end := min(start+pageSize, len(entries))
	var followingToken []byte
	if end < len(entries) {
		followingToken = []byte(entries[end].GetId())
	}
	return currentVersion, followingToken, entries[start:end], nil
}

func (c *conformanceMatchingClient) CreateNexusEndpoint(
	ctx context.Context,
	request *matchingservice.CreateNexusEndpointRequest,
	_ ...grpc.CallOption,
) (*matchingservice.CreateNexusEndpointResponse, error) {
	spec, err := c.nexus.persistenceSpecToAPI(request.GetSpec())
	if err != nil {
		return nil, err
	}
	response, err := c.nexus.operator.CreateNexusEndpoint(
		ctx,
		&operatorservice.CreateNexusEndpointRequest{Spec: spec},
	)
	if err != nil {
		return nil, err
	}
	entry, err := c.nexus.apiEndpointToPersistence(ctx, response.GetEndpoint())
	if err != nil {
		return nil, err
	}
	c.nexus.advanceTableVersion()
	return &matchingservice.CreateNexusEndpointResponse{Entry: entry}, nil
}

func (c *conformanceMatchingClient) UpdateNexusEndpoint(
	ctx context.Context,
	request *matchingservice.UpdateNexusEndpointRequest,
	_ ...grpc.CallOption,
) (*matchingservice.UpdateNexusEndpointResponse, error) {
	spec, err := c.nexus.persistenceSpecToAPI(request.GetSpec())
	if err != nil {
		return nil, err
	}
	response, err := c.nexus.operator.UpdateNexusEndpoint(
		ctx,
		&operatorservice.UpdateNexusEndpointRequest{
			Id:      request.GetId(),
			Version: request.GetVersion(),
			Spec:    spec,
		},
	)
	if err != nil {
		return nil, err
	}
	entry, err := c.nexus.apiEndpointToPersistence(ctx, response.GetEndpoint())
	if err != nil {
		return nil, err
	}
	c.nexus.advanceTableVersion()
	return &matchingservice.UpdateNexusEndpointResponse{Entry: entry}, nil
}

func (c *conformanceMatchingClient) DeleteNexusEndpoint(
	ctx context.Context,
	request *matchingservice.DeleteNexusEndpointRequest,
	_ ...grpc.CallOption,
) (*matchingservice.DeleteNexusEndpointResponse, error) {
	entries, err := c.nexus.listAll(ctx)
	if err != nil {
		return nil, err
	}
	var version int64
	for _, entry := range entries {
		if entry.GetId() == request.GetId() {
			version = entry.GetVersion()
			break
		}
	}
	if version == 0 {
		return nil, serviceerror.NewNotFoundf(
			"error deleting nexus endpoint with ID: %v",
			request.GetId(),
		)
	}
	_, err = c.nexus.operator.DeleteNexusEndpoint(
		ctx,
		&operatorservice.DeleteNexusEndpointRequest{Id: request.GetId(), Version: version},
	)
	if err != nil {
		return nil, err
	}
	c.nexus.advanceTableVersion()
	return &matchingservice.DeleteNexusEndpointResponse{}, nil
}

func (c *conformanceMatchingClient) ListNexusEndpoints(
	ctx context.Context,
	request *matchingservice.ListNexusEndpointsRequest,
	_ ...grpc.CallOption,
) (*matchingservice.ListNexusEndpointsResponse, error) {
	tableVersion, nextPageToken, entries, err := c.nexus.list(
		ctx,
		request.GetLastKnownTableVersion(),
		request.GetNextPageToken(),
		int(request.GetPageSize()),
		request.GetWait(),
	)
	if err != nil {
		return nil, err
	}
	return &matchingservice.ListNexusEndpointsResponse{
		TableVersion:  tableVersion,
		NextPageToken: nextPageToken,
		Entries:       entries,
	}, nil
}

// conformanceNexusEndpointManager supplies the persistence-level list used only to
// compare ordering in the endpoint corpus. It shares the matching adapter's real public
// registry view and table-version fence; direct persistence mutations remain unsupported.
type conformanceNexusEndpointManager struct {
	state *conformanceNexusEndpointState
}

func (m *conformanceNexusEndpointManager) GetName() string { return "tokeira-conformance" }

func (m *conformanceNexusEndpointManager) Close() {}

func (m *conformanceNexusEndpointManager) GetNexusEndpoint(
	ctx context.Context,
	request *persistence.GetNexusEndpointRequest,
) (*persistencespb.NexusEndpointEntry, error) {
	response, err := m.state.operator.GetNexusEndpoint(
		ctx,
		&operatorservice.GetNexusEndpointRequest{Id: request.ID},
	)
	if err != nil {
		return nil, err
	}
	return m.state.apiEndpointToPersistence(ctx, response.GetEndpoint())
}

func (m *conformanceNexusEndpointManager) ListNexusEndpoints(
	ctx context.Context,
	request *persistence.ListNexusEndpointsRequest,
) (*persistence.ListNexusEndpointsResponse, error) {
	tableVersion, nextPageToken, entries, err := m.state.list(
		ctx,
		request.LastKnownTableVersion,
		request.NextPageToken,
		request.PageSize,
		false,
	)
	if err != nil {
		return nil, err
	}
	return &persistence.ListNexusEndpointsResponse{
		TableVersion:  tableVersion,
		NextPageToken: nextPageToken,
		Entries:       entries,
	}, nil
}

func (m *conformanceNexusEndpointManager) CreateOrUpdateNexusEndpoint(
	context.Context,
	*persistence.CreateOrUpdateNexusEndpointRequest,
) (*persistence.CreateOrUpdateNexusEndpointResponse, error) {
	return nil, errConformanceUnsupported
}

func (m *conformanceNexusEndpointManager) DeleteNexusEndpoint(
	context.Context,
	*persistence.DeleteNexusEndpointRequest,
) error {
	return errConformanceUnsupported
}

func (m *conformanceMetadataManager) GetNamespace(context.Context, *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
	return nil, errConformanceUnsupported
}

func (m *conformanceMetadataManager) UpdateNamespace(context.Context, *persistence.UpdateNamespaceRequest) error {
	return errConformanceUnsupported
}

func (m *conformanceMetadataManager) RenameNamespace(context.Context, *persistence.RenameNamespaceRequest) error {
	return errConformanceUnsupported
}

func (m *conformanceMetadataManager) DeleteNamespace(context.Context, *persistence.DeleteNamespaceRequest) error {
	return errConformanceUnsupported
}

func (m *conformanceMetadataManager) DeleteNamespaceByName(context.Context, *persistence.DeleteNamespaceByNameRequest) error {
	return errConformanceUnsupported
}

func (m *conformanceMetadataManager) ListNamespaces(context.Context, *persistence.ListNamespacesRequest) (*persistence.ListNamespacesResponse, error) {
	return nil, errConformanceUnsupported
}

func (m *conformanceMetadataManager) GetMetadata(context.Context) (*persistence.GetMetadataResponse, error) {
	return nil, errConformanceUnsupported
}

func (m *conformanceMetadataManager) InitializeSystemNamespaces(context.Context, string) error {
	return errConformanceUnsupported
}

func (m *conformanceMetadataManager) WatchNamespaces(context.Context) (<-chan *persistence.NamespaceWatchEvent, error) {
	return nil, errConformanceUnsupported
}
