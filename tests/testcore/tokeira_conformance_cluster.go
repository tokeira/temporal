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
	"context"
	"errors"
	"fmt"
	"testing"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/grpcinject"
	"go.temporal.io/server/common/testing/testhooks"
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
	testBase := &persistencetests.TestBase{
		MetadataManager: &conformanceMetadataManager{frontend: frontendClient},
		ClusterMetadata: clusterMetadata,
	}

	host := &TemporalImpl{
		logger:                logger,
		frontendClient:        frontendClient,
		operatorClient:        operatorservice.NewOperatorServiceClient(conn),
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

	return &TestCluster{testBase: testBase, host: host}, nil
}

// conformanceMetadataManager forwards namespace registration to `tokeirad`'s frontend so
// FunctionalTestBase.RegisterNamespace runs unmodified over the wire. Only CreateNamespace
// is implemented; the rest of the MetadataManager surface returns errConformanceUnsupported.
type conformanceMetadataManager struct {
	frontend workflowservice.WorkflowServiceClient
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
	return &persistence.CreateNamespaceResponse{ID: info.GetId()}, nil
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
