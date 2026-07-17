package testcore

// Tokeira Tier-2 conformance: namespace-scoped SetOnAuthorize callback bridge.
//
// Shape-2 runs the Temporal corpus in a different process from tokeirad, so the
// suite's exact Go authorization closure cannot be installed in the server.
// The runner reserves a loopback callback address and passes it to both
// processes. This file serves that address inside the corpus process and routes
// each resolved Nexus target to the TemporalImpl whose dedicated cluster
// registered that namespace. No callback or decision is persisted in tokeirad,
// and production builds contain no client for this bridge.

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/common/authorization"
)

const tokeiraAuthorizationCallbackURLEnv = "TOKEIRA_CONFORMANCE_AUTH_CALLBACK_URL"

type conformanceAuthorizeRequest struct {
	APIName           string  `json:"api_name"`
	Namespace         string  `json:"namespace"`
	NexusEndpointName *string `json:"nexus_endpoint_name"`
}

type conformanceAuthorizeResponse struct {
	Decision  string  `json:"decision"`
	Reason    *string `json:"reason,omitempty"`
	ErrorType *string `json:"error_type,omitempty"`
	Message   string  `json:"message,omitempty"`
}

var conformanceAuthorizationBridge = struct {
	sync.RWMutex
	hosts map[*TemporalImpl]*conformanceNamespaceSet
}{hosts: map[*TemporalImpl]*conformanceNamespaceSet{}}

var conformanceAuthorizationServerOnce sync.Once

func registerConformanceAuthorizationHost(host *TemporalImpl, namespaces *conformanceNamespaceSet) {
	conformanceAuthorizationBridge.Lock()
	conformanceAuthorizationBridge.hosts[host] = namespaces
	conformanceAuthorizationBridge.Unlock()
	startConformanceAuthorizationServer()
}

func unregisterConformanceAuthorizationHost(host *TemporalImpl) {
	conformanceAuthorizationBridge.Lock()
	delete(conformanceAuthorizationBridge.hosts, host)
	conformanceAuthorizationBridge.Unlock()
}

func startConformanceAuthorizationServer() {
	callbackURL := strings.TrimSpace(os.Getenv(tokeiraAuthorizationCallbackURLEnv))
	if callbackURL == "" {
		return
	}
	conformanceAuthorizationServerOnce.Do(func() {
		address := strings.TrimPrefix(callbackURL, "http://")
		address = strings.TrimSuffix(address, "/authorize")
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return
		}
		server := &http.Server{Handler: http.HandlerFunc(handleConformanceAuthorize)}
		go func() { _ = server.Serve(listener) }()
	})
}

func handleConformanceAuthorize(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != "/authorize" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	defer func() { _ = request.Body.Close() }()
	var input conformanceAuthorizeRequest
	if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	conformanceAuthorizationBridge.RLock()
	var host *TemporalImpl
	for candidate, namespaces := range conformanceAuthorizationBridge.hosts {
		if namespaces.containsExact(input.Namespace) && candidate.hasOnAuthorize() {
			host = candidate
			break
		}
	}
	conformanceAuthorizationBridge.RUnlock()
	if host == nil {
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	endpointName := ""
	if input.NexusEndpointName != nil {
		endpointName = *input.NexusEndpointName
	}
	result, err := host.Authorize(request.Context(), nil, &authorization.CallTarget{
		APIName:           input.APIName,
		Namespace:         input.Namespace,
		NexusEndpointName: endpointName,
	})
	response := conformanceAuthorizeResponse{Decision: "allow"}
	if err != nil {
		response.Decision = "authorizer_error"
		response.Message = err.Error()
		var handlerError *nexus.HandlerError
		if errors.As(err, &handlerError) {
			errorType := string(handlerError.Type)
			response.ErrorType = &errorType
			response.Message = handlerError.Message
		}
	} else if result.Decision == authorization.DecisionDeny {
		response.Decision = "deny"
		if result.Reason != "" {
			reason := result.Reason
			response.Reason = &reason
		}
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(response)
}

func (c *TemporalImpl) hasOnAuthorize() bool {
	c.callbackLock.RLock()
	defer c.callbackLock.RUnlock()
	return c.onAuthorize != nil
}
