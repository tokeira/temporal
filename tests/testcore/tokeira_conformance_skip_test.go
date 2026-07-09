package testcore

import (
	"regexp"
	"strings"
	"testing"
)

// matchesSkip mirrors `go test`'s positional skip matching: split the test
// identifier and the skip pattern on unbracketed '/', and require each pattern
// element to match the corresponding identifier element (extra pattern elements
// past the identifier's depth are ignored). It lets the test assert which leaves
// the generated regexp would skip without spawning a real `go test`.
func matchesSkip(t *testing.T, pattern, name string) bool {
	t.Helper()
	if pattern == "" {
		return false
	}
	pParts := strings.Split(pattern, "/")
	nParts := strings.Split(name, "/")
	for i, n := range nParts {
		if i >= len(pParts) {
			break
		}
		re := regexp.MustCompile(pParts[i])
		if !re.MatchString(n) {
			return false
		}
	}
	return true
}

func TestConformanceSkipRegexp_StandaloneActivity(t *testing.T) {
	const suite = "TestStandaloneActivityTestSuite"
	pattern := ConformanceSkipRegexp(suite)
	if pattern == "" {
		t.Fatalf("expected a skip regexp for %s", suite)
	}

	skipped := []string{
		suite + "/TestStart/RequestValidations/InputTooLarge",
		suite + "/TestRequestCancel/RequestValidations/ReasonTooLong",
		suite + "/TestTerminate/RequestValidations/ReasonTooLong",
	}
	for _, name := range skipped {
		if !matchesSkip(t, pattern, name) {
			t.Errorf("expected %q to be skipped by %q", name, pattern)
		}
	}

	// Sibling validations and happy paths must keep running.
	kept := []string{
		suite + "/TestStart/RequestValidations/RequestIDTooLong",
		suite + "/TestStart/RequestValidations/IdentityTooLong",
		suite + "/TestStart/RequestValidations/SearchAttributesInvalid",
		suite + "/TestRequestCancel/RequestValidations/EmptyActivityID",
		suite + "/TestTerminate/RequestValidations/ActivityIDTooLong",
		suite + "/TestComplete/ByToken",
	}
	for _, name := range kept {
		if matchesSkip(t, pattern, name) {
			t.Errorf("did not expect %q to be skipped by %q", name, pattern)
		}
	}
}

func TestConformanceSkipRegexp_EagerWorkflow(t *testing.T) {
	const suite = "TestEagerWorkflowTestSuite"
	pattern := ConformanceSkipRegexp(suite)
	if pattern == "" {
		t.Fatalf("expected a skip regexp for %s", suite)
	}

	if name := suite + "/TestEagerWorkflowStart_TerminateDuplicate"; !matchesSkip(t, pattern, name) {
		t.Errorf("expected %q to be skipped by %q", name, pattern)
	}

	for _, name := range []string{
		suite + "/TestEagerWorkflowStart_StartNew",
		suite + "/TestEagerWorkflowStart_RetryTaskAfterTimeout",
		suite + "/TestEagerWorkflowStart_RetryStartAfterTimeout",
		suite + "/TestEagerWorkflowStart_RetryStartImmediately",
		suite + "/TestEagerWorkflowStart_WorkflowRetry",
	} {
		if matchesSkip(t, pattern, name) {
			t.Errorf("did not expect %q to be skipped by %q", name, pattern)
		}
	}
}

func TestConformanceSkipRegexp_UnknownEntrypointIsEmpty(t *testing.T) {
	if got := ConformanceSkipRegexp("TestNoSuchSuite"); got != "" {
		t.Errorf("expected empty skip regexp for an unregistered entrypoint, got %q", got)
	}
}
