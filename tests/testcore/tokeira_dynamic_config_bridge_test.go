package testcore

import (
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
)

func TestEncodeDynamicConfigOverrideValue_UnwrapsSingleNamespaceConstraint(t *testing.T) {
	encoded, ok := encodeDynamicConfigOverrideValue([]dynamicconfig.ConstrainedValue{{
		Constraints: dynamicconfig.Constraints{Namespace: "test-namespace"},
		Value:       1.0,
	}})
	if !ok {
		t.Fatal("expected a single constrained value to be encodable")
	}
	if encoded != `{"doubleValue":1}` {
		t.Fatalf("unexpected encoding: %s", encoded)
	}
}

func TestEncodeDynamicConfigOverrideValue_RejectsMultipleConstraints(t *testing.T) {
	_, ok := encodeDynamicConfigOverrideValue([]dynamicconfig.ConstrainedValue{
		{Value: 1.0},
		{Value: 2.0},
	})
	if ok {
		t.Fatal("expected multiple constrained values to remain unsupported")
	}
}
