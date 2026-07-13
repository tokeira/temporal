package testcore

import (
	"encoding/json"
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

func TestEncodeDynamicConfigOverrideValue_SerializesStructuredValueAsJSON(t *testing.T) {
	value := []any{
		map[string]any{"Pattern": "*", "AllowInsecure": true},
		map[string]any{"Pattern": "secure.example", "AllowInsecure": false},
	}
	encoded, ok := encodeDynamicConfigOverrideValue(value)
	if !ok {
		t.Fatal("expected structured value to be encodable")
	}
	var envelope struct {
		JSONValue string `json:"jsonValue"`
	}
	if err := json.Unmarshal([]byte(encoded), &envelope); err != nil {
		t.Fatalf("invalid oneof JSON: %v", err)
	}
	var roundTripped []map[string]any
	if err := json.Unmarshal([]byte(envelope.JSONValue), &roundTripped); err != nil {
		t.Fatalf("invalid structured JSON value: %v", err)
	}
	if len(roundTripped) != 2 || roundTripped[0]["Pattern"] != "*" {
		t.Fatalf("unexpected structured round trip: %#v", roundTripped)
	}
}

func TestEncodeDynamicConfigOverrideValue_RejectsUnserializableComposite(t *testing.T) {
	_, ok := encodeDynamicConfigOverrideValue(make(chan struct{}))
	if ok {
		t.Fatal("expected channel value to remain unsupported")
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
