package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tests/testcore"
)

func TestTimeoutForEntrypoint(t *testing.T) {
	if got := timeoutForEntrypoint("TestVersioning3FunctionalSuite"); got != 30*time.Minute {
		t.Fatalf("Versioning3 timeout = %s, want 30m", got)
	}
	if got := timeoutForEntrypoint("TestTaskQueueSuite"); got != 5*time.Minute {
		t.Fatalf("ordinary entrypoint timeout = %s, want 5m", got)
	}
}

func TestRecordExclusionsPreservesIdentityAndReason(t *testing.T) {
	var output bytes.Buffer
	require.NoError(t, recordExclusions(&output, "TestUpdateWithStartSuite"))
	want := testcore.ConformanceExclusions("TestUpdateWithStartSuite")
	decoder := json.NewDecoder(&output)
	got := make(map[string]string)
	for decoder.More() {
		var event map[string]string
		require.NoError(t, decoder.Decode(&event))
		require.Equal(t, "skip", event["Action"])
		require.Equal(t, "tokeira-skip-registry", event["Source"])
		require.Equal(t, "go.temporal.io/server/tests", event["Package"])
		got[event["Test"]] = event["Reason"]
	}
	require.Equal(t, want, got)
	require.NotContains(t, got, "TestUpdateWithStartSuite/TestReturnUpdateRateLimitError")
}
