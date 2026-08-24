package main

import (
	"testing"
	"time"
)

func TestTimeoutForEntrypoint(t *testing.T) {
	if got := timeoutForEntrypoint("TestVersioning3FunctionalSuite"); got != 30*time.Minute {
		t.Fatalf("Versioning3 timeout = %s, want 30m", got)
	}
	if got := timeoutForEntrypoint("TestTaskQueueSuite"); got != 5*time.Minute {
		t.Fatalf("ordinary entrypoint timeout = %s, want 5m", got)
	}
}
