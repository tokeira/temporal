package runner //nolint:revive // Matches the shared CLI helper package.

import "testing"

func TestSignalCleanupJoinsHandlerWithoutStoppingProcess(t *testing.T) {
	// Joining the listener lets repeated entrypoints release signal registrations
	// without waiting for a signal or leaving callbacks holding old process handles.
	for range 10 {
		cleanup := InstallSignalCleanup(&Process{})
		cleanup()
	}
}
