//go:build unit_test

package alertmanager // nolint: testpackage

import "context"

// ForTest_WorkerStep executes one step of the worker's per-event logic
// synchronously (bypasses the internal channel and goroutine). It is only
// available when the unit_test build tag is active.
func (a *AlertManager) ForTest_WorkerStep(ctx context.Context, alert *AlertPayload) {
	logID, isDeduplicated, err := a.logEvent(ctx, alert)
	if err != nil {
		a.logger.Error("failed to log event", "error", err)
	}

	if isDeduplicated {
		return
	}

	err = a.PrepareAndSendEvent(alert) // nolint:contextcheck
	if err != nil {
		a.logger.Error("alert worker send error", "error", err)
	}

	if alert.HasDefaultChannelLabel() {
		if err != nil {
			a.alertLogProvider.UpdateStatus(ctx, logID, StatusFailed, err.Error())
		} else {
			a.alertLogProvider.UpdateStatus(ctx, logID, StatusSent, "")
		}
	}
}
