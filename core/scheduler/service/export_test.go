package service

import (
	"context"

	"github.com/goto/optimus/core/scheduler"
)

// SendRootCauseAlertsForTest exposes the unexported alert router to the external test
// package, so severity and suppression rules can be tested without the full engine run.
func (s *JobRootCauseService) SendRootCauseAlertsForTest(ctx context.Context, incidents []*scheduler.SLABreachIncidentWithImpacts, previousByKey map[scheduler.SLABreachIncidentKey]*scheduler.SLABreachIncident) {
	s.sendRootCauseAlerts(ctx, incidents, previousByKey)
}
