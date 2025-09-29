package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type JobRepo interface {
	GetJob(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error)
}

type OperatorSLARepo interface {
	GetExpiredSLAsForProcessing(ctx context.Context, signature string, processingDuration time.Duration) ([]*scheduler.OperatorsSLA, error)
	RemoveProcessedSLA(ctx context.Context, slaID uuid.UUID) error
}

type SLAWorker struct {
	operatorSLARepo OperatorSLARepo
	jobRepo         JobRepo
	logger          log.Logger
	alertManager    AlertManager
}

func NewSLAWorker(logger log.Logger, alertManager AlertManager, operatorSLARepo OperatorSLARepo) *SLAWorker {
	return &SLAWorker{
		logger:          logger,
		alertManager:    alertManager,
		operatorSLARepo: operatorSLARepo,
	}
}

func (w *SLAWorker) ScheduleSLAHandling(ctx context.Context, interval time.Duration, processDuration time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			signature := uuid.New().String()
			w.handleSLACalculation(ctx, signature, processDuration)
		}
	}
}

func getAlertAttributes(team, severity, msg string, slaObj *scheduler.OperatorsSLA) *scheduler.OperatorSLAAlertAttrs {

	alertAttr := scheduler.OperatorSLAAlertAttrs{
		Team:               team,
		JobName:            slaObj.JobName.String(),
		OperatorType:       slaObj.OperatorType.String(),
		OperatorName:       slaObj.OperatorName,
		Message:            msg,
		Severity:           severity,
		ScheduledAt:        time.Time{},
		StartTime:          time.Time{},
		ExpectedSLAEndTime: slaObj.SLATime,
		CurrentState:       "",
	}
	return &alertAttr
}

func (w *SLAWorker) SendOperatorSLAEvent(ctx context.Context, slaObj *scheduler.OperatorsSLA) error {
	job, err := w.jobRepo.GetJob(ctx, slaObj.ProjectName, slaObj.JobName)
	if err != nil {
		return err
	}
	alertConfig := job.GetOperatorAlertConfigByName(slaObj.OperatorType, slaObj.OperatorName)
	if alertConfig == nil {
		w.logger.Warn("no alert config found for job %s operator %s", job.Name, slaObj.OperatorName)
		return nil
	}

	var alertTeam string
	var alertSeverity scheduler.Severity
	var alertMsg string
	if alertConfig != nil {
		alertTeam = job.Tenant.NamespaceName().String()
		alertSeverity = scheduler.Warning
		if alertConfig.Team != nil && alertConfig.Team != "" {
			alertTeam = alertConfig.Team
		}
		slaAlertConfig := alertConfig.GetSLAOperatorAlertConfigByTag(slaObj.AlertTag)
		if slaAlertConfig != nil {
			alertSeverity = slaAlertConfig.Severity
			alertMsg = fmt.Sprintf("%s: %s, durationSLA: %s", slaObj.OperatorType.String(), slaObj.OperatorName, slaAlertConfig.DurationThreshold.String())
		}
	}

	alertAttr := getAlertAttributes(alertTeam, alertSeverity.String(), alertMsg, slaObj)

}

func (w *SLAWorker) handleSLACalculation(ctx context.Context, signature string, processDuration time.Duration) {
	expiredSLAs, err := w.operatorSLARepo.GetExpiredSLAsForProcessing(ctx, signature, processDuration)
	if err != nil {
		w.logger.Error("failed to fetch expired SLAs: %v", err)
		return
	}
	// todo: think of dooing this paralel
	for _, sla := range expiredSLAs {
		// check if airflow the task is finished
		if err := w.alertManager.SendOperatorSLAEvent(ctx, sla); err != nil {
			w.logger.Error("failed to notify SLA for job %s: %v", sla.JobName, err)
			continue
		}
		if err := w.operatorSLARepo.RemoveProcessedSLA(ctx, sla.ID); err != nil {
			w.logger.Error("failed to update SLA processed for job %s: %v", sla.JobName, err)
		}
	}
}
