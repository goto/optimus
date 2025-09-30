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

type RunScheduler interface {
	GetOperatorInstance(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, jobRunID, operatorName string) (*scheduler.OperatorRunInstance, error)
}

type SLAWorker struct {
	alertManager    AlertManager
	operatorSLARepo OperatorSLARepo
	jobRepo         JobRepo
	jobRunRepo      JobRunRepository
	operatorRunRepo OperatorRunRepository
	tenantService   TenantService
	scheduler       RunScheduler
	logger          log.Logger
}

func NewSLAWorker(logger log.Logger, alertManager AlertManager, operatorSLARepo OperatorSLARepo, jobRepo JobRepo,
	jobRunRepo JobRunRepository, operatorRunRepo OperatorRunRepository, tenantService TenantService, scheduler RunScheduler,
) *SLAWorker {
	return &SLAWorker{
		logger:          logger,
		alertManager:    alertManager,
		operatorSLARepo: operatorSLARepo,
		jobRepo:         jobRepo,
		jobRunRepo:      jobRunRepo,
		operatorRunRepo: operatorRunRepo,
		tenantService:   tenantService,
		scheduler:       scheduler,
	}
}

func (w *SLAWorker) ScheduleSLAHandling(ctx context.Context, interval, processDuration time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				signature := uuid.New().String()
				w.handleSLACalculation(ctx, signature, processDuration)
			}
		}
	}()
}

func getAlertAttributes(teanatWithDetails *tenant.WithDetails, team, severity, msg string, currentState scheduler.State, slaObj *scheduler.OperatorsSLA) *scheduler.OperatorSLAAlertAttrs {
	alertAttr := scheduler.OperatorSLAAlertAttrs{
		Team:               team,
		Project:            teanatWithDetails.Project().Name().String(),
		Namespace:          teanatWithDetails.Namespace().Name().String(),
		JobName:            slaObj.JobName.String(),
		OperatorType:       slaObj.OperatorType.String(),
		OperatorName:       slaObj.OperatorName,
		Message:            msg,
		Severity:           severity,
		ScheduledAt:        slaObj.ScheduledAt,
		StartTime:          slaObj.OperatorStartTime,
		ExpectedSLAEndTime: slaObj.SLATime,
		CurrentState:       currentState,

		AlertManager: getAlertManagerProjectConfig(teanatWithDetails),
	}
	return &alertAttr
}

func (w *SLAWorker) SendOperatorSLAEvent(ctx context.Context, job *scheduler.Job, slaObj *scheduler.OperatorsSLA) error {
	jobRun, err := w.jobRunRepo.GetByScheduledAt(ctx, job.Tenant, job.Name, slaObj.ScheduledAt)
	if err != nil {
		return err
	}

	tenantWithDetails, err := w.tenantService.GetDetails(ctx, job.Tenant)
	if err != nil {
		return err
	}

	operatorRun, err := w.operatorRunRepo.GetOperatorRun(ctx, slaObj.OperatorName, slaObj.OperatorType, jobRun.ID)
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
		if alertConfig.Team != "" {
			alertTeam = alertConfig.Team
		}
		slaAlertConfig := alertConfig.GetSLAOperatorAlertConfigByTag(slaObj.AlertTag)
		if slaAlertConfig != nil {
			alertSeverity = slaAlertConfig.Severity
			alertMsg = fmt.Sprintf("%s: %s, durationSLA: %s", slaObj.OperatorType.String(), slaObj.OperatorName, slaAlertConfig.DurationThreshold.String())
		}
	}

	alertAttr := getAlertAttributes(tenantWithDetails, alertTeam, alertSeverity.String(), alertMsg, operatorRun.Status, slaObj)
	w.alertManager.SendOperatorSLAEvent(alertAttr)
	return nil
}

func (w *SLAWorker) handleSLACalculation(ctx context.Context, signature string, processDuration time.Duration) {
	expiredSLAs, err := w.operatorSLARepo.GetExpiredSLAsForProcessing(ctx, signature, processDuration)
	if err != nil {
		w.logger.Error("failed to fetch expired SLAs: %v", err)
		return
	}

	for _, slaObj := range expiredSLAs {
		job, err := w.jobRepo.GetJob(ctx, slaObj.ProjectName, slaObj.JobName)
		if err != nil {
			w.logger.Error("failed to fetch job %s for SLA %s: %v", slaObj.JobName, slaObj.ID, err)
			continue
		}
		// check in airflow if the task is finished
		taskInstance, err := w.scheduler.GetOperatorInstance(ctx, job.Tenant, job.Name, slaObj.RunID, slaObj.OperatorName)
		if err != nil {
			w.logger.Error("failed to fetch operator instance from scheduler, proceeding to alert the user about SLA breach err: %v", err)
		}
		if taskInstance != nil {
			if taskInstance.IsTerminated() && taskInstance.EndTime != nil && taskInstance.EndTime.Before(slaObj.SLATime) {
				w.logger.Error("Operator Finished Before SLA time, removing it from SLA records table")
				if err := w.operatorSLARepo.RemoveProcessedSLA(ctx, slaObj.ID); err != nil {
					w.logger.Error("failed to update SLA processed for job %s: %v", slaObj.JobName, err)
				}
				continue
			}
		}
		if err := w.SendOperatorSLAEvent(ctx, job, slaObj); err != nil {
			w.logger.Error("failed to notify SLA for job %s: %v", slaObj.JobName, err)
			continue
		}
		if err := w.operatorSLARepo.RemoveProcessedSLA(ctx, slaObj.ID); err != nil {
			w.logger.Error("failed to update SLA processed for job %s: %v", slaObj.JobName, err)
		}
	}
}
