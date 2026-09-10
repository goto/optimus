package notify

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"

	"github.com/goto/optimus/ext/notify/alertmanager"
)

type AlertLogRepository interface {
	InsertWithStatus(ctx context.Context, payload *alertmanager.AlertPayload, status alertmanager.AlertStatus) (uuid.UUID, error)
	UpdateStatus(ctx context.Context, recordID uuid.UUID, status alertmanager.AlertStatus, message string) error
	HasRecentAlert(ctx context.Context, alertType string, dedupValues map[string]string, since time.Time) (bool, error)
}

type AlertDeduplicationConfig struct {
	DedupKeys             []string
	WindowMinutes         int
	ActiveWindowTimezone  *time.Location
	ActiveWindowStartHour int
	ActiveWindowEndHour   int
}

func (c AlertDeduplicationConfig) IsWithinActiveWindow(t time.Time) bool {
	hour := t.In(c.ActiveWindowTimezone).Hour()
	return hour >= c.ActiveWindowStartHour && hour < c.ActiveWindowEndHour
}

type AlertLogProvider struct {
	repo                      AlertLogRepository
	dedupConfigByAlertTypeMap map[string]AlertDeduplicationConfig
	logger                    log.Logger
}

func NewAlertLogProvider(repo AlertLogRepository, dedupConfigByAlertTypeMap map[string]AlertDeduplicationConfig, logger log.Logger) *AlertLogProvider {
	return &AlertLogProvider{
		repo:                      repo,
		dedupConfigByAlertTypeMap: dedupConfigByAlertTypeMap,
		logger:                    logger,
	}
}

func (a *AlertLogProvider) Insert(ctx context.Context, alert *alertmanager.AlertPayload) (uuid.UUID, bool, error) {
	// do not alert
	status := alertmanager.StatusPending
	if a.shouldDeduplicate(ctx, alert) {
		status = alertmanager.StatusDeduplicated
	}

	id, err := a.repo.InsertWithStatus(ctx, alert, status)
	if err != nil {
		return uuid.Nil, false, err
	}

	return id, status == alertmanager.StatusDeduplicated, nil
}

func (a *AlertLogProvider) shouldDeduplicate(ctx context.Context, alert *alertmanager.AlertPayload) bool {
	// get the config first by alert type
	dedupConfig, found := a.dedupConfigByAlertTypeMap[alert.AlertType]
	if !found {
		return false
	}

	alertAt := time.Now()
	if !dedupConfig.IsWithinActiveWindow(alertAt) {
		return false
	}

	// on empty dedup keys, below function will return empty and we return false
	dedupValues := getDedupValues(alert, dedupConfig.DedupKeys)
	if len(dedupValues) == 0 {
		return false
	}

	since := alertAt.Add(-time.Duration(dedupConfig.WindowMinutes) * time.Minute)
	exists, err := a.repo.HasRecentAlert(ctx, alert.AlertType, dedupValues, since)
	if err != nil {
		a.logger.Error("alert-manager: dedup check failed, sending alert anyway", "alert", alert.AlertType, "error", err)
		return false
	}

	return exists
}

func getDedupValues(alert *alertmanager.AlertPayload, dedupKeys []string) map[string]string {
	dedupValues := make(map[string]string, len(dedupKeys))
	for _, dedupKey := range dedupKeys {
		components := strings.Split(dedupKey, ".")
		if len(components) != 2 {
			continue
		}

		// currently only supports 1-level keyed fields (data.value, label.value)
		attribute := components[0]
		key := components[1]
		value := ""
		switch attribute {
		case "labels":
			value = alert.Labels[key]
		case "data":
			if valueRaw, ok := alert.Data[key]; ok {
				if valueStr, ok := valueRaw.(string); ok {
					value = valueStr
				}
			}
		}

		// every dedup key must have the values in the payload, else, it is considered
		// as invalid and dedup is cancelled
		if value == "" {
			return map[string]string{}
		}

		dedupValues[dedupKey] = value
	}

	return dedupValues
}

func (a *AlertLogProvider) UpdateStatus(ctx context.Context, logID uuid.UUID, status alertmanager.AlertStatus, message string) error {
	return a.repo.UpdateStatus(ctx, logID, status, message)
}
