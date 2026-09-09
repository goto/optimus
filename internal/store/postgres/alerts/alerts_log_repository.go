package alerts

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/ext/notify/alertmanager"
	"github.com/goto/optimus/internal/errors"
)

// AlertRepository provides methods to interact with the alerts table
type AlertRepository struct {
	db *pgxpool.Pool
}

// NewAlertRepository creates a new instance of AlertRepository
func NewAlertRepository(db *pgxpool.Pool) *AlertRepository {
	return &AlertRepository{db: db}
}

// InsertWithStatus logs an alert payload with an explicit initial status
func (r *AlertRepository) InsertWithStatus(ctx context.Context, alertPayload *alertmanager.AlertPayload, status alertmanager.AlertStatus) (uuid.UUID, error) {
	recordID := uuid.New()

	alertLog, err := toDBSpec(alertPayload)
	if err != nil {
		return recordID, err
	}

	query := `
		INSERT INTO alert_logs (project_name, data, template_name, labels, endpoint, status)
		VALUES ($1, $2, $3, $4, $5, $6) RETURNING id;
	`

	err = r.db.QueryRow(ctx, query,
		alertLog.Project, alertLog.Data, alertLog.Template,
		alertLog.Labels, alertLog.Endpoint, status,
	).Scan(&recordID)
	if err != nil {
		return recordID, err
	}

	return recordID, nil
}

// HasRecentAlert returns true when a SENT alert for the given (alertType, dedupValues) exists
// in alert_logs with created_at >= since.
func (r *AlertRepository) HasRecentAlert(ctx context.Context, alertType string, dedupValues map[string]string, since time.Time) (bool, error) {
	if len(dedupValues) == 0 {
		return false, nil
	}

	args := []interface{}{alertType, since}
	argIdx := 3

	conditions := []string{
		"alert_type = $1",
		"created_at >= $2",
		"status = 'SENT'",
	}

	for dedupKey, dedupValue := range dedupValues {
		parts := strings.SplitN(dedupKey, ".", 2)
		if len(parts) != 2 {
			continue
		}
		column := parts[0]
		if column != "data" && column != "labels" {
			continue
		}
		fieldKey := parts[1]
		// parameterise both the field key and value to avoid SQL injection
		conditions = append(conditions, fmt.Sprintf("%s->>$%d = $%d", column, argIdx, argIdx+1))
		args = append(args, fieldKey, dedupValue)
		argIdx += 2
	}

	query := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM alert_logs WHERE %s)`, strings.Join(conditions, " AND "))

	var exists bool
	if err := r.db.QueryRow(ctx, query, args...).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// UpdateStatus updates the alert status and optionally logs an error message
func (r *AlertRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status alertmanager.AlertStatus, message string) error {
	query := `
		UPDATE alert_logs
		SET status = $1, message = $2
		WHERE id = $3;
	`
	tag, err := r.db.Exec(ctx, query, status, message, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errors.NotFound("AlertLogger", fmt.Sprintf("could not update status of alert log, id: %s", id))
	}
	return err
}
