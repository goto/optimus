package alerts

import (
	"context"
	"fmt"

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

// Insert logs an alert payload into the database
func (r *AlertRepository) Insert(ctx context.Context, alertPayload *alertmanager.AlertPayload) (uuid.UUID, error) {
	// default record Id in case inserting to DB fails
	recordID := uuid.New()

	alertLog, err := toDBSpec(alertPayload)
	if err != nil {
		return recordID, err
	}

	query := `
		INSERT INTO alert_logs (project_name, data, template, labels, endpoint)
		VALUES ($1, $2, $3, $4, $5) RETURNING id;
	`

	err = r.db.QueryRow(ctx, query, alertLog.Project, alertLog.Data, alertLog.Template, alertLog.Labels, alertLog.Endpoint).Scan(&recordID)
	if err != nil {
		return recordID, err
	}

	return recordID, nil
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
