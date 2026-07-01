package scheduler

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils/filter"
)

const (
	backfillColumnsToStore = `scheduler_run_id, project_name, namespace_name, job_name, description, category, approval_id, user_id, start_time, status, message, dstart, dend, custom_assets, job_config`
	backfillColumns        = `id, ` + backfillColumnsToStore + `, end_time, canceled_by, created_at, updated_at`
)

type backfillDB struct {
	ID             uuid.UUID
	SchedulerRunID string
	ProjectName    string
	NamespaceName  string
	JobName        string
	Description    string
	Category       string
	ApprovalID     string
	UserID         string
	StartTime      time.Time
	Status         string
	Message        string
	Dstart         time.Time
	Dend           time.Time
	CustomAssets   map[string]string
	JobConfig      map[string]string
	EndTime        *time.Time
	CanceledBy     *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func fromBackfillDB(b *backfillDB) (*scheduler.Backfill, error) {
	t, err := tenant.NewTenant(b.ProjectName, b.NamespaceName)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityBackfill, "unable to build tenant from backfill row", err)
	}

	jobName, err := scheduler.JobNameFrom(b.JobName)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityBackfill, "unable to build job name from backfill row", err)
	}

	config := scheduler.NewBackfillConfig(b.Dstart, b.Dend, b.JobConfig, b.CustomAssets, b.Description, b.Category, b.ApprovalID, b.UserID)

	backfill := scheduler.NewBackfill(b.ID, jobName, t, config, scheduler.BackfillState(b.Status), b.CreatedAt, b.UpdatedAt, b.Message)
	backfill.SchedulerRunID = b.SchedulerRunID
	if b.CanceledBy != nil {
		backfill.CanceledBy = *b.CanceledBy
	}
	return backfill, nil
}

type BackfillRepository struct {
	db *pgxpool.Pool
}

func (r BackfillRepository) RegisterBackfill(ctx context.Context, backfill *scheduler.Backfill) (uuid.UUID, error) {
	insertBackfill := `INSERT INTO backfill (` + backfillColumnsToStore + `, created_at, updated_at) ` +
		`VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9, $10, $11, $12, $13, $14, NOW(), NOW()) RETURNING id`

	var id uuid.UUID
	err := r.db.QueryRow(ctx, insertBackfill,
		"",
		backfill.Tenant().ProjectName(),
		backfill.Tenant().NamespaceName(),
		backfill.JobName().String(),
		backfill.Config().Description,
		backfill.Config().Category,
		backfill.Config().ApprovalID,
		backfill.Config().UserID,
		backfill.State(),
		backfill.Message(),
		backfill.Config().Dstart,
		backfill.Config().Dend,
		backfill.Config().Assets,
		backfill.Config().JobConfig,
	).Scan(&id)
	if err != nil {
		return uuid.Nil, errors.Wrap(scheduler.EntityBackfill, "unable to store backfill", err)
	}
	return id, nil
}

func (r BackfillRepository) GetBackfillDetails(ctx context.Context, backfillID uuid.UUID) (*scheduler.Backfill, error) {
	selectBackfill := `SELECT ` + backfillColumns + ` FROM backfill WHERE id = $1`

	var row backfillDB
	err := r.db.QueryRow(ctx, selectBackfill, backfillID).Scan(
		&row.ID,
		&row.SchedulerRunID,
		&row.ProjectName,
		&row.NamespaceName,
		&row.JobName,
		&row.Description,
		&row.Category,
		&row.ApprovalID,
		&row.UserID,
		&row.StartTime,
		&row.Status,
		&row.Message,
		&row.Dstart,
		&row.Dend,
		&row.CustomAssets,
		&row.JobConfig,
		&row.EndTime,
		&row.CanceledBy,
		&row.CreatedAt,
		&row.UpdatedAt,
	)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityBackfill, "unable to get backfill details", err)
	}

	return fromBackfillDB(&row)
}

func NewBackfillRepository(db *pgxpool.Pool) *BackfillRepository {
	return &BackfillRepository{db: db}
}

func (r BackfillRepository) UpdateBackfillStatus(ctx context.Context, backfillID uuid.UUID, state scheduler.BackfillState, message string) error {
	query := `UPDATE backfill SET status = $1, message = $2, updated_at = NOW() WHERE id = $3`
	if _, err := r.db.Exec(ctx, query, state, message, backfillID); err != nil {
		return errors.Wrap(scheduler.EntityBackfill, "unable to update backfill status", err)
	}
	return nil
}

func (r BackfillRepository) UpdateBackfillHeartbeat(ctx context.Context, backfillID uuid.UUID) error {
	query := `UPDATE backfill SET updated_at = NOW() WHERE id = $1`
	if _, err := r.db.Exec(ctx, query, backfillID); err != nil {
		return errors.Wrap(scheduler.EntityBackfill, "unable to update backfill heartbeat", err)
	}
	return nil
}

func (r BackfillRepository) UpdateBackfillSchedulerRunID(ctx context.Context, backfillID uuid.UUID, schedulerRunID string) error {
	query := `UPDATE backfill SET scheduler_run_id = $1, updated_at = NOW() WHERE id = $2`
	if _, err := r.db.Exec(ctx, query, schedulerRunID, backfillID); err != nil {
		return errors.Wrap(scheduler.EntityBackfill, "unable to update backfill scheduler run ID", err)
	}
	return nil
}

func (r BackfillRepository) ScanAbandonedBackfillRequests(ctx context.Context, unhandledClassifierDuration time.Duration) ([]*scheduler.Backfill, error) {
	nonTerminalStates := make([]string, len(scheduler.BackfillNonTerminalStates))
	for i, state := range scheduler.BackfillNonTerminalStates {
		nonTerminalStates[i] = state.String()
	}
	query := fmt.Sprintf(
		`SELECT %s FROM backfill WHERE status IN ('%s') AND (NOW() - updated_at) > INTERVAL '%d Seconds'`,
		backfillColumns,
		strings.Join(nonTerminalStates, "', '"),
		int64(unhandledClassifierDuration.Seconds()),
	)
	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityBackfill, "unable to scan abandoned backfill requests", err)
	}
	defer rows.Close()

	var backfills []*scheduler.Backfill
	for rows.Next() {
		var row backfillDB
		if err := rows.Scan(
			&row.ID, &row.SchedulerRunID, &row.ProjectName, &row.NamespaceName, &row.JobName,
			&row.Description, &row.Category, &row.ApprovalID, &row.UserID, &row.StartTime,
			&row.Status, &row.Message, &row.Dstart, &row.Dend, &row.CustomAssets, &row.JobConfig,
			&row.EndTime, &row.CanceledBy, &row.CreatedAt, &row.UpdatedAt,
		); err != nil {
			return nil, errors.Wrap(scheduler.EntityBackfill, "unable to scan backfill row", err)
		}
		backfill, err := fromBackfillDB(&row)
		if err != nil {
			return nil, err
		}
		backfills = append(backfills, backfill)
	}
	return backfills, nil
}

func (r BackfillRepository) AcquireBackfillRequest(ctx context.Context, backfillID uuid.UUID, unhandledClassifierDuration time.Duration) error {
	query := fmt.Sprintf(
		`UPDATE backfill SET updated_at = NOW() WHERE id = $1 AND (NOW() - updated_at) > INTERVAL '%d Seconds'`,
		int64(unhandledClassifierDuration.Seconds()),
	)
	tag, err := r.db.Exec(ctx, query, backfillID)
	if err != nil {
		return errors.Wrap(scheduler.EntityBackfill, "error acquiring backfill request", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.NotFound(scheduler.EntityBackfill, "unable to acquire backfill request, perhaps acquired by another server")
	}
	return nil
}

func (r BackfillRepository) GetBackfillsByFilter(ctx context.Context, projectName tenant.ProjectName, filters ...filter.FilterOpt) ([]*scheduler.Backfill, error) {
	f := filter.NewFilter(filters...)

	var fragments []string
	fragments = append(fragments, fmt.Sprintf("project_name = '%s'", projectName))

	if f.Contains(filter.BackfillID) {
		fragments = append(fragments, fmt.Sprintf("id = '%s'", f.GetStringValue(filter.BackfillID)))
	}
	if f.Contains(filter.SchedulerRunID) {
		fragments = append(fragments, fmt.Sprintf("scheduler_run_id = '%s'", f.GetStringValue(filter.SchedulerRunID)))
	}
	if f.Contains(filter.JobNames) {
		jobNames := f.GetStringArrayValue(filter.JobNames)
		quoted := make([]string, len(jobNames))
		for i, n := range jobNames {
			quoted[i] = fmt.Sprintf("'%s'", n)
		}
		fragments = append(fragments, fmt.Sprintf("job_name IN (%s)", strings.Join(quoted, ", ")))
	}
	if f.Contains(filter.BackfillStatus) {
		fragments = append(fragments, fmt.Sprintf("status = '%s'", f.GetStringValue(filter.BackfillStatus)))
	}
	if f.Contains(filter.ApprovalID) {
		approvalID := f.GetStringValue(filter.ApprovalID)
		approvalIDs := strings.Split(approvalID, ",")
		quoted := make([]string, len(approvalIDs))
		for i, id := range approvalIDs {
			quoted[i] = fmt.Sprintf("'%s'", strings.TrimSpace(id))
		}
		fragments = append(fragments, fmt.Sprintf("approval_id IN (%s)", strings.Join(quoted, ", ")))
	}
	if f.Contains(filter.UserID) {
		fragments = append(fragments, fmt.Sprintf("user_id = '%s'", f.GetStringValue(filter.UserID)))
	}

	query := fmt.Sprintf(`SELECT %s FROM backfill WHERE %s ORDER BY created_at DESC`, backfillColumns, strings.Join(fragments, " AND "))

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityBackfill, "unable to get backfills by filter", err)
	}
	defer rows.Close()

	var backfills []*scheduler.Backfill
	for rows.Next() {
		var row backfillDB
		if err := rows.Scan(
			&row.ID, &row.SchedulerRunID, &row.ProjectName, &row.NamespaceName, &row.JobName,
			&row.Description, &row.Category, &row.ApprovalID, &row.UserID, &row.StartTime,
			&row.Status, &row.Message, &row.Dstart, &row.Dend, &row.CustomAssets, &row.JobConfig,
			&row.EndTime, &row.CanceledBy, &row.CreatedAt, &row.UpdatedAt,
		); err != nil {
			return nil, errors.Wrap(scheduler.EntityBackfill, "unable to scan backfill row", err)
		}
		backfill, err := fromBackfillDB(&row)
		if err != nil {
			return nil, err
		}
		backfills = append(backfills, backfill)
	}
	return backfills, nil
}

func (r BackfillRepository) CancelBackfill(ctx context.Context, backfillID uuid.UUID, canceledBy string) error {
	query := `UPDATE backfill SET status = $1, canceled_by = $2, updated_at = NOW() WHERE id = $3`
	if _, err := r.db.Exec(ctx, query, scheduler.BackfillStateCancelled, canceledBy, backfillID); err != nil {
		return errors.Wrap(scheduler.EntityBackfill, "unable to cancel backfill", err)
	}
	return nil
}
