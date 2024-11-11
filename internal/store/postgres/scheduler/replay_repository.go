package scheduler

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils/filter"
)

const (
	replayColumnsToStore = `job_name, namespace_name, project_name, start_time, end_time, description, parallel, job_config, status, message`
	replayColumns        = `id, ` + replayColumnsToStore + `, created_at, updated_at`

	replayRunColumns    = `replay_id, scheduled_at, status`
	updateReplayRequest = `UPDATE replay_request SET status = $1, message = $2, updated_at = NOW() WHERE id = $3 and status <> 'cancelled'`
)

type ReplayRepository struct {
	db *pgxpool.Pool
}

type replayRequest struct {
	ID uuid.UUID

	JobName       string
	NamespaceName string
	ProjectName   string

	StartTime   time.Time
	EndTime     time.Time
	Description string
	Parallel    bool
	JobConfig   map[string]string

	Status  string
	Message string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (r *replayRequest) toSchedulerReplayRequest() (*scheduler.Replay, error) {
	tnnt, err := tenant.NewTenant(r.ProjectName, r.NamespaceName)
	if err != nil {
		return nil, err
	}
	conf := scheduler.NewReplayConfig(r.StartTime, r.EndTime, r.Parallel, r.JobConfig, r.Description)
	replayStatus, err := scheduler.ReplayStateFromString(r.Status)
	if err != nil {
		return nil, err
	}
	jobName, err := scheduler.JobNameFrom(r.JobName)
	if err != nil {
		return nil, err
	}
	return scheduler.NewReplay(r.ID, jobName, tnnt, conf, replayStatus, r.CreatedAt, r.UpdatedAt, r.Message), nil
}

type replayRun struct {
	ID uuid.UUID

	JobName       string
	NamespaceName string
	ProjectName   string

	StartTime   time.Time
	EndTime     time.Time
	Description string
	Parallel    bool
	JobConfig   map[string]string

	ReplayStatus string
	Message      string

	ScheduledTime time.Time
	RunStatus     string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (r ReplayRepository) RegisterReplay(ctx context.Context, replay *scheduler.Replay, runs []*scheduler.JobRunStatus) (uuid.UUID, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return uuid.Nil, err
	}

	if err := r.insertReplay(ctx, tx, replay); err != nil {
		tx.Rollback(ctx)
		return uuid.Nil, err
	}

	storedReplay, err := r.getReplayRequest(ctx, tx, replay)
	if err != nil {
		tx.Rollback(ctx)
		return uuid.Nil, err
	}

	// TODO: consider to store message of each run
	if err := r.insertReplayRuns(ctx, tx, storedReplay.ID, runs); err != nil {
		tx.Rollback(ctx)
		return uuid.Nil, err
	}

	tx.Commit(ctx)
	return storedReplay.ID, nil
}

func (r ReplayRepository) GetReplayRequestsByStatus(ctx context.Context, statusList []scheduler.ReplayState) ([]*scheduler.Replay, error) {
	getReplayRequest := `SELECT ` + replayColumns + ` FROM replay_request WHERE status = ANY($1)`
	rows, err := r.db.Query(ctx, getReplayRequest, statusList)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "unable to get replay list", err)
	}
	defer rows.Close()

	var replayReqs []*scheduler.Replay
	for rows.Next() {
		var rr replayRequest
		if err := rows.Scan(&rr.ID, &rr.JobName, &rr.NamespaceName, &rr.ProjectName, &rr.StartTime, &rr.EndTime, &rr.Description, &rr.Parallel, &rr.JobConfig,
			&rr.Status, &rr.Message, &rr.CreatedAt, &rr.UpdatedAt); err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "unable to get the stored replay", err)
		}
		schedulerReplayReq, err := rr.toSchedulerReplayRequest()
		if err != nil {
			return nil, err
		}
		replayReqs = append(replayReqs, schedulerReplayReq)
	}
	return replayReqs, nil
}

func (r ReplayRepository) GetReplaysByProject(ctx context.Context, projectName tenant.ProjectName, dayLimits int) ([]*scheduler.Replay, error) {
	getReplayRequest := `SELECT ` + replayColumns + ` FROM replay_request WHERE project_name=$1 LIMIT $2`
	rows, err := r.db.Query(ctx, getReplayRequest, projectName, dayLimits)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "unable to get replay list", err)
	}
	defer rows.Close()

	var replayReqs []*scheduler.Replay
	for rows.Next() {
		var rr replayRequest
		if err := rows.Scan(&rr.ID, &rr.JobName, &rr.NamespaceName, &rr.ProjectName, &rr.StartTime, &rr.EndTime, &rr.Description, &rr.Parallel, &rr.JobConfig,
			&rr.Status, &rr.Message, &rr.CreatedAt, &rr.UpdatedAt); err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "unable to get the stored replay", err)
		}
		schedulerReplayReq, err := rr.toSchedulerReplayRequest()
		if err != nil {
			return nil, err
		}
		replayReqs = append(replayReqs, schedulerReplayReq)
	}
	return replayReqs, nil
}

func (r ReplayRepository) GetReplayByID(ctx context.Context, replayID uuid.UUID) (*scheduler.ReplayWithRun, error) {
	rr, err := r.getReplayRequestByID(ctx, replayID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		return nil, errors.NotFound(job.EntityJob, fmt.Sprintf("no replay found for replay ID %s", replayID.String()))
	}

	runs, err := r.getReplayRuns(ctx, replayID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	replayTenant, err := tenant.NewTenant(rr.ProjectName, rr.NamespaceName)
	if err != nil {
		return nil, err
	}
	replayConfig := scheduler.ReplayConfig{
		StartTime:   rr.StartTime,
		EndTime:     rr.EndTime,
		JobConfig:   rr.JobConfig,
		Parallel:    rr.Parallel,
		Description: rr.Description,
	}
	replay := scheduler.NewReplay(rr.ID, scheduler.JobName(rr.JobName), replayTenant, &replayConfig, scheduler.ReplayState(rr.Status), rr.CreatedAt, rr.UpdatedAt, rr.Message)
	replayRuns := make([]*scheduler.JobRunStatus, len(runs))
	for i := range runs {
		replayRun := &scheduler.JobRunStatus{
			ScheduledAt: runs[i].ScheduledTime,
			State:       scheduler.State(runs[i].RunStatus),
		}
		replayRuns[i] = replayRun
	}

	return &scheduler.ReplayWithRun{
		Replay: replay,
		Runs:   replayRuns,
	}, nil
}

func (r ReplayRepository) UpdateReplayStatus(ctx context.Context, id uuid.UUID, replayStatus scheduler.ReplayState, message string) error {
	return r.updateReplayRequest(ctx, id, replayStatus, message)
}

func (r ReplayRepository) UpdateReplay(ctx context.Context, id uuid.UUID, replayStatus scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error {
	if err := r.UpdateReplayRuns(ctx, id, runs); err != nil {
		return err
	}
	return r.updateReplayRequest(ctx, id, replayStatus, message)
}

func (r ReplayRepository) GetReplayJobConfig(ctx context.Context, jobTenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (map[string]string, error) {
	getReplayRequest := `SELECT job_config FROM replay_request WHERE job_name=$1 AND namespace_name=$2 AND project_name=$3 AND start_time<=$4 AND $4<=end_time AND status=$5 ORDER BY created_at DESC LIMIT 1`
	configs := map[string]string{}
	if err := r.db.QueryRow(ctx, getReplayRequest, jobName, jobTenant.NamespaceName(), jobTenant.ProjectName(), scheduledAt, scheduler.ReplayStateInProgress.String()).
		Scan(&configs); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.Wrap(job.EntityJob, "unable to get replay job configs", err)
		}
	}
	return configs, nil
}

func (r ReplayRepository) ScanAbandonedReplayRequests(ctx context.Context, unhandledClassifierDuration time.Duration) ([]*scheduler.Replay, error) {
	nonTerminalStateString := make([]string, len(scheduler.ReplayNonTerminalStates))
	for i, state := range scheduler.ReplayNonTerminalStates {
		nonTerminalStateString[i] = state.String()
	}
	getReplayRequest := fmt.Sprintf("SELECT %s FROM replay_request WHERE status in ('%s') and  (now() - updated_at) > INTERVAL '%d Seconds'", replayColumns, strings.Join(nonTerminalStateString, "', '"), int64(unhandledClassifierDuration.Seconds()))
	rows, err := r.db.Query(ctx, getReplayRequest)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "unable to get replay list", err)
	}
	defer rows.Close()
	var replayReqs []*scheduler.Replay
	for rows.Next() {
		var rr replayRequest
		if err := rows.Scan(&rr.ID, &rr.JobName, &rr.NamespaceName, &rr.ProjectName, &rr.StartTime, &rr.EndTime, &rr.Description, &rr.Parallel, &rr.JobConfig,
			&rr.Status, &rr.Message, &rr.CreatedAt, &rr.UpdatedAt); err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "unable to get the stored replay", err)
		}
		schedulerReplayReq, err := rr.toSchedulerReplayRequest()
		if err != nil {
			return nil, err
		}
		replayReqs = append(replayReqs, schedulerReplayReq)
	}
	return replayReqs, nil
}

func (r ReplayRepository) AcquireReplayRequest(ctx context.Context, replayID uuid.UUID, unhandledClassifierDuration time.Duration) error {
	query := fmt.Sprintf("update replay_request set updated_at = now() WHERE id = $1 and ( now() - updated_at ) > INTERVAL '%d Seconds'", int64(unhandledClassifierDuration.Seconds()))
	tag, err := r.db.Exec(ctx, query, replayID)
	if err != nil {
		return errors.Wrap(scheduler.EntityReplay, "error acquiring replay request", err)
	}

	if tag.RowsAffected() == 0 {
		return errors.NotFound(scheduler.EntityReplay, "unable to acquire replay request, perhaps acquired by another server")
	}
	return nil
}

func (r ReplayRepository) getReplayRequestWithFilters(ctx context.Context, projectName tenant.ProjectName, filters ...filter.FilterOpt) ([]replayRequest, error) {
	f := filter.NewFilter(filters...)
	var filterQueryFragments []string
	filterQueryFragments = append(filterQueryFragments, fmt.Sprintf(`project_name = '%s'`, projectName))
	if f.Contains(filter.JobNames) {
		jobNames := f.GetStringArrayValue(filter.JobNames)
		for _, name := range jobNames {
			jobName, err := scheduler.JobNameFrom(name)
			if err != nil {
				return nil, err
			}
			jobNames = append(jobNames, jobName.String())
		}
		if len(jobNames) > 1 {
			filterQueryFragments = append(filterQueryFragments, fmt.Sprintf("job_name in ('%s')", strings.Join(jobNames, "', '")))
		} else {
			filterQueryFragments = append(filterQueryFragments, fmt.Sprintf(`job_name = '%s'`, jobNames[0]))
		}
	}
	if f.Contains(filter.ScheduledAt) {
		scheduledAt := f.GetTimeValue(filter.ScheduledAt)
		filterQueryFragments = append(filterQueryFragments, fmt.Sprintf("start_time<='%s' AND '%s'<=end_time", scheduledAt.Format(time.DateTime), scheduledAt.Format(time.DateTime)))
	}
	if f.Contains(filter.ReplayStatus) {
		replayStatusString := f.GetStringValue(filter.ReplayStatus)
		replayState, err := scheduler.ReplayStateFromString(replayStatusString)
		if err != nil {
			return nil, err
		}
		filterQueryFragments = append(filterQueryFragments, fmt.Sprintf("status ='%s'", replayState))
	}
	getReplayRequest := fmt.Sprintf(`SELECT %s FROM replay_request WHERE %s ORDER BY created_at`, replayColumns, strings.Join(filterQueryFragments, " AND "))

	rows, err := r.db.Query(ctx, getReplayRequest)
	if err != nil {
		return nil, err
	}
	var rrRows []replayRequest
	for rows.Next() {
		var rr replayRequest
		err = rows.Scan(&rr.ID, &rr.JobName, &rr.NamespaceName, &rr.ProjectName, &rr.StartTime, &rr.EndTime, &rr.Description, &rr.Parallel, &rr.JobConfig,
			&rr.Status, &rr.Message, &rr.CreatedAt, &rr.UpdatedAt)
		if err != nil {
			return nil, err
		}
		rrRows = append(rrRows, rr)
	}
	return rrRows, nil
}

func (r ReplayRepository) GetReplayByFilters(ctx context.Context, projectName tenant.ProjectName, filters ...filter.FilterOpt) ([]*scheduler.ReplayWithRun, error) {
	rrr, err := r.getReplayRequestWithFilters(ctx, projectName, filters...)
	if err != nil {
		return nil, err
	}

	replayWithRuns := make([]*scheduler.ReplayWithRun, len(rrr))

	for i, rr := range rrr {
		runs, err := r.getReplayRuns(ctx, rr.ID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}

		replayTenant, err := tenant.NewTenant(rr.ProjectName, rr.NamespaceName)
		if err != nil {
			return nil, err
		}
		replayConfig := scheduler.ReplayConfig{
			StartTime:   rr.StartTime,
			EndTime:     rr.EndTime,
			JobConfig:   rr.JobConfig,
			Parallel:    rr.Parallel,
			Description: rr.Description,
		}
		replay := scheduler.NewReplay(rr.ID, scheduler.JobName(rr.JobName), replayTenant, &replayConfig, scheduler.ReplayState(rr.Status), rr.CreatedAt, rr.UpdatedAt, rr.Message)
		replayRuns := make([]*scheduler.JobRunStatus, len(runs))
		for i := range runs {
			replayRun := &scheduler.JobRunStatus{
				ScheduledAt: runs[i].ScheduledTime,
				State:       scheduler.State(runs[i].RunStatus),
			}
			replayRuns[i] = replayRun
		}

		replayWithRuns[i] = &scheduler.ReplayWithRun{
			Replay: replay,
			Runs:   replayRuns,
		}
	}

	return replayWithRuns, nil
}

func (r ReplayRepository) updateReplayRequest(ctx context.Context, id uuid.UUID, replayStatus scheduler.ReplayState, message string) error {
	_, err := r.db.Exec(ctx, updateReplayRequest, replayStatus, message, id)
	if err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "unable to update replay", err)
	}
	return nil
}

func (r ReplayRepository) CancelReplayRequest(ctx context.Context, id uuid.UUID, message string) error {
	query := `UPDATE replay_request SET status = $1, message = $2, updated_at = NOW() WHERE id = $3`
	if _, err := r.db.Exec(ctx, query, scheduler.ReplayStateCancelled, message, id); err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "unable to cancel replay", err)
	}
	return nil
}

func (r ReplayRepository) UpdateReplayRuns(ctx context.Context, id uuid.UUID, runs []*scheduler.JobRunStatus) error {
	query := `UPDATE replay_run SET status=$1, updated_at=NOW() WHERE replay_id=$2 AND scheduled_at=$3 AND status<>$1`
	for _, run := range runs {
		_, err := r.db.Exec(ctx, query, run.State, id, run.ScheduledAt)
		if err != nil {
			return errors.Wrap(scheduler.EntityJobRun, "unable to update replay runs", err)
		}
	}
	return nil
}

func (r ReplayRepository) UpdateReplayHeartbeat(ctx context.Context, id uuid.UUID) error {
	query := `UPDATE replay_request SET updated_at = NOW() WHERE id = $1`
	if _, err := r.db.Exec(ctx, query, id); err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "unable to update replay heatbeat", err)
	}
	return nil
}

func (ReplayRepository) insertReplay(ctx context.Context, tx pgx.Tx, replay *scheduler.Replay) error {
	insertReplay := `INSERT INTO replay_request (` + replayColumnsToStore + `, created_at, updated_at) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())`
	_, err := tx.Exec(ctx, insertReplay, replay.JobName().String(), replay.Tenant().NamespaceName(), replay.Tenant().ProjectName(),
		replay.Config().StartTime, replay.Config().EndTime, replay.Config().Description, replay.Config().Parallel, replay.Config().JobConfig, replay.State(), replay.Message())
	if err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "unable to store replay", err)
	}
	return nil
}

func (ReplayRepository) getReplayRequest(ctx context.Context, tx pgx.Tx, replay *scheduler.Replay) (replayRequest, error) {
	var rr replayRequest
	getReplayRequest := `SELECT ` + replayColumns + ` FROM replay_request where project_name = $1 and job_name = $2 and start_time = $3 and end_time = $4 order by created_at desc limit 1`
	if err := tx.QueryRow(ctx, getReplayRequest, replay.Tenant().ProjectName(), replay.JobName().String(), replay.Config().StartTime, replay.Config().EndTime).
		Scan(&rr.ID, &rr.JobName, &rr.NamespaceName, &rr.ProjectName, &rr.StartTime, &rr.EndTime, &rr.Description, &rr.Parallel, &rr.JobConfig,
			&rr.Status, &rr.Message, &rr.CreatedAt, &rr.UpdatedAt); err != nil {
		return rr, errors.Wrap(scheduler.EntityJobRun, "unable to get the stored replay", err)
	}
	return rr, nil
}

func (r ReplayRepository) getReplayRequestByID(ctx context.Context, replayID uuid.UUID) (replayRequest, error) {
	var rr replayRequest
	getReplayRequest := `SELECT ` + replayColumns + ` FROM replay_request WHERE id=$1`
	err := r.db.QueryRow(ctx, getReplayRequest, replayID).Scan(&rr.ID, &rr.JobName, &rr.NamespaceName, &rr.ProjectName, &rr.StartTime, &rr.EndTime, &rr.Description, &rr.Parallel, &rr.JobConfig,
		&rr.Status, &rr.Message, &rr.CreatedAt, &rr.UpdatedAt)
	if err != nil {
		return rr, err
	}
	return rr, nil
}

func (r ReplayRepository) getReplayRuns(ctx context.Context, replayID uuid.UUID) ([]replayRun, error) {
	var runs []replayRun
	getRuns := `SELECT ` + replayRunColumns + ` FROM replay_run WHERE replay_id=$1`
	rows, err := r.db.Query(ctx, getRuns, replayID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var run replayRun
		err = rows.Scan(&run.ID, &run.ScheduledTime, &run.RunStatus)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

func (ReplayRepository) insertReplayRuns(ctx context.Context, tx pgx.Tx, replayID uuid.UUID, runs []*scheduler.JobRunStatus) error {
	insertReplayRun := `INSERT INTO replay_run (` + replayRunColumns + `, created_at, updated_at) values ($1, $2, $3, NOW(), NOW())`
	for _, run := range runs {
		_, err := tx.Exec(ctx, insertReplayRun, replayID, run.ScheduledAt, run.State)
		if err != nil {
			return errors.Wrap(scheduler.EntityJobRun, "unable to store replay", err)
		}
	}
	return nil
}

func NewReplayRepository(db *pgxpool.Pool) *ReplayRepository {
	return &ReplayRepository{db: db}
}
