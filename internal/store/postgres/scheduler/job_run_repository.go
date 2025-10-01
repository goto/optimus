package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/interval"
)

const (
	columnsToStore = `job_name, namespace_name, project_name, scheduled_at, start_time, end_time, window_start, window_end, status, sla_definition, sla_alert`
	jobRunColumns  = `id, ` + columnsToStore + `, monitoring`
	dbTimeFormat   = "2006-01-02 15:04:05.000000+00"
)

type JobRunRepository struct {
	db *pgxpool.Pool
}

type jobRun struct {
	ID uuid.UUID

	JobName       string
	NamespaceName string
	ProjectName   string

	ScheduledAt time.Time
	StartTime   time.Time
	EndTime     *time.Time
	WindowStart *time.Time
	WindowEnd   *time.Time

	Status        string
	SLAAlert      bool
	SLADefinition int64

	CreatedAt time.Time
	UpdatedAt time.Time

	Monitoring json.RawMessage
}

func (j *jobRun) toJobRun() (*scheduler.JobRun, error) {
	t, err := tenant.NewTenant(j.ProjectName, j.NamespaceName)
	if err != nil {
		return nil, err
	}
	state, err := scheduler.StateFromString(j.Status)
	if err != nil {
		return nil, errors.AddErrContext(err, scheduler.EntityJobRun, "invalid job run state in database")
	}
	var monitoring map[string]any
	if j.Monitoring != nil {
		if err := json.Unmarshal(j.Monitoring, &monitoring); err != nil {
			return nil, errors.AddErrContext(err, scheduler.EntityJobRun, "invalid monitoring values in database")
		}
	}
	var windowStart *time.Time
	if j.WindowStart != nil {
		t1 := j.WindowStart.UTC()
		windowStart = &t1
	}

	var windowEnd *time.Time
	if j.WindowEnd != nil {
		t2 := j.WindowEnd.UTC()
		windowEnd = &t2
	}

	return &scheduler.JobRun{
		ID:            j.ID,
		JobName:       scheduler.JobName(j.JobName),
		Tenant:        t,
		State:         state,
		ScheduledAt:   j.ScheduledAt,
		SLAAlert:      j.SLAAlert,
		StartTime:     j.StartTime,
		EndTime:       j.EndTime,
		WindowStart:   windowStart,
		WindowEnd:     windowEnd,
		SLADefinition: j.SLADefinition,
		Monitoring:    monitoring,
	}, nil
}

func (j *JobRunRepository) GetByID(ctx context.Context, id scheduler.JobRunID) (*scheduler.JobRun, error) {
	var jr jobRun
	getJobRunByID := `SELECT ` + jobRunColumns + ` FROM job_run where id = $1`
	err := j.db.QueryRow(ctx, getJobRunByID, id.UUID()).
		Scan(&jr.ID, &jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.WindowStart, &jr.WindowEnd, &jr.Status, &jr.SLADefinition, &jr.SLAAlert, &jr.Monitoring)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for job run id "+id.UUID().String())
		}
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job run", err)
	}
	return jr.toJobRun()
}

func (j *JobRunRepository) GetLatestRun(ctx context.Context, project tenant.ProjectName, jobName scheduler.JobName, runState *scheduler.State) (*scheduler.JobRun, error) {
	var jr jobRun
	var stateClause string
	if runState != nil {
		stateClause = fmt.Sprintf(" and status = '%s'", runState)
	}
	getLatestRun := fmt.Sprintf("SELECT %s, created_at FROM job_run j where project_name = $1 and job_name = $2 %s order by scheduled_at desc limit 1", jobRunColumns, stateClause)
	err := j.db.QueryRow(ctx, getLatestRun, project, jobName).
		Scan(&jr.ID, &jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.WindowStart, &jr.WindowEnd, &jr.Status, &jr.SLADefinition, &jr.SLAAlert, &jr.Monitoring, &jr.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for job:"+jobName.String()+stateClause)
		}
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting run", err)
	}
	return jr.toJobRun()
}

func (j *JobRunRepository) GetRunsByInterval(ctx context.Context, project tenant.ProjectName, jobName scheduler.JobName, interval interval.Interval) ([]*scheduler.JobRun, error) {
	var jobRunList []*scheduler.JobRun

	getRuns := fmt.Sprintf("SELECT %s FROM job_run j where project_name = $1 and job_name = $2 and window_end >= $3 and window_start <= $4", jobRunColumns)
	rows, err := j.db.Query(ctx, getRuns, project, jobName, interval.Start(), interval.End())
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs", err)
	}
	for rows.Next() {
		var jr jobRun
		err := rows.Scan(&jr.ID, &jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.WindowStart, &jr.WindowEnd, &jr.Status, &jr.SLADefinition, &jr.SLAAlert, &jr.Monitoring)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return []*scheduler.JobRun{}, nil
			}
			return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting run", err)
		}
		jobRun, err := jr.toJobRun()
		if err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs", err)
		}
		jobRunList = append(jobRunList, jobRun)
	}
	return jobRunList, nil
}

func (j *JobRunRepository) GetRunsByTimeRange(ctx context.Context, project tenant.ProjectName, jobName scheduler.JobName, runState *scheduler.State, since, until time.Time) ([]*scheduler.JobRun, error) {
	var jobRunList []*scheduler.JobRun
	var stateClause string
	if runState != nil {
		stateClause = fmt.Sprintf(" and status = '%s'", runState)
	}
	getLatestRun := fmt.Sprintf("SELECT %s, created_at FROM job_run j where project_name = $1 and job_name = $2 and scheduled_at >= $3 and scheduled_at <= $4 %s", jobRunColumns, stateClause)
	rows, err := j.db.Query(ctx, getLatestRun, project, jobName, since, until)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs", err)
	}
	for rows.Next() {
		var jr jobRun
		err := rows.Scan(&jr.ID, &jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.WindowStart, &jr.WindowEnd, &jr.Status, &jr.SLADefinition, &jr.SLAAlert, &jr.Monitoring, &jr.CreatedAt)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return []*scheduler.JobRun{}, nil
			}
			return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting run", err)
		}
		jobRun, err := jr.toJobRun()
		if err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs", err)
		}
		jobRunList = append(jobRunList, jobRun)
	}
	return jobRunList, nil
}

func (j *JobRunRepository) GetByScheduledAt(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	var jr jobRun
	// todo: check if `order by created_at desc limit 1` is required
	getJobRunByScheduledAt := `SELECT ` + jobRunColumns + `, created_at FROM job_run j where project_name = $1 and namespace_name = $2 and job_name = $3 and scheduled_at = $4 order by created_at desc limit 1`
	err := j.db.QueryRow(ctx, getJobRunByScheduledAt, t.ProjectName(), t.NamespaceName(), jobName, scheduledAt).
		Scan(&jr.ID, &jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.WindowStart, &jr.WindowEnd, &jr.Status, &jr.SLADefinition, &jr.SLAAlert, &jr.Monitoring, &jr.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for job:"+jobName.String()+" scheduled at: "+scheduledAt.String())
		}
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting run", err)
	}
	return jr.toJobRun()
}

func (j *JobRunRepository) GetByScheduledTimes(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduleTimes []time.Time) ([]*scheduler.JobRun, error) {
	var jobRunList []*scheduler.JobRun
	var scheduledTimesString []string
	for _, scheduleTime := range scheduleTimes {
		scheduledTimesString = append(scheduledTimesString, scheduleTime.UTC().Format(dbTimeFormat))
	}

	getJobRunByScheduledTimesTemp := `SELECT ` + jobRunColumns + `,created_at FROM job_run j where project_name = $1 and namespace_name = $2 and job_name = $3 and scheduled_at in ('` + strings.Join(scheduledTimesString, "', '") + `')`
	rows, err := j.db.Query(ctx, getJobRunByScheduledTimesTemp, t.ProjectName(), t.NamespaceName(), jobName.String())
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs", err)
	}
	for rows.Next() {
		var jr jobRun
		err := rows.Scan(&jr.ID, &jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.WindowStart, &jr.WindowEnd, &jr.Status, &jr.SLADefinition, &jr.SLAAlert, &jr.Monitoring, &jr.CreatedAt)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NotFound(scheduler.EntityJobRun, "no record of job run :"+jobName.String()+" for schedule Times : "+strings.Join(scheduledTimesString, ", "))
			}
			return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs", err)
		}
		jobRun, err := jr.toJobRun()
		if err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs", err)
		}
		jobRunList = append(jobRunList, jobRun)
	}

	return jobRunList, nil
}

func (j *JobRunRepository) GetP95DurationByJobNames(ctx context.Context, jobNames []scheduler.JobName, tasks map[string][]string, lastNRuns int) (map[scheduler.JobName]*time.Duration, error) {
	// TODO: calculate based on task type
	if len(jobNames) == 0 {
		return map[scheduler.JobName]*time.Duration{}, nil
	}
	var jobNamesString []string
	for _, jobName := range jobNames {
		jobNamesString = append(jobNamesString, fmt.Sprintf("'%s'", jobName))
	}
	query := `
	WITH last_n_runs AS (
		SELECT
			j.id AS job_run_id,
			j.job_name,
			t.start_time,
			t.end_time,
			ROW_NUMBER() OVER (
				PARTITION BY j.job_name
				ORDER BY j.scheduled_at DESC
			) AS rn
		FROM job_run j
		JOIN task_run t ON t.job_run_id = j.id
		WHERE t.end_time IS NOT NULL
		AND j.job_name IN (%s)
	)
	SELECT
		job_name,
		percentile_cont(0.95) WITHIN GROUP (
			ORDER BY EXTRACT(EPOCH FROM (end_time - start_time))
		) AS p95_duration_seconds
	FROM last_n_runs
	WHERE rn <= %d  -- keep only last N runs per job
	GROUP BY job_name
	ORDER BY p95_duration_seconds DESC;`
	query = fmt.Sprintf(query, lastNRuns, strings.Join(jobNamesString, ","))
	rows, err := j.db.Query(ctx, query)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs duration", err)
	}
	jobDurations := make(map[scheduler.JobName]*time.Duration)
	for rows.Next() {
		var jobName string
		var p95DurationSeconds float64
		err := rows.Scan(&jobName, &p95DurationSeconds)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return map[scheduler.JobName]*time.Duration{}, nil
			}
			return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs duration", err)
		}
		duration := time.Duration(p95DurationSeconds * float64(time.Second))
		jobDurations[scheduler.JobName(jobName)] = &duration
	}
	return jobDurations, nil
}

func (j *JobRunRepository) UpdateState(ctx context.Context, jobRunID uuid.UUID, status scheduler.State) error {
	updateJobRun := "update job_run set status = $1, updated_at = NOW() where id = $2"
	_, err := j.db.Exec(ctx, updateJobRun, status, jobRunID)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to update job run", err)
}

func (j *JobRunRepository) Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, status scheduler.State) error {
	updateJobRun := "update job_run set status = $1, end_time = $2, updated_at = NOW() where id = $3"
	_, err := j.db.Exec(ctx, updateJobRun, status, endTime, jobRunID)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to update job run", err)
}

func (j *JobRunRepository) UpdateSLA(ctx context.Context, jobName scheduler.JobName, projectName tenant.ProjectName, scheduleTimes []time.Time) error {
	if len(scheduleTimes) == 0 {
		return nil
	}
	var scheduleTimesListString []string
	for _, scheduleTime := range scheduleTimes {
		scheduleTimesListString = append(scheduleTimesListString, scheduleTime.UTC().Format(dbTimeFormat))
	}
	query := `
update
    job_run
set
    sla_alert = True, updated_at = NOW()
where
    job_name = $1 and project_name = $2 and scheduled_at IN ('` + strings.Join(scheduleTimesListString, "', '") + "')"
	_, err := j.db.Exec(ctx, query, jobName, projectName)

	return errors.WrapIfErr(scheduler.EntityJobRun, "cannot update job Run State as SLA miss", err)
}

func (j *JobRunRepository) UpdateMonitoring(ctx context.Context, jobRunID uuid.UUID, monitoringValues map[string]any) error {
	monitoringBytes, err := json.Marshal(monitoringValues)
	if err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "error marshalling monitoring values", err)
	}
	query := `update job_run set monitoring = $1 where id = $2`
	_, err = j.db.Exec(ctx, query, monitoringBytes, jobRunID)
	return errors.WrapIfErr(scheduler.EntityJobRun, "cannot update monitoring", err)
}

func (j *JobRunRepository) Create(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time, window interval.Interval, slaDefinitionInSec int64) error {
	// TODO: startTime should be event time
	insertJobRun := `INSERT INTO job_run (` + columnsToStore + `, created_at, updated_at) values ($1, $2, $3, $4, NOW(), null, $5, $6, $7, $8, FALSE, NOW(), NOW()) ON CONFLICT DO NOTHING`
	_, err := j.db.Exec(ctx, insertJobRun, jobName, t.NamespaceName(), t.ProjectName(), scheduledAt, window.Start(), window.End(), scheduler.StateRunning, slaDefinitionInSec)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to create job run", err)
}

func (j *JobRunRepository) GetRunSummaryByIdentifiers(ctx context.Context, identifiers []scheduler.JobRunIdentifier) ([]*scheduler.JobRunSummary, error) {
	if len(identifiers) == 0 {
		return []*scheduler.JobRunSummary{}, nil
	}

	var conditions []string
	var args []any
	argIndex := 1

	for _, identifier := range identifiers {
		condition := fmt.Sprintf("(jr.job_name = $%d AND jr.scheduled_at = $%d)",
			argIndex, argIndex+1)
		conditions = append(conditions, condition)
		args = append(args, identifier.JobName, identifier.ScheduledAt.UTC().Format(dbTimeFormat))
		argIndex += 2
	}

	query := fmt.Sprintf(`
WITH operations AS (
	SELECT 
		jr.job_name,
		jr.scheduled_at,
		jr.start_time AS job_start_time,
		jr.end_time AS job_end_time,
		opr.operation_type,
		opr.start_time,
		COALESCE(opr.end_time, CASE WHEN jr.status IN ('failed', 'success') THEN jr.end_time ELSE NOW() END) AS end_time,
		ROW_NUMBER() OVER (PARTITION BY jr.job_name, jr.scheduled_at, opr.operation_type ORDER BY EXTRACT(EPOCH FROM (COALESCE(opr.end_time, jr.end_time) - opr.start_time)) DESC) as rn
	FROM job_run jr
	JOIN (
		SELECT job_run_id, start_time, end_time, 'sensor' AS operation_type FROM sensor_run 
		UNION ALL 
		SELECT job_run_id, start_time, end_time, 'task' AS operation_type FROM task_run 
		UNION ALL 
		SELECT job_run_id, start_time, end_time, 'hook' AS operation_type FROM hook_run 
	) opr ON opr.job_run_id = jr.id
	WHERE %s
	AND opr.start_time IS NOT NULL
)
SELECT 
	job_name,
	scheduled_at,
	job_start_time,
	job_end_time,
	MAX(CASE WHEN operation_type = 'sensor' AND rn = 1 THEN start_time END) as sensor_start_time,
	MAX(CASE WHEN operation_type = 'sensor' AND rn = 1 THEN end_time END) as sensor_end_time,
	MAX(CASE WHEN operation_type = 'task' AND rn = 1 THEN start_time END) as task_start_time,
	MAX(CASE WHEN operation_type = 'task' AND rn = 1 THEN end_time END) as task_end_time,
	MAX(CASE WHEN operation_type = 'hook' AND rn = 1 THEN start_time END) as hook_start_time,
	MAX(CASE WHEN operation_type = 'hook' AND rn = 1 THEN end_time END) as hook_end_time
FROM operations
WHERE rn = 1
GROUP BY job_name, scheduled_at, job_start_time, job_end_time
ORDER BY scheduled_at DESC
	`, strings.Join(conditions, " OR "))

	rows, err := j.db.Query(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job run summaries", err)
	}
	defer rows.Close()

	var summaries []*scheduler.JobRunSummary
	for rows.Next() {
		var (
			jobName         string
			scheduledAt     time.Time
			jobStartTime    *time.Time
			jobEndTime      *time.Time
			sensorStartTime *time.Time
			sensorEndTime   *time.Time
			taskStartTime   *time.Time
			taskEndTime     *time.Time
			hookStartTime   *time.Time
			hookEndTime     *time.Time
		)

		err = rows.Scan(&jobName, &scheduledAt, &jobStartTime, &jobEndTime,
			&sensorStartTime, &sensorEndTime,
			&taskStartTime, &taskEndTime,
			&hookStartTime, &hookEndTime)
		if err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "error while scanning job run summary", err)
		}

		summary := &scheduler.JobRunSummary{
			JobName:       scheduler.JobName(jobName),
			ScheduledAt:   scheduledAt,
			JobStartTime:  jobStartTime,
			JobEndTime:    jobEndTime,
			WaitStartTime: sensorStartTime,
			WaitEndTime:   sensorEndTime,
			TaskStartTime: taskStartTime,
			TaskEndTime:   taskEndTime,
			HookStartTime: hookStartTime,
			HookEndTime:   hookEndTime,
		}
		summaries = append(summaries, summary)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error iterating job run rows", err)
	}

	return summaries, nil
}

func NewJobRunRepository(pool *pgxpool.Pool) *JobRunRepository {
	return &JobRunRepository{
		db: pool,
	}
}
