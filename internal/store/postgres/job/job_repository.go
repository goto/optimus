package job

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/store/postgres"
	"github.com/goto/optimus/internal/utils"
)

const (
	jobColumnsToStore = `name, version, owner, description, labels, schedule, alert, webhook, static_upstreams, http_upstreams, 
	task_name, task_config, window_spec, assets, hooks, metadata, destination, sources, project_name, namespace_name, created_at, updated_at`

	jobColumns = `id, ` + jobColumnsToStore + `, deleted_at, is_dirty`

	enabledOnlyStatement = ` AND state != 'disabled'`
)

var changelogMetrics = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "job_repository_changelog_metrics_error",
	Help: "success or failure metrics",
}, []string{"project", "namespace", "name", "msg"})

type JobRepository struct {
	db *pgxpool.Pool
}

func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{db: pool}
}

func (j JobRepository) Add(ctx context.Context, jobs []*job.Job) ([]*job.Job, error) {
	me := errors.NewMultiError("add jobs errors")
	var storedJobs []*job.Job
	for _, jobEntity := range jobs {
		if err := j.insertJobSpec(ctx, jobEntity); err != nil {
			me.Append(err)
			continue
		}
		storedJobs = append(storedJobs, jobEntity)
	}
	return storedJobs, me.ToErr()
}

func (j JobRepository) insertJobSpec(ctx context.Context, jobEntity *job.Job) error {
	existingJob, err := j.get(ctx, jobEntity.ProjectName(), jobEntity.Spec().Name(), false, false)
	if err != nil {
		if errors.IsErrorType(err, errors.ErrNotFound) {
			return j.triggerInsert(ctx, jobEntity)
		}
		return errors.NewError(errors.ErrInternalError, job.EntityJob, fmt.Sprintf("failed to check job %s in db: %s", jobEntity.Spec().Name().String(), err.Error()))
	}
	if existingJob.DeletedAt.Valid {
		if existingJob.NamespaceName == jobEntity.Tenant().NamespaceName().String() {
			return j.updateAndPersistChangelog(ctx, jobEntity)
		}
		errorMsg := fmt.Sprintf("job %s already exists and soft deleted in namespace %s. consider hard delete the job before inserting to this namespace.", existingJob.Name, existingJob.NamespaceName)
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, errorMsg)
	}
	return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, fmt.Sprintf("job %s already exists in namespace %s", existingJob.Name, existingJob.NamespaceName))
}

func (j JobRepository) triggerInsert(ctx context.Context, jobEntity *job.Job) error {
	storageJob, err := toStorageSpec(jobEntity)
	if err != nil {
		return err
	}

	insertJobQuery := `INSERT INTO job (` + jobColumnsToStore + `)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
	$17, $18, $19, $20, NOW(), NOW());`

	tag, err := j.db.Exec(ctx, insertJobQuery,
		storageJob.Name, storageJob.Version, storageJob.Owner, storageJob.Description, storageJob.Labels,
		storageJob.Schedule, storageJob.Alert, storageJob.Webhook, storageJob.StaticUpstreams, storageJob.HTTPUpstreams,
		storageJob.TaskName, storageJob.TaskConfig, storageJob.WindowSpec, storageJob.Assets,
		storageJob.Hooks, storageJob.Metadata, storageJob.Destination, storageJob.Sources,
		storageJob.ProjectName, storageJob.NamespaceName)
	if err != nil {
		return errors.Wrap(job.EntityJob, "unable to save job spec", err)
	}

	if tag.RowsAffected() == 0 {
		return errors.InternalError(job.EntityJob, "unable to save job spec, rows affected 0", nil)
	}
	return nil
}

func (j JobRepository) Update(ctx context.Context, jobs []*job.Job) ([]*job.Job, error) {
	me := errors.NewMultiError("update jobs errors")
	var storedJobs []*job.Job
	for _, jobEntity := range jobs {
		if err := j.preCheckUpdate(ctx, jobEntity); err != nil {
			me.Append(err)
			continue
		}
		if err := j.updateAndPersistChangelog(ctx, jobEntity); err != nil {
			me.Append(err)
			continue
		}
		storedJobs = append(storedJobs, jobEntity)
	}
	return storedJobs, me.ToErr()
}

func (j JobRepository) UpdateState(ctx context.Context, jobTenant tenant.Tenant, jobNames []job.Name, jobState job.State, remark string) error {
	updateJobStateQuery := `
UPDATE job SET state = $1, remark = $2, updated_at = NOW()
WHERE project_name = $4 AND namespace_name = $5 AND name = any ($3);`

	tag, err := j.db.Exec(ctx, updateJobStateQuery, jobState, remark, jobNames, jobTenant.ProjectName(), jobTenant.NamespaceName())
	if err != nil {
		return errors.Wrap(job.EntityJob, "error during job state update", err)
	}
	if tag.RowsAffected() != int64(len(jobNames)) {
		return errors.NewError(errors.ErrNotFound, job.EntityJob, "failed to update state of all of the selected job in DB")
	}
	return nil
}

func (j JobRepository) SyncState(ctx context.Context, jobTenant tenant.Tenant, disabledJobNames, enabledJobNames []job.Name) error {
	tx, err := j.db.Begin(ctx)
	if err != nil {
		return err
	}
	updateJobStateQuery := `
UPDATE job SET state = $1
WHERE project_name = $2 AND namespace_name = $3 AND name = any ($4);`

	_, err = tx.Exec(ctx, updateJobStateQuery, job.ENABLED, jobTenant.ProjectName(), jobTenant.NamespaceName(), enabledJobNames)
	if err != nil {
		tx.Rollback(ctx)
		return errors.Wrap(job.EntityJob, "error during job state enable sync", err)
	}

	_, err = j.db.Exec(ctx, updateJobStateQuery, job.DISABLED, jobTenant.ProjectName(), jobTenant.NamespaceName(), disabledJobNames)
	if err != nil {
		tx.Rollback(ctx)
		return errors.Wrap(job.EntityJob, "error during job state disable sync", err)
	}

	tx.Commit(ctx)
	return nil
}

func (j JobRepository) ChangeJobNamespace(ctx context.Context, jobName job.Name, tenant, newTenant tenant.Tenant) error {
	tx, err := j.db.Begin(ctx)
	if err != nil {
		return errors.InternalError(job.EntityJob, "unable to begin transaction", err)
	}

	if err = changeJobNamespace(ctx, tx, jobName, tenant, newTenant); err != nil {
		tx.Rollback(ctx)
		return err
	}
	if err = changeJobUpstreamNamespace(ctx, tx, jobName, tenant, newTenant); err != nil {
		tx.Rollback(ctx)
		return err
	}
	if err = changeJobRunNamespace(ctx, tx, jobName, tenant, newTenant); err != nil {
		tx.Rollback(ctx)
		return err
	}
	tx.Commit(ctx)
	return nil
}

func changeJobNamespace(ctx context.Context, tx pgx.Tx, jobName job.Name, tenant, newTenant tenant.Tenant) error {
	changeJobNamespaceQuery := `
UPDATE job SET
	namespace_name = $1,
	updated_at = NOW()
WHERE
	name = $2 AND
	project_name = $3 AND
	namespace_name = $4 AND
	deleted_at is null
;`
	tag, err := tx.Exec(ctx, changeJobNamespaceQuery, newTenant.NamespaceName(), jobName,
		tenant.ProjectName(), tenant.NamespaceName())
	if err != nil {
		return errors.Wrap(job.EntityJob, err.Error(), err)
	}

	if tag.RowsAffected() == 0 {
		return errors.NotFound(job.EntityJob, "job not found with the given namespace: "+tenant.NamespaceName().String())
	}
	return nil
}

func changeJobUpstreamNamespace(ctx context.Context, tx pgx.Tx, jobName job.Name, tenant, newTenant tenant.Tenant) error {
	changeJobUpstreamsQuery := `
UPDATE job_upstream SET
	upstream_namespace_name = $1
WHERE
	upstream_job_name = $2 AND
	upstream_project_name = $3 AND
	upstream_namespace_name = $4
;`
	_, err := tx.Exec(ctx, changeJobUpstreamsQuery, newTenant.NamespaceName(), jobName,
		tenant.ProjectName(), tenant.NamespaceName())
	if err != nil {
		return errors.Wrap(job.EntityJob, err.Error(), err)
	}
	return nil
}

func changeJobRunNamespace(ctx context.Context, tx pgx.Tx, jobName job.Name, tenant, newTenant tenant.Tenant) error {
	changeJobRunNamespaceQuery := `
UPDATE job_run SET
	namespace_name = $1
WHERE
	job_name = $2 AND
	project_name = $3 AND
	namespace_name = $4
;`
	_, err := tx.Exec(ctx, changeJobRunNamespaceQuery, newTenant.NamespaceName(), jobName,
		tenant.ProjectName(), tenant.NamespaceName())
	if err != nil {
		return errors.Wrap(job.EntityJob, err.Error(), err)
	}
	return nil
}

func (j JobRepository) preCheckUpdate(ctx context.Context, jobEntity *job.Job) error {
	existingJob, err := j.get(ctx, jobEntity.ProjectName(), jobEntity.Spec().Name(), false, false)
	if err != nil && errors.IsErrorType(err, errors.ErrNotFound) {
		return errors.NewError(errors.ErrNotFound, job.EntityJob, fmt.Sprintf("job %s not exists yet", jobEntity.Spec().Name()))
	}
	if err != nil {
		return errors.NewError(errors.ErrInternalError, job.EntityJob, fmt.Sprintf("failed to check job %s in db: %s", jobEntity.Spec().Name().String(), err.Error()))
	}
	if existingJob.NamespaceName != jobEntity.Tenant().NamespaceName().String() && existingJob.DeletedAt.Valid {
		errorMsg := fmt.Sprintf("job %s already exists and soft deleted in namespace %s.", existingJob.Name, existingJob.NamespaceName)
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, errorMsg)
	}
	if existingJob.NamespaceName != jobEntity.Tenant().NamespaceName().String() && !existingJob.DeletedAt.Valid {
		errorMsg := fmt.Sprintf("job %s already exists in namespace %s.", existingJob.Name, existingJob.NamespaceName)
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, errorMsg)
	}
	if existingJob.DeletedAt.Valid {
		errorMsg := fmt.Sprintf("update is not allowed as job %s has been soft deleted. please re-add the job before updating.", existingJob.Name)
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, errorMsg)
	}
	return nil
}

func getJobDiff(storageSpecOld *Spec, newJobEntity *job.Job) ([]Change, error) {
	toIgnore := map[string]struct{}{
		"ID":        {},
		"CreatedAt": {},
		"UpdatedAt": {},
	}
	var changelog []Change
	storageSpecNew, err := toStorageSpec(newJobEntity)
	if err != nil {
		return changelog, err
	}

	diff, err := utils.GetDiffs(*storageSpecOld, *storageSpecNew, nil)
	if err != nil {
		return changelog, err
	}

	for _, d := range diff {
		if _, ok := toIgnore[d.Field]; !ok {
			changelog = append(changelog, Change{
				Property: d.Field,
				Diff:     d.Diff,
			})
		}
	}

	return changelog, nil
}

func (j JobRepository) insertChangelog(ctx context.Context, jobName job.Name, projectName tenant.ProjectName, changeLog []Change) error {
	changeLogBytes, err := json.Marshal(changeLog)
	if err != nil {
		return err
	}
	insertChangeLogQuery := `INSERT INTO changelog ( entity_type , name , project_name , change_type , changes , created_at )
	VALUES ($1, $2, $3, $4, $5, NOW());`

	tag, err := j.db.Exec(ctx, insertChangeLogQuery, "job", jobName, projectName, "update", string(changeLogBytes))
	if err != nil {
		return errors.Wrap(job.EntityJobChangeLog, "unable to insert job changelog", err)
	}

	if tag.RowsAffected() == 0 {
		return errors.InternalError(job.EntityJobChangeLog, "unable to insert job changelog, rows affected 0", nil)
	}
	return err
}

func (j JobRepository) GetChangelog(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.ChangeLog, error) {
	me := errors.NewMultiError("get change log errors")

	getChangeLogQuery := `select changes, change_type, created_at from changeLog where project_name = $1 and name = $2 and entity_type = $3;`

	rows, err := j.db.Query(ctx, getChangeLogQuery, projectName, jobName, "job")
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while changeLog for job: "+projectName.String()+"/"+jobName.String(), err)
	}
	defer rows.Close()

	var changeLog []*job.ChangeLog
	for rows.Next() {
		log, err := FromChangelogRow(rows)
		if err != nil {
			me.Append(err)
			continue
		}
		changeLog = append(changeLog, fromStorageChangelog(log))
	}

	return changeLog, me.ToErr()
}

func (j JobRepository) computeAndPersistChangeLog(ctx context.Context, existingJob *Spec, incomingJobEntity *job.Job) error {
	changeLog, err := getJobDiff(existingJob, incomingJobEntity)
	if err != nil {
		return err
	}
	if changeLog == nil {
		return nil
	}
	return j.insertChangelog(ctx, incomingJobEntity.Spec().Name(), incomingJobEntity.Tenant().ProjectName(), changeLog)
}

func (j JobRepository) updateAndPersistChangelog(ctx context.Context, incomingJobEntity *job.Job) error {
	existingJob, err := j.get(ctx, incomingJobEntity.ProjectName(), incomingJobEntity.Spec().Name(), false, false)
	if err != nil {
		return err
	}

	if err := j.triggerUpdate(ctx, incomingJobEntity); err != nil {
		return err
	}

	if err = j.computeAndPersistChangeLog(ctx, existingJob, incomingJobEntity); err != nil {
		changelogMetrics.WithLabelValues(
			incomingJobEntity.ProjectName().String(),
			incomingJobEntity.Tenant().NamespaceName().String(),
			incomingJobEntity.GetName(),
			err.Error(),
		).Inc()
	}
	return nil
}

func (j JobRepository) triggerUpdate(ctx context.Context, jobEntity *job.Job) error {
	storageJob, err := toStorageSpec(jobEntity)
	if err != nil {
		return err
	}

	updateJobQuery := `
UPDATE job SET
	version = $1, owner = $2, description = $3, labels = $4, schedule = $5, alert = $6,
	static_upstreams = $7, http_upstreams = $8, task_name = $9, task_config = $10,
	window_spec = $11, assets = $12, hooks = $13, metadata = $14, destination = $15, sources = $16, webhook = $19,
	updated_at = NOW(), deleted_at = null
WHERE
	name = $17 AND
	project_name = $18;`

	tag, err := j.db.Exec(ctx, updateJobQuery,
		storageJob.Version, storageJob.Owner, storageJob.Description,
		storageJob.Labels, storageJob.Schedule, storageJob.Alert,
		storageJob.StaticUpstreams, storageJob.HTTPUpstreams, storageJob.TaskName, storageJob.TaskConfig,
		storageJob.WindowSpec, storageJob.Assets, storageJob.Hooks, storageJob.Metadata,
		storageJob.Destination, storageJob.Sources,
		storageJob.Name, storageJob.ProjectName, storageJob.Webhook)
	if err != nil {
		return errors.Wrap(job.EntityJob, "unable to update job spec", err)
	}

	if tag.RowsAffected() == 0 {
		return errors.InternalError(job.EntityJob, "unable to update job spec, rows affected 0", nil)
	}
	return nil
}

func (j JobRepository) get(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, onlyActiveJob bool, onlyEnabledJob bool) (*Spec, error) {
	getJobByNameAtProject := `SELECT ` + jobColumns + ` FROM job WHERE name = $1 AND project_name = $2`

	if onlyActiveJob {
		jobDeletedFilter := " AND deleted_at IS NULL"
		getJobByNameAtProject += jobDeletedFilter
	}

	if onlyEnabledJob {
		getJobByNameAtProject += enabledOnlyStatement
	}

	spec, err := FromRow(j.db.QueryRow(ctx, getJobByNameAtProject, jobName.String(), projectName.String()))
	if errors.IsErrorType(err, errors.ErrNotFound) {
		err = errors.NotFound(job.EntityJob, fmt.Sprintf("unable to get job %s", jobName))
	}
	return spec, err
}

func (j JobRepository) SetDirty(ctx context.Context, jobsTenant tenant.Tenant, jobNames []job.Name, isDirty bool) error {
	if len(jobNames) < 1 {
		return nil
	}
	query := `
UPDATE job SET
	is_dirty = $1,
	updated_at = NOW()
WHERE
	project_name = $2 and
    name = any ($3)
;`
	tag, err := j.db.Exec(ctx, query, isDirty, jobsTenant.ProjectName(), jobNames)
	if err != nil {
		return errors.Wrap(job.EntityJob, err.Error(), err)
	}

	if tag.RowsAffected() == 0 {
		jobNamesString := make([]string, len(jobNames))
		for i, jobName := range jobNames {
			jobNamesString[i] = jobName.String()
		}
		return errors.NotFound(job.EntityJob, fmt.Sprintf("jobs: [%s], not found in the given project: %s", strings.Join(jobNamesString, ", "), jobsTenant.ProjectName()))
	}
	return nil
}

func (j JobRepository) ResolveUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Upstream, error) {
	query := `
WITH static_upstreams AS (
	SELECT j.name, j.project_name, d.static_upstream
	FROM job j
	JOIN UNNEST(j.static_upstreams) d(static_upstream) ON true
	WHERE project_name = $1
    AND name = any ($2)
	AND j.deleted_at IS NULL
	AND state != 'disabled'
), 

inferred_upstreams AS (
	SELECT j.name, j.project_name, s.source
	FROM job j
	JOIN UNNEST(j.sources) s(source) ON true
	WHERE project_name = $1
	AND name = any ($2)
	AND j.deleted_at IS NULL
	AND state != 'disabled'
)

SELECT
	su.name AS job_name,
	su.project_name,
	j.name AS upstream_job_name,
	j.project_name AS upstream_project_name,
	j.namespace_name AS upstream_namespace_name,
	j.destination AS upstream_resource_urn,
	j.task_name AS upstream_task_name,
	'static' AS upstream_type,
	false AS upstream_external
FROM static_upstreams su
JOIN job j ON
	(su.static_upstream = j.name and su.project_name = j.project_name) OR
	(su.static_upstream = j.project_name || '/' ||j.name)
WHERE j.deleted_at IS NULL
AND j.state != 'disabled'

UNION ALL

SELECT
	id.name AS job_name,
	id.project_name,
	j.name AS upstream_job_name,
	j.project_name AS upstream_project_name,
	j.namespace_name AS upstream_namespace_name,
	j.destination AS upstream_resource_urn,
	j.task_name AS upstream_task_name,
	'inferred' AS upstream_type,
	false AS upstream_external
FROM inferred_upstreams id
JOIN job j ON id.source = j.destination
WHERE j.deleted_at IS NULL
AND j.state != 'disabled';`

	rows, err := j.db.Query(ctx, query, projectName, jobNames)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job with upstreams", err)
	}
	defer rows.Close()

	var storeJobsWithUpstreams []*JobWithUpstream
	for rows.Next() {
		var jwu JobWithUpstream
		err := rows.Scan(&jwu.JobName, &jwu.ProjectName, &jwu.UpstreamJobName, &jwu.UpstreamProjectName,
			&jwu.UpstreamNamespaceName, &jwu.UpstreamResourceURN, &jwu.UpstreamTaskName, &jwu.UpstreamType, &jwu.UpstreamExternal)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NotFound(job.EntityJob, "job upstream not found")
			}

			return nil, errors.Wrap(resource.EntityResource, "error in reading row for resource", err)
		}
		storeJobsWithUpstreams = append(storeJobsWithUpstreams, &jwu)
	}
	return j.toJobNameWithUpstreams(storeJobsWithUpstreams)
}

func (j JobRepository) toJobNameWithUpstreams(storeJobsWithUpstreams []*JobWithUpstream) (map[job.Name][]*job.Upstream, error) {
	me := errors.NewMultiError("to job name with upstreams errors")
	upstreamsPerJobName := groupUpstreamsPerJobFullName(storeJobsWithUpstreams)

	jobNameWithUpstreams := make(map[job.Name][]*job.Upstream)
	for _, storeUpstreams := range upstreamsPerJobName {
		if len(storeUpstreams) == 0 {
			continue
		}
		name, err := job.NameFrom(storeUpstreams[0].JobName)
		if err != nil {
			me.Append(err)
			continue
		}
		upstreams, err := j.toUpstreams(storeUpstreams)
		if err != nil {
			me.Append(err)
			continue
		}

		jobNameWithUpstreams[name] = upstreams
	}

	return jobNameWithUpstreams, me.ToErr()
}

func groupUpstreamsPerJobFullName(upstreams []*JobWithUpstream) map[string][]*JobWithUpstream {
	upstreamsMap := make(map[string][]*JobWithUpstream)
	for _, upstream := range upstreams {
		upstreamsMap[upstream.getJobFullName()] = append(upstreamsMap[upstream.getJobFullName()], upstream)
	}
	return upstreamsMap
}

func (JobRepository) toUpstreams(storeUpstreams []*JobWithUpstream) ([]*job.Upstream, error) {
	me := errors.NewMultiError("to upstreams errors")

	var upstreams []*job.Upstream
	for _, storeUpstream := range storeUpstreams {
		var resourceURN resource.URN
		if storeUpstream.UpstreamResourceURN.Valid {
			if storeUpstream.UpstreamResourceURN.String != "" {
				resourceURN, _ = resource.ParseURN(storeUpstream.UpstreamResourceURN.String)
			}
		}

		var upstreamName job.Name
		if storeUpstream.UpstreamJobName.Valid {
			upstreamName, _ = job.NameFrom(storeUpstream.UpstreamJobName.String)
		}

		var upstreamProjectName tenant.ProjectName
		if storeUpstream.UpstreamProjectName.Valid {
			upstreamProjectName, _ = tenant.ProjectNameFrom(storeUpstream.UpstreamProjectName.String)
		}

		if storeUpstream.UpstreamState == job.UpstreamStateUnresolved.String() && storeUpstream.UpstreamJobName.Valid {
			upstreams = append(upstreams, job.NewUpstreamUnresolvedStatic(upstreamName, upstreamProjectName))
			continue
		}

		if storeUpstream.UpstreamState == job.UpstreamStateUnresolved.String() && storeUpstream.UpstreamResourceURN.Valid {
			upstreams = append(upstreams, job.NewUpstreamUnresolvedInferred(resourceURN))
			continue
		}

		var upstreamTenant tenant.Tenant
		var err error
		if storeUpstream.UpstreamProjectName.Valid && storeUpstream.UpstreamNamespaceName.Valid {
			upstreamTenant, err = tenant.NewTenant(storeUpstream.UpstreamProjectName.String, storeUpstream.UpstreamNamespaceName.String)
			if err != nil {
				me.Append(errors.Wrap(job.EntityJob, fmt.Sprintf("failed to get tenant for upstream %s of job %s", storeUpstream.UpstreamJobName.String, storeUpstream.JobName), err))
				continue
			}
		}

		var taskName job.TaskName
		if storeUpstream.UpstreamTaskName.Valid {
			taskName, err = job.TaskNameFrom(storeUpstream.UpstreamTaskName.String)
			if err != nil {
				me.Append(errors.Wrap(job.EntityJob, fmt.Sprintf("failed to get task for upstream %s of job %s", storeUpstream.UpstreamJobName.String, storeUpstream.JobName), err))
				continue
			}
		}

		upstreamType, err := job.UpstreamTypeFrom(storeUpstream.UpstreamType)
		if err != nil {
			me.Append(errors.Wrap(job.EntityJob, fmt.Sprintf("failed to get upstream type for upstream %s of job %s", storeUpstream.UpstreamJobName.String, storeUpstream.JobName), err))
			continue
		}

		var upstreamHost string
		if storeUpstream.UpstreamHost.Valid {
			upstreamHost = storeUpstream.UpstreamHost.String
		}

		var upstreamExternal bool
		if storeUpstream.UpstreamExternal.Valid {
			upstreamExternal = storeUpstream.UpstreamExternal.Bool
		}

		upstream := job.NewUpstreamResolved(upstreamName, upstreamHost, resourceURN, upstreamTenant, upstreamType, taskName, upstreamExternal)
		upstreams = append(upstreams, upstream)
	}
	return job.Upstreams(upstreams).Deduplicate(), me.ToErr()
}

func (j JobRepository) GetByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error) {
	spec, err := j.get(ctx, projectName, jobName, true, false)
	if err != nil {
		return nil, err
	}

	job, err := specToJob(spec)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (j JobRepository) GetEnabledJobByName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error) {
	spec, err := j.get(ctx, projectName, jobName, true, true)
	if err != nil {
		return nil, err
	}

	job, err := specToJob(spec)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (j JobRepository) GetAllByProjectName(ctx context.Context, projectName tenant.ProjectName) ([]*job.Job, error) {
	me := errors.NewMultiError("get all job specs by project name errors")

	getAllByProjectName := `SELECT ` + jobColumns + ` FROM job WHERE project_name = $1 AND deleted_at IS NULL;`

	rows, err := j.db.Query(ctx, getAllByProjectName, projectName)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while jobs for project:  "+projectName.String(), err)
	}
	defer rows.Close()

	var jobs []*job.Job
	for rows.Next() {
		spec, err := FromRow(rows)
		if err != nil {
			me.Append(err)
			continue
		}

		jobSpec, err := specToJob(spec)
		if err != nil {
			me.Append(err)
			continue
		}

		jobs = append(jobs, jobSpec)
	}

	return jobs, me.ToErr()
}

func (j JobRepository) GetAllByResourceDestination(ctx context.Context, resourceDestination resource.URN) ([]*job.Job, error) {
	return j.getAllByResourceDestination(ctx, resourceDestination, false)
}

func (j JobRepository) GetAllEnabledByResourceDestination(ctx context.Context, resourceDestination resource.URN) ([]*job.Job, error) {
	return j.getAllByResourceDestination(ctx, resourceDestination, true)
}

func (j JobRepository) getAllByResourceDestination(ctx context.Context, resourceDestination resource.URN, enabledOnly bool) ([]*job.Job, error) {
	me := errors.NewMultiError("get all job specs by resource destination")

	getAllByDestination := `SELECT ` + jobColumns + ` FROM job WHERE destination = $1 AND deleted_at IS NULL`
	if enabledOnly {
		getAllByDestination += enabledOnlyStatement
	}

	rows, err := j.db.Query(ctx, getAllByDestination, resourceDestination.String())
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while jobs for destination:  "+resourceDestination.String(), err)
	}
	defer rows.Close()

	var jobs []*job.Job
	for rows.Next() {
		spec, err := FromRow(rows)
		if err != nil {
			me.Append(err)
			continue
		}

		jobSpec, err := specToJob(spec)
		if err != nil {
			me.Append(err)
			continue
		}

		jobs = append(jobs, jobSpec)
	}

	return jobs, me.ToErr()
}

func specToJob(spec *Spec) (*job.Job, error) {
	jobSpec, err := fromStorageSpec(spec)
	if err != nil {
		return nil, err
	}

	tenantName, err := tenant.NewTenant(spec.ProjectName, spec.NamespaceName)
	if err != nil {
		return nil, err
	}

	var destination resource.URN
	if spec.Destination != "" {
		destination, err = resource.ParseURN(spec.Destination)
		if err != nil {
			return nil, err
		}
	}

	var sources []resource.URN
	for _, source := range spec.Sources {
		var resourceURN resource.URN
		if source != "" {
			resourceURN, err = resource.ParseURN(source)
			if err != nil {
				return nil, err
			}
		}

		sources = append(sources, resourceURN)
	}

	return job.NewJob(tenantName, jobSpec, destination, sources, spec.IsDirty), nil
}

type JobWithUpstream struct {
	JobName               string         `json:"job_name"`
	ProjectName           string         `json:"project_name"`
	UpstreamJobName       sql.NullString `json:"upstream_job_name"`
	UpstreamResourceURN   sql.NullString `json:"upstream_resource_urn"`
	UpstreamProjectName   sql.NullString `json:"upstream_project_name"`
	UpstreamNamespaceName sql.NullString `json:"upstream_namespace_name"`
	UpstreamTaskName      sql.NullString `json:"upstream_task_name"`
	UpstreamHost          sql.NullString `json:"upstream_host"`
	UpstreamType          string         `json:"upstream_type"`
	UpstreamState         string         `json:"upstream_state"`
	UpstreamExternal      sql.NullBool   `json:"upstream_external"`
}

func (j *JobWithUpstream) getJobFullName() string {
	return j.ProjectName + "/" + j.JobName
}

func (j JobRepository) ReplaceUpstreams(ctx context.Context, jobsWithUpstreams []*job.WithUpstream) error {
	var jobUpstreams []*JobWithUpstream
	for _, jobWithUpstreams := range jobsWithUpstreams {
		singleJobUpstreams := toJobUpstream(jobWithUpstreams)
		jobUpstreams = append(jobUpstreams, singleJobUpstreams...)
	}

	tx, err := j.db.Begin(ctx)
	if err != nil {
		return errors.InternalError(job.EntityJob, "unable to update upstreams", err)
	}

	var jobFullName []string
	for _, jobWithUpstream := range jobsWithUpstreams {
		jobFullName = append(jobFullName, jobWithUpstream.Job().FullName())
	}

	if err = j.deleteUpstreamsByJobNames(ctx, tx, jobFullName); err != nil {
		tx.Rollback(ctx)
		return err
	}
	if err = j.insertUpstreams(ctx, tx, jobUpstreams); err != nil {
		tx.Rollback(ctx)
		return err
	}

	tx.Commit(ctx)
	return nil
}

func (JobRepository) insertUpstreams(ctx context.Context, tx pgx.Tx, storageJobUpstreams []*JobWithUpstream) error {
	insertResolvedUpstreamQuery := `
INSERT INTO job_upstream (
	job_id, job_name, project_name,
	upstream_job_id, upstream_job_name, upstream_resource_urn,
	upstream_project_name, upstream_namespace_name, upstream_host,
	upstream_task_name, upstream_external,
	upstream_type, upstream_state,
	created_at
)
VALUES (
	(select id FROM job WHERE name = $1 and project_name = $2), $1, $2,
	(select id FROM job WHERE name = $3 and project_name = $5), $3, $4,
	$5, $6, $7,
	$8, $9,
	$10, $11,
	NOW()
);`

	insertUnresolvedUpstreamQuery := `
INSERT INTO job_upstream (
	job_id, job_name, project_name,
	upstream_job_name, upstream_resource_urn, upstream_project_name,
	upstream_type, upstream_state,
	created_at
)
VALUES (
	(select id FROM job WHERE name = $1 and project_name = $2), $1, $2, 
	$3, $4, $5,
	$6, $7, 
	NOW()
);
`

	var tag pgconn.CommandTag
	var err error
	for _, upstream := range storageJobUpstreams {
		if upstream.UpstreamState == job.UpstreamStateResolved.String() {
			tag, err = tx.Exec(ctx, insertResolvedUpstreamQuery,
				upstream.JobName, upstream.ProjectName,
				upstream.UpstreamJobName, upstream.UpstreamResourceURN,
				upstream.UpstreamProjectName, upstream.UpstreamNamespaceName, upstream.UpstreamHost,
				upstream.UpstreamTaskName, upstream.UpstreamExternal,
				upstream.UpstreamType, upstream.UpstreamState)
		} else {
			tag, err = tx.Exec(ctx, insertUnresolvedUpstreamQuery,
				upstream.JobName, upstream.ProjectName,
				upstream.UpstreamJobName, upstream.UpstreamResourceURN, upstream.UpstreamProjectName,
				upstream.UpstreamType, upstream.UpstreamState)
		}

		if err != nil {
			if postgres.ErrorCodeEqual(err, postgres.ErrPgCodeUniqueConstraints) {
				errMsg := fmt.Sprintf("duplicate constraints found in job_name %s with upstream job_name %s", upstream.JobName, upstream.UpstreamJobName.String)
				wrappedErr := errors.NewError(errors.ErrFailedPrecond, job.EntityJob, errMsg)
				wrappedErr.WrappedErr = err
				return wrappedErr
			}

			return errors.InternalError(job.EntityJob, "unable to save job upstream", err)
		}

		if tag.RowsAffected() == 0 {
			return errors.NewError(errors.ErrInternalError, job.EntityJob, "unable to save job upstream, rows affected 0")
		}
	}
	return nil
}

func (JobRepository) deleteUpstreamsByJobNames(ctx context.Context, tx pgx.Tx, jobUpstreams []string) error {
	deleteForProjectScope := `DELETE
FROM job_upstream
WHERE project_name || '/' || job_name = any ($1);`

	_, err := tx.Exec(ctx, deleteForProjectScope, jobUpstreams)
	if err != nil {
		return errors.Wrap(job.EntityJob, "error during delete of job upstream", err)
	}

	return nil
}

func toJobUpstream(jobWithUpstream *job.WithUpstream) []*JobWithUpstream {
	var jobUpstreams []*JobWithUpstream
	for _, upstream := range jobWithUpstream.Upstreams() {
		var upstreamProjectName, upstreamNamespaceName string
		// TODO: re-check this implementation as project and namespace name is not supposed to be empty within a tenant
		if upstream.ProjectName() != "" {
			upstreamProjectName = upstream.ProjectName().String()
		}
		if upstream.NamespaceName() != "" {
			upstreamNamespaceName = upstream.NamespaceName().String()
		}
		jobUpstreams = append(jobUpstreams, &JobWithUpstream{
			JobName:               jobWithUpstream.Name().String(),
			ProjectName:           jobWithUpstream.Job().ProjectName().String(),
			UpstreamJobName:       toNullString(upstream.Name().String()),
			UpstreamResourceURN:   toNullString(upstream.Resource().String()),
			UpstreamProjectName:   toNullString(upstreamProjectName),
			UpstreamNamespaceName: toNullString(upstreamNamespaceName),
			UpstreamTaskName:      toNullString(upstream.TaskName().String()),
			UpstreamHost:          toNullString(upstream.Host()),
			UpstreamType:          upstream.Type().String(),
			UpstreamState:         upstream.State().String(),
			UpstreamExternal:      toNullBool(upstream.External()),
		})
	}
	return jobUpstreams
}

func toNullString(val string) sql.NullString {
	if val == "" {
		return sql.NullString{}
	}
	return sql.NullString{
		String: val,
		Valid:  true,
	}
}

func toNullBool(val bool) sql.NullBool {
	return sql.NullBool{
		Bool:  val,
		Valid: true,
	}
}

type ProjectAndJobNames struct {
	ProjectName string `json:"project_name"`
	JobName     string `json:"job_name"`
}

func (j JobRepository) Delete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, cleanHistory bool) error {
	if cleanHistory {
		return j.hardDelete(ctx, projectName, jobName)
	}
	return j.softDelete(ctx, projectName, jobName)
}

func (j JobRepository) hardDelete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) error {
	query := `DELETE FROM job WHERE project_name = $1 AND name = $2`

	tag, err := j.db.Exec(ctx, query, projectName, jobName)
	if err != nil {
		return errors.Wrap(job.EntityJob, "error during job deletion", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.NewError(errors.ErrInternalError, job.EntityJob, fmt.Sprintf("job %s failed to be deleted", jobName.String()))
	}
	return nil
}

func (j JobRepository) softDelete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) error {
	query := `UPDATE job SET deleted_at = current_timestamp WHERE project_name = $1 AND name = $2`

	tag, err := j.db.Exec(ctx, query, projectName, jobName)
	if err != nil {
		return errors.Wrap(job.EntityJob, "error during job deletion", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.NewError(errors.ErrInternalError, job.EntityJob, fmt.Sprintf("job %s failed to be deleted", jobName.String()))
	}
	return nil
}

func (j JobRepository) GetAllByTenant(ctx context.Context, jobTenant tenant.Tenant) ([]*job.Job, error) {
	me := errors.NewMultiError("get all job specs by project name errors")

	getAllByProjectName := `SELECT ` + jobColumns + ` FROM job
	WHERE project_name = $1 AND namespace_name = $2 AND deleted_at IS NULL;`

	rows, err := j.db.Query(ctx, getAllByProjectName, jobTenant.ProjectName(), jobTenant.NamespaceName())
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while jobs for project:  "+jobTenant.ProjectName().String(), err)
	}
	defer rows.Close()

	var jobs []*job.Job
	for rows.Next() {
		spec, err := FromRow(rows)
		if err != nil {
			me.Append(err)
			continue
		}

		jobSpec, err := specToJob(spec)
		if err != nil {
			me.Append(err)
			continue
		}

		jobs = append(jobs, jobSpec)
	}

	return jobs, me.ToErr()
}

func (j JobRepository) GetUpstreams(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.Upstream, error) {
	query := `
SELECT
	job_name, project_name, upstream_job_name, upstream_resource_urn, upstream_project_name,
	upstream_namespace_name, upstream_task_name, upstream_host, upstream_type, upstream_state, upstream_external
FROM job_upstream
WHERE project_name=$1 AND job_name=$2;`

	rows, err := j.db.Query(ctx, query, projectName, jobName)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting jobs with upstreams", err)
	}
	defer rows.Close()

	var storeJobsWithUpstreams []*JobWithUpstream
	for rows.Next() {
		upstream, err := UpstreamFromRow(rows)
		if err != nil {
			return nil, err
		}
		storeJobsWithUpstreams = append(storeJobsWithUpstreams, upstream)
	}

	return j.toUpstreams(storeJobsWithUpstreams)
}

func (j JobRepository) GetDownstreamByDestination(ctx context.Context, projectName tenant.ProjectName, destination resource.URN) ([]*job.Downstream, error) {
	query := `
SELECT
	name as job_name, project_name, namespace_name, task_name
FROM job
WHERE project_name = $1 AND $2 = ANY(sources)
AND deleted_at IS NULL;`

	rows, err := j.db.Query(ctx, query, projectName, destination.String())
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job downstream", err)
	}
	defer rows.Close()

	var storeDownstream []Downstream
	for rows.Next() {
		var downstream Downstream
		err := rows.Scan(&downstream.JobName, &downstream.ProjectName, &downstream.NamespaceName, &downstream.TaskName)
		if err != nil {
			return nil, errors.Wrap(job.EntityJob, "error while getting downstream by destination", err)
		}
		storeDownstream = append(storeDownstream, downstream)
	}

	return fromStoreDownstream(storeDownstream)
}

type Downstream struct {
	JobName       string `json:"job_name"`
	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`
	TaskName      string `json:"task_name"`
}

func (j JobRepository) GetDownstreamByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.Downstream, error) {
	query := `
SELECT
	j.name as job_name, j.project_name, j.namespace_name, j.task_name
FROM job_upstream ju
JOIN job j ON (ju.job_name = j.name AND ju.project_name = j.project_name)
WHERE upstream_project_name=$1 AND upstream_job_name=$2
AND j.deleted_at IS NULL;`

	rows, err := j.db.Query(ctx, query, projectName, jobName)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job downstream by job name", err)
	}
	defer rows.Close()

	var storeDownstream []Downstream
	for rows.Next() {
		var downstream Downstream
		err := rows.Scan(&downstream.JobName, &downstream.ProjectName, &downstream.NamespaceName, &downstream.TaskName)
		if err != nil {
			return nil, errors.Wrap(job.EntityJob, "error while getting downstream by destination", err)
		}
		storeDownstream = append(storeDownstream, downstream)
	}

	return fromStoreDownstream(storeDownstream)
}

func fromStoreDownstream(storeDownstreamList []Downstream) ([]*job.Downstream, error) {
	var downstreamList []*job.Downstream
	me := errors.NewMultiError("get downstream by destination errors")
	for _, storeDownstream := range storeDownstreamList {
		downstreamJobName, err := job.NameFrom(storeDownstream.JobName)
		if err != nil {
			me.Append(err)
			continue
		}
		downstreamProjectName, err := tenant.ProjectNameFrom(storeDownstream.ProjectName)
		if err != nil {
			me.Append(err)
			continue
		}
		downstreamNamespaceName, err := tenant.NamespaceNameFrom(storeDownstream.NamespaceName)
		if err != nil {
			me.Append(err)
			continue
		}
		downstreamTaskName, err := job.TaskNameFrom(storeDownstream.TaskName)
		if err != nil {
			me.Append(err)
			continue
		}
		downstreamList = append(downstreamList, job.NewDownstream(downstreamJobName, downstreamProjectName, downstreamNamespaceName, downstreamTaskName))
	}
	return downstreamList, me.ToErr()
}

func (j JobRepository) GetDownstreamBySources(ctx context.Context, sources []resource.URN) ([]*job.Downstream, error) {
	if len(sources) == 0 {
		return nil, nil
	}

	var sourceWhereStatements []string
	for _, r := range sources {
		statement := "'" + r.String() + "' = any(sources)"
		sourceWhereStatements = append(sourceWhereStatements, statement)
	}
	sourceStatement := "(" + strings.Join(sourceWhereStatements, " or \n") + ")"

	queryBuilder := new(strings.Builder)
	queryBuilder.WriteString(`
SELECT
	name as job_name, project_name, namespace_name, task_name
FROM job
WHERE
deleted_at IS NULL and
`)
	queryBuilder.WriteString(sourceStatement)
	queryBuilder.WriteString(";\n")

	query := queryBuilder.String()

	rows, err := j.db.Query(ctx, query)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job downstream", err)
	}
	defer rows.Close()

	var storeDownstream []Downstream
	for rows.Next() {
		var downstream Downstream
		err := rows.Scan(&downstream.JobName, &downstream.ProjectName, &downstream.NamespaceName, &downstream.TaskName)
		if err != nil {
			return nil, errors.Wrap(job.EntityJob, "error while getting downstream by destination", err)
		}
		storeDownstream = append(storeDownstream, downstream)
	}

	return fromStoreDownstream(storeDownstream)
}
