package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
	"github.com/goto/optimus/internal/utils"
)

const (
	jobColumns = `id, name, version, owner, description, labels, schedule, alert, webhook, static_upstreams, http_upstreams,
				  task_name, task_config, window_spec, assets, hooks, metadata, destination, sources, project_name, namespace_name, created_at, updated_at`
	upstreamColumns = `
    job_name, project_name, upstream_job_name, upstream_project_name, upstream_host,
    upstream_namespace_name, upstream_resource_urn, upstream_task_name, upstream_type, upstream_external, upstream_state`

	thirdPartyUpstreamColumns = `job_id, job_name, project_name, upstream_third_party_type, upstream_third_party_identifier, upstream_third_party_config, created_at`

	jobSummaryColumns = `name, version, project_name, namespace_name, schedule, window_spec, alert`
)

type JobRepository struct {
	db *pgxpool.Pool
}

type Schedule struct {
	StartDate     time.Time
	EndDate       *time.Time
	Interval      string
	DependsOnPast bool
	CatchUp       bool
	Retry         *Retry
}
type Retry struct {
	Count              int   `json:"count"`
	Delay              int32 `json:"delay"`
	ExponentialBackoff bool
}

type ThirdPartyUpstream struct {
	JobID                        uuid.UUID
	JobName                      string
	ProjectName                  string
	UpstreamThirdPartyType       string
	UpstreamThirdPartyIdentifier string
	UpstreamThirdPartyConfig     map[string]string
	CreatedAt                    time.Time
}

type JobUpstreams struct {
	JobID                 uuid.UUID
	JobName               string
	ProjectName           string
	UpstreamJobID         uuid.UUID
	UpstreamJobName       sql.NullString
	UpstreamResourceUrn   sql.NullString
	UpstreamProjectName   sql.NullString
	UpstreamNamespaceName sql.NullString
	UpstreamTaskName      sql.NullString
	UpstreamHost          sql.NullString
	UpstreamType          string
	UpstreamState         string
	UpstreamExternal      sql.NullBool

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (j *JobUpstreams) toJobUpstreams() (*scheduler.JobUpstream, error) {
	t, err := tenant.NewTenant(j.UpstreamProjectName.String, j.UpstreamNamespaceName.String)
	if err != nil {
		return nil, err
	}

	var destinationURN resource.URN
	if j.UpstreamResourceUrn.String != "" {
		tmpURN, err := resource.ParseURN(j.UpstreamResourceUrn.String)
		if err != nil {
			return nil, err
		}

		destinationURN = tmpURN
	}

	return &scheduler.JobUpstream{
		JobName:        j.UpstreamJobName.String,
		Host:           j.UpstreamHost.String,
		TaskName:       j.UpstreamTaskName.String,
		DestinationURN: destinationURN,
		Tenant:         t,
		Type:           j.UpstreamType,
		External:       j.UpstreamExternal.Bool,
		State:          j.UpstreamState,
	}, nil
}

type Job struct {
	ID          uuid.UUID
	Name        string
	Version     int
	Owner       string
	Description string
	Labels      map[string]string

	Schedule   json.RawMessage
	WindowSpec json.RawMessage

	Alert   json.RawMessage
	Webhook json.RawMessage

	StaticUpstreams pq.StringArray
	HTTPUpstreams   json.RawMessage

	TaskName   string
	TaskConfig json.RawMessage

	Hooks json.RawMessage

	Assets map[string]string

	Metadata json.RawMessage

	Destination string
	Sources     pq.StringArray

	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime
}
type Window struct {
	WindowSize       string `json:",omitempty"`
	WindowShiftBy    string `json:",omitempty"`
	WindowTruncateTo string `json:",omitempty"`
	WindowLocation   string `json:",omitempty"`
	WindowOffset     string `json:",omitempty"`
	Preset           string `json:",omitempty"`
	Type             string
}

type jobSummary struct {
	Version       int
	JobName       string
	ProjectName   string
	NamespaceName string
	Schedule      json.RawMessage
	WindowSpec    json.RawMessage
	Alert         json.RawMessage
}

func fromJobSummaryRow(row pgx.Row) (*jobSummary, error) {
	var js jobSummary

	err := row.Scan(&js.JobName, &js.Version, &js.ProjectName, &js.NamespaceName, &js.Schedule, &js.WindowSpec, &js.Alert)
	if err != nil {
		return nil, err
	}

	return &js, nil
}

func (j *jobSummary) toJobSummary() (*scheduler.JobSummary, error) {
	tnnt, err := tenant.NewTenant(j.ProjectName, j.NamespaceName)
	if err != nil {
		return nil, err
	}

	var schedule Schedule
	if err := json.Unmarshal(j.Schedule, &schedule); err != nil {
		return nil, err
	}

	var windowConfig window.Config
	if j.WindowSpec != nil {
		windowConfig, err = fromStorageWindow(j.WindowSpec, j.Version)
		if err != nil {
			return nil, err
		}
	}

	slaDuration := time.Duration(0)
	if j.Alert != nil {
		var alerts []scheduler.Alert
		if err := json.Unmarshal(j.Alert, &alerts); err != nil {
			return nil, err
		}
		duration, _ := scheduler.GetSLADuration(alerts)
		slaDuration = time.Duration(duration) * time.Second
	}

	jobSummary := &scheduler.JobSummary{
		JobName:          scheduler.JobName(j.JobName),
		Tenant:           tnnt,
		ScheduleInterval: schedule.Interval,
		Window:           windowConfig,
		SLA: scheduler.SLAConfig{
			Duration: slaDuration,
		},
	}
	return jobSummary, nil
}

func fromStorageWindow(raw []byte, jobVersion int) (window.Config, error) {
	var storageWindow Window
	if err := json.Unmarshal(raw, &storageWindow); err != nil {
		return window.Config{}, err
	}

	if storageWindow.Type == string(window.Preset) {
		return window.NewPresetConfig(storageWindow.Preset)
	}

	if storageWindow.Type == string(window.Incremental) {
		return window.NewIncrementalConfig(), nil
	}

	if jobVersion == window.NewWindowVersion {
		sc := window.SimpleConfig{
			Size:       storageWindow.WindowSize,
			ShiftBy:    storageWindow.WindowShiftBy,
			Location:   storageWindow.WindowLocation,
			TruncateTo: storageWindow.WindowTruncateTo,
		}
		return window.NewCustomConfigWithTimezone(sc), nil
	}

	w, err := models.NewWindow(
		jobVersion,
		storageWindow.WindowTruncateTo,
		storageWindow.WindowOffset,
		storageWindow.WindowSize,
	)
	if err != nil {
		return window.Config{}, err
	}

	return window.NewCustomConfig(w), nil
}

type change struct {
	Property string `json:"attribute_name"`
	Diff     string `json:"diff"`
}

type changelog struct {
	Change []change
	Type   string
	Time   time.Time
}

func fromStorageChangelog(changelog *changelog) *scheduler.Changelog {
	jobChangeLog := scheduler.Changelog{
		Type:      changelog.Type,
		CreatedAt: changelog.Time,
	}

	jobChangeLog.Change = make([]scheduler.Change, len(changelog.Change))
	for i, change := range changelog.Change {
		jobChangeLog.Change[i].Property = change.Property
		jobChangeLog.Change[i].Diff = scheduler.ChangeDiff(change.Diff)
	}
	return &jobChangeLog
}

func FromChangelogRow(row pgx.Row) (*changelog, error) {
	var cl changelog
	err := row.Scan(&cl.Change, &cl.Type, &cl.Time)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(job.EntityJobChangeLog, "changelog not found")
		}
		return nil, errors.Wrap(job.EntityJobChangeLog, "error in reading row for changelog", err)
	}
	return &cl, nil
}

type Metadata struct {
	Resource  *MetadataResource
	Scheduler map[string]string
}

type MetadataResource struct {
	Request *MetadataResourceConfig
	Limit   *MetadataResourceConfig
}

type MetadataResourceConfig struct {
	CPU    string
	Memory string
}

func fromStorageMetadata(metadata json.RawMessage) (scheduler.RuntimeConfig, error) {
	if metadata == nil {
		return scheduler.RuntimeConfig{}, nil
	}
	var storeMetadata Metadata
	if err := json.Unmarshal(metadata, &storeMetadata); err != nil {
		return scheduler.RuntimeConfig{}, err
	}
	var runtimeConfig scheduler.RuntimeConfig
	if storeMetadata.Resource != nil {
		var resourceRequest *scheduler.ResourceConfig
		if storeMetadata.Resource.Request != nil {
			resourceRequest = &scheduler.ResourceConfig{
				CPU:    storeMetadata.Resource.Request.CPU,
				Memory: storeMetadata.Resource.Request.Memory,
			}
		}
		var resourceLimit *scheduler.ResourceConfig
		if storeMetadata.Resource.Limit != nil {
			resourceLimit = &scheduler.ResourceConfig{
				CPU:    storeMetadata.Resource.Limit.CPU,
				Memory: storeMetadata.Resource.Limit.Memory,
			}
		}
		runtimeConfig.Resource = &scheduler.Resource{
			Request: resourceRequest,
			Limit:   resourceLimit,
		}
	}
	if storeMetadata.Scheduler != nil {
		runtimeConfig.Scheduler = storeMetadata.Scheduler
	}
	return runtimeConfig, nil
}

func (j *Job) toJob() (*scheduler.Job, error) {
	t, err := tenant.NewTenant(j.ProjectName, j.NamespaceName)
	if err != nil {
		return nil, err
	}
	var w window.Config
	if j.WindowSpec != nil {
		w, err = fromStorageWindow(j.WindowSpec, j.Version)
		if err != nil {
			return nil, err
		}
	}
	var destination resource.URN
	if j.Destination != "" {
		tempURN, err := resource.ParseURN(j.Destination)
		if err != nil {
			return nil, err
		}
		destination = tempURN
	}
	schedulerJob := scheduler.Job{
		ID:           j.ID,
		Name:         scheduler.JobName(j.Name),
		Tenant:       t,
		Destination:  destination,
		WindowConfig: w,
		Assets:       j.Assets,
		Task: &scheduler.Task{
			Name: j.TaskName,
		},
	}

	if j.TaskConfig != nil {
		var taskConf *scheduler.Task
		if err := json.Unmarshal(j.TaskConfig, &taskConf); err != nil {
			config := map[string]string{}
			if err2 := json.Unmarshal(j.TaskConfig, &config); err2 == nil {
				taskConf.Config = config
			}
		}
		schedulerJob.Task.Config = taskConf.Config
		schedulerJob.Task.Version = taskConf.Version
		schedulerJob.Task.AlertConfig = taskConf.AlertConfig
	}

	if j.Hooks != nil {
		var hookConfig []*scheduler.Hook
		if err := json.Unmarshal(j.Hooks, &hookConfig); err != nil {
			return nil, err
		}
		schedulerJob.Hooks = hookConfig
	}

	return &schedulerJob, nil
}

func (j *Job) toJobWithDetails() (*scheduler.JobWithDetails, error) {
	job, err := j.toJob()
	if err != nil {
		return nil, err
	}
	var storageSchedule Schedule
	if err := json.Unmarshal(j.Schedule, &storageSchedule); err != nil {
		return nil, err
	}

	runtimeConfig, err := fromStorageMetadata(j.Metadata)
	if err != nil {
		return nil, err
	}

	schedulerJobWithDetails := &scheduler.JobWithDetails{
		Name: job.Name,
		Job:  job,
		JobMetadata: &scheduler.JobMetadata{
			Version:     j.Version,
			Owner:       j.Owner,
			Description: j.Description,
			Labels:      j.Labels,
		},
		Schedule: &scheduler.Schedule{
			DependsOnPast: storageSchedule.DependsOnPast,
			CatchUp:       storageSchedule.CatchUp,
			StartDate:     storageSchedule.StartDate,
			Interval:      storageSchedule.Interval,
		},
		RuntimeConfig: runtimeConfig,
	}
	if storageSchedule.EndDate != nil && !storageSchedule.EndDate.IsZero() {
		schedulerJobWithDetails.Schedule.EndDate = storageSchedule.EndDate
	}

	if storageSchedule.Retry != nil {
		schedulerJobWithDetails.Retry = scheduler.Retry{
			ExponentialBackoff: storageSchedule.Retry.ExponentialBackoff,
			Count:              storageSchedule.Retry.Count,
			Delay:              storageSchedule.Retry.Delay,
		}
	}

	if j.Alert != nil {
		var alerts []scheduler.Alert
		if err := json.Unmarshal(j.Alert, &alerts); err != nil {
			return nil, err
		}
		schedulerJobWithDetails.Alerts = alerts
	}
	if j.Webhook != nil {
		var webhook []scheduler.Webhook
		if err := json.Unmarshal(j.Webhook, &webhook); err != nil {
			return nil, err
		}
		schedulerJobWithDetails.Webhook = webhook
	}

	return schedulerJobWithDetails, nil
}

func FromRow(row pgx.Row) (*Job, error) {
	var js Job

	err := row.Scan(&js.ID, &js.Name, &js.Version, &js.Owner, &js.Description,
		&js.Labels, &js.Schedule, &js.Alert, &js.Webhook, &js.StaticUpstreams, &js.HTTPUpstreams,
		&js.TaskName, &js.TaskConfig, &js.WindowSpec, &js.Assets, &js.Hooks, &js.Metadata, &js.Destination, &js.Sources,
		&js.ProjectName, &js.NamespaceName, &js.CreatedAt, &js.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(job.EntityJob, "job not found")
		}

		return nil, errors.Wrap(scheduler.EntityJobRun, "error in reading row for job", err)
	}

	return &js, nil
}

func (j *JobRepository) GetJob(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error) {
	getJobByNameAtProject := `SELECT ` + jobColumns + ` FROM job WHERE name = $1 AND project_name = $2 AND deleted_at IS NULL`
	spec, err := FromRow(j.db.QueryRow(ctx, getJobByNameAtProject, jobName, projectName))
	if err != nil {
		return nil, err
	}
	return spec.toJob()
}

func (j *JobRepository) GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error) {
	getJobByNameAtProject := `SELECT ` + jobColumns + ` FROM job WHERE name = $1 AND project_name = $2 AND deleted_at IS NULL`
	spec, err := FromRow(j.db.QueryRow(ctx, getJobByNameAtProject, jobName, projectName))
	if err != nil {
		return nil, err
	}
	return spec.toJobWithDetails()
}

func groupUpstreamsByJobName(jobUpstreams []*JobUpstreams) (map[string][]*scheduler.JobUpstream, error) {
	multiError := errors.NewMultiError("errorsInGroupUpstreamsByJobName")
	jobUpstreamGroup := map[string][]*scheduler.JobUpstream{}

	for _, upstream := range jobUpstreams {
		if upstream.UpstreamState != "resolved" {
			if strings.EqualFold(upstream.UpstreamType, "static") {
				multiError.Append(errors.NewError(errors.ErrInvalidState, scheduler.EntityJobRun, "unresolved upstream "+upstream.UpstreamJobName.String+" for "+upstream.JobName))
			}
			continue
		}
		schedulerUpstream, err := upstream.toJobUpstreams()
		if err != nil {
			msg := fmt.Sprintf("unable to parse upstream:%s for job:%s", upstream.UpstreamJobName.String, upstream.JobName)
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, msg, err))
			continue
		}
		jobUpstreamGroup[upstream.JobName] = append(jobUpstreamGroup[upstream.JobName], schedulerUpstream)
	}
	return jobUpstreamGroup, multiError.ToErr()
}

func (j *JobRepository) getJobsUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []string) (map[string][]*scheduler.JobUpstream, error) {
	getJobUpstreamsByNameAtProject := "SELECT " + upstreamColumns + " FROM job_upstream WHERE project_name = $1 and job_name = any ($2)"
	rows, err := j.db.Query(ctx, getJobUpstreamsByNameAtProject, projectName, jobNames)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job with upstreams", err)
	}
	defer rows.Close()

	var upstreams []*JobUpstreams
	for rows.Next() {
		var jwu JobUpstreams
		err := rows.Scan(&jwu.JobName, &jwu.ProjectName, &jwu.UpstreamJobName, &jwu.UpstreamProjectName, &jwu.UpstreamHost,
			&jwu.UpstreamNamespaceName, &jwu.UpstreamResourceUrn, &jwu.UpstreamTaskName, &jwu.UpstreamType, &jwu.UpstreamExternal, &jwu.UpstreamState)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NotFound(scheduler.EntityJobRun, "job upstream not found")
			}

			return nil, errors.Wrap(scheduler.EntityJobRun, "error in reading row for resource", err)
		}
		upstreams = append(upstreams, &jwu)
	}

	return groupUpstreamsByJobName(upstreams)
}

const (
	urnSeparator       = "://"
	urnComponentLength = 2
)

func GetStore(urn string) (string, error) {
	splitURN := strings.Split(urn, urnSeparator)
	if len(splitURN) != urnComponentLength {
		return "", fmt.Errorf("urn does not follow pattern <store>%s<name>", urnSeparator)
	}

	store := splitURN[0]
	name := splitURN[1]

	if store == "" {
		return "", fmt.Errorf("urn store is not specified")
	}

	if name == "" {
		return "", fmt.Errorf("urn name is not specified")
	}

	if trimmedStore := strings.TrimSpace(store); len(trimmedStore) != len(store) {
		return "", fmt.Errorf("urn store does not match urn name")
	}

	if trimmedName := strings.TrimSpace(name); len(trimmedName) != len(name) {
		return "", fmt.Errorf("urn name contains whitespace")
	}

	return store, nil
}

func toSchedulerThirdPartyUpstream(jwu ThirdPartyUpstream) (*scheduler.ThirdPartyUpstream, error) {
	if jwu.UpstreamThirdPartyType == job.ThirdPartyTypeDex {
		resourceUrn := jwu.UpstreamThirdPartyConfig["resource_urn"]
		store, err := GetStore(resourceUrn)
		if err != nil {
			return nil, err
		}
		jwu.UpstreamThirdPartyConfig["store"] = store
	}
	return &scheduler.ThirdPartyUpstream{
		Type:       jwu.UpstreamThirdPartyType,
		Identifier: jwu.UpstreamThirdPartyIdentifier,
		Config:     jwu.UpstreamThirdPartyConfig,
	}, nil
}

func (j *JobRepository) getThirdPartyUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []string) (map[string][]*scheduler.ThirdPartyUpstream, error) {
	getJobUpstreamsByNameAtProject := "SELECT " + thirdPartyUpstreamColumns + " FROM job_third_party_upstream WHERE project_name = $1 and job_name = any ($2)"
	rows, err := j.db.Query(ctx, getJobUpstreamsByNameAtProject, projectName, jobNames)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job with third party upstreams", err)
	}
	defer rows.Close()

	result := make(map[string][]*scheduler.ThirdPartyUpstream)
	for rows.Next() {
		var jwu ThirdPartyUpstream
		err := rows.Scan(&jwu.JobID, &jwu.JobName, &jwu.ProjectName, &jwu.UpstreamThirdPartyType, &jwu.UpstreamThirdPartyIdentifier, &jwu.UpstreamThirdPartyConfig, &jwu.CreatedAt)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NotFound(scheduler.EntityJobRun, "job third party upstream not found")
			}

			return nil, errors.Wrap(scheduler.EntityJobRun, "error in reading row for resource", err)
		}
		if _, ok := result[jwu.JobName]; !ok {
			result[jwu.JobName] = []*scheduler.ThirdPartyUpstream{}
		}
		schedulerThirdPartyUpstream, err := toSchedulerThirdPartyUpstream(jwu)
		if err != nil {
			return nil, errors.InternalError(scheduler.EntityJobRun, "job third party upstream could not be parsed", err)
		}
		result[jwu.JobName] = append(result[jwu.JobName], schedulerThirdPartyUpstream)
	}

	return result, nil
}

func (j *JobRepository) GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.JobWithDetails, error) {
	getJobByNameAtProject := `SELECT ` + jobColumns + ` FROM job WHERE project_name = $1 AND deleted_at IS NULL`
	rows, err := j.db.Query(ctx, getJobByNameAtProject, projectName)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting all jobs", err)
	}
	defer rows.Close()

	jobsMap := map[string]*scheduler.JobWithDetails{}
	var jobNameList []string
	multiError := errors.NewMultiError("errorInGetAll")
	for rows.Next() {
		spec, err := FromRow(rows)
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error parsing job:"+spec.Name, err))
			continue
		}

		job, err := spec.toJobWithDetails()
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error parsing job:"+spec.Name, err))
			continue
		}
		jobNameList = append(jobNameList, job.GetName())
		jobsMap[job.GetName()] = job
	}
	if len(jobNameList) == 0 {
		return nil, errors.NotFound(scheduler.EntityJobRun, "unable to find jobs in project:"+projectName.String())
	}

	jobUpstreamGroupedByName, err := j.getJobsUpstreams(ctx, projectName, jobNameList)
	multiError.Append(err)

	thirdPartyUpstreamGroupedByName, err := j.getThirdPartyUpstreams(ctx, projectName, jobNameList)
	multiError.Append(err)

	for jobName := range jobsMap {
		if upstreamList, ok := jobUpstreamGroupedByName[jobName]; ok {
			jobsMap[jobName].Upstreams.UpstreamJobs = upstreamList
		}
		if thirdPartyList, ok := thirdPartyUpstreamGroupedByName[jobName]; ok {
			jobsMap[jobName].Upstreams.ThirdParty = thirdPartyList
		}
	}

	return utils.MapToList[*scheduler.JobWithDetails](jobsMap), multiError.ToErr()
}

func (j *JobRepository) GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error) {
	getJobByNames := `SELECT ` + jobColumns + ` FROM job WHERE project_name = $1 AND name = any ($2) AND deleted_at IS NULL`
	rows, err := j.db.Query(ctx, getJobByNames, projectName, jobs)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting selected jobs", err)
	}
	defer rows.Close()

	jobsMap := map[string]*scheduler.JobWithDetails{}
	var jobNameList []string
	multiError := errors.NewMultiError("errorInGetJobs")
	for rows.Next() {
		spec, err := FromRow(rows)
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error parsing job:"+spec.Name, err))
			continue
		}

		job, err := spec.toJobWithDetails()
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error parsing job:"+spec.Name, err))
			continue
		}
		jobNameList = append(jobNameList, job.GetName())
		jobsMap[job.GetName()] = job
	}
	for _, jobName := range jobs {
		if _, ok := jobsMap[jobName]; !ok {
			multiError.Append(errors.NotFound(scheduler.EntityJobRun, "unable to find job "+jobName))
		}
	}

	jobUpstreamGroupedByName, err := j.getJobsUpstreams(ctx, projectName, jobNameList)
	multiError.Append(err)

	thirdPartyUpstreamGroupedByName, err := j.getThirdPartyUpstreams(ctx, projectName, jobNameList)
	multiError.Append(err)

	for jobName := range jobsMap {
		if upstreamList, ok := jobUpstreamGroupedByName[jobName]; ok {
			jobsMap[jobName].Upstreams.UpstreamJobs = upstreamList
		}
		if thirdPartyList, ok := thirdPartyUpstreamGroupedByName[jobName]; ok {
			jobsMap[jobName].Upstreams.ThirdParty = thirdPartyList
		}
	}

	return utils.MapToList[*scheduler.JobWithDetails](jobsMap), errors.MultiToError(multiError)
}

func (j *JobRepository) GetJobsByLabels(ctx context.Context, projectName tenant.ProjectName, labels map[string]string) ([]*scheduler.JobWithDetails, error) {
	if len(labels) == 0 {
		return []*scheduler.JobWithDetails{}, nil
	}

	// Convert labels map into a JSON string for the @> operator
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "failed to marshal labels", err)
	}

	query := `SELECT ` + jobColumns + `
		FROM job
		WHERE project_name = $1
		AND deleted_at IS NULL
		AND labels @> $2::jsonb`

	rows, err := j.db.Query(ctx, query, projectName, string(labelsJSON))
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while querying jobs by labels", err)
	}
	defer rows.Close()

	jobsMap := map[string]*scheduler.JobWithDetails{}
	var jobNameList []string
	multiError := errors.NewMultiError("errorInGetJobsByLabels")

	for rows.Next() {
		spec, err := FromRow(rows)
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error parsing job:"+spec.Name, err))
			continue
		}

		job, err := spec.toJobWithDetails()
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error parsing job:"+spec.Name, err))
			continue
		}

		jobNameList = append(jobNameList, job.GetName())
		jobsMap[job.GetName()] = job
	}

	// Load upstreams for the filtered jobs
	if len(jobNameList) > 0 {
		jobUpstreamGroupedByName, err := j.getJobsUpstreams(ctx, projectName, jobNameList)
		multiError.Append(err)

		thirdPartyUpstreamGroupedByName, err := j.getThirdPartyUpstreams(ctx, projectName, jobNameList)
		multiError.Append(err)

		for jobName := range jobsMap {
			if upstreamList, ok := jobUpstreamGroupedByName[jobName]; ok {
				jobsMap[jobName].Upstreams.UpstreamJobs = upstreamList
			}
			if thirdPartyList, ok := thirdPartyUpstreamGroupedByName[jobName]; ok {
				jobsMap[jobName].Upstreams.ThirdParty = thirdPartyList
			}
		}
	}

	return utils.MapToList[*scheduler.JobWithDetails](jobsMap), errors.MultiToError(multiError)
}

func (j *JobRepository) GetAllResolvedUpstreams(ctx context.Context) (map[scheduler.JobName][]scheduler.JobName, error) {
	query := `SELECT job_name, upstream_job_name 
				FROM job_upstream 
				WHERE upstream_state = 'resolved'`

	rows, err := j.db.Query(ctx, query)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting resolved upstreams", err)
	}
	defer rows.Close()

	upstreamMap := make(map[scheduler.JobName][]scheduler.JobName)

	for rows.Next() {
		var jobName, upstreamJobName string
		err := rows.Scan(&jobName, &upstreamJobName)
		if err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "error scanning upstream row", err)
		}

		jobKey := scheduler.JobName(jobName)
		upstreamJob := scheduler.JobName(upstreamJobName)
		upstreamMap[jobKey] = append(upstreamMap[jobKey], upstreamJob)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error iterating upstream rows", err)
	}

	return upstreamMap, nil
}

func (j *JobRepository) GetSummaryByNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*scheduler.JobSummary, error) {
	if len(jobNames) == 0 {
		return make(map[scheduler.JobName]*scheduler.JobSummary), nil
	}

	jobNameStrings := make([]string, len(jobNames))
	for i, jobName := range jobNames {
		jobNameStrings[i] = string(jobName)
	}

	query := `SELECT ` + jobSummaryColumns + ` FROM job WHERE name = any($1) AND deleted_at IS NULL AND state = 'enabled'`
	rows, err := j.db.Query(ctx, query, jobNameStrings)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while finding jobs by names", err)
	}
	defer rows.Close()

	jobsMap := make(map[scheduler.JobName]*scheduler.JobSummary)
	multiError := errors.NewMultiError("errorInFindByNames")

	for rows.Next() {
		spec, err := fromJobSummaryRow(rows)
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error parsing job summary", err))
			continue
		}

		jobSummary, err := spec.toJobSummary()
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error converting to job summary", err))
			continue
		}

		jobsMap[jobSummary.JobName] = jobSummary
	}

	return jobsMap, multiError.ToErr()
}

func (j *JobRepository) GetChangelogs(ctx context.Context, filter scheduler.ChangelogFilter) ([]*scheduler.Changelog, error) {
	me := errors.NewMultiError("get changelog errors")

	query := `
	SELECT
		changes,
		change_type,
		created_at
	FROM changelog
	WHERE
		project_name = $1 AND name = $2 AND entity_type = $3`

	args := []any{filter.ProjectName, filter.Name, "job"}

	if !filter.StartTime.IsZero() {
		query += " AND created_at >= $" + fmt.Sprintf("%d", len(args)+1)
		args = append(args, filter.StartTime)
	}

	if !filter.EndTime.IsZero() {
		query += " AND created_at <= $" + fmt.Sprintf("%d", len(args)+1)
		args = append(args, filter.EndTime)
	}

	query += " ORDER BY created_at DESC"

	rows, err := j.db.Query(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting changeLog for job", err)
	}
	defer rows.Close()

	var changeLog []*scheduler.Changelog
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

func NewJobProviderRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{
		db: pool,
	}
}
