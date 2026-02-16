package airflow

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goto/salt/log"
	"github.com/kushsharma/parallel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
)

//go:embed __lib.py
var SharedLib []byte

const (
	EntityAirflow = "Airflow"

	rolesURL           = "auth/fab/v1/roles"
	rolesPermissionURL = "auth/fab/v1/roles/%s"
	dagStatusBatchURL  = "api/v1/dags/~/dagRuns/list"
	dagURL             = "api/v1/dags"
	dagRunClearURL     = "api/v1/dags/%s/clearTaskInstances"
	dagRunCreateURL    = "api/v1/dags/%s/dagRuns"
	getTaskInstanceURL = "api/v1/dags/%s/dagRuns/%s/taskInstances/%s"
	dagRunModifyURL    = "api/v1/dags/%s/dagRuns/%s"
	airflowDateFormat  = "2006-01-02T15:04:05+00:00"

	schedulerHostKey = "SCHEDULER_HOST"

	baseLibFileName = "__lib.py"
	jobsDir         = "dags"
	jobsExtension   = ".py"

	concurrentTicketPerSec = 50
	concurrentLimit        = 100

	metricJobStateSuccess = "success"
	metricJobStateFailed  = "failed"
)

type DAGs struct {
	DAGS         []DAGObj `json:"dags"`
	TotalEntries int      `json:"total_entries"`
}

type DAGObj struct {
	DAGDisplayName              string   `json:"dag_display_name"`
	DAGID                       string   `json:"dag_id"`
	DefaultView                 string   `json:"default_view"`
	Description                 *string  `json:"description"`
	FileToken                   string   `json:"file_token"`
	Fileloc                     string   `json:"fileloc"`
	HasImportErrors             bool     `json:"has_import_errors"`
	HasTaskConcurrencyLimits    bool     `json:"has_task_concurrency_limits"`
	IsActive                    bool     `json:"is_active"`
	IsPaused                    bool     `json:"is_paused"`
	IsSubdag                    bool     `json:"is_subdag"`
	LastExpired                 *string  `json:"last_expired"`
	LastParsedTime              string   `json:"last_parsed_time"`
	LastPickled                 *string  `json:"last_pickled"`
	MaxActiveRuns               int      `json:"max_active_runs"`
	MaxActiveTasks              int      `json:"max_active_tasks"`
	MaxConsecutiveFailedDAGRuns int      `json:"max_consecutive_failed_dag_runs"`
	NextDagRun                  string   `json:"next_dagrun"`
	NextDagRunCreateAfter       string   `json:"next_dagrun_create_after"`
	NextDagRunDataIntervalEnd   string   `json:"next_dagrun_data_interval_end"`
	NextDagRunDataIntervalStart string   `json:"next_dagrun_data_interval_start"`
	Owners                      []string `json:"owners"`
	PickleID                    *string  `json:"pickle_id"`
	RootDagID                   *string  `json:"root_dag_id"`
	ScheduleInterval            Schedule `json:"schedule_interval"`
	SchedulerLock               *string  `json:"scheduler_lock"`
	Tags                        []Tag    `json:"tags"`
	TimetableDescription        string   `json:"timetable_description"`
}

type TaskInstance struct {
	TaskID          string   `json:"task_id"`
	TaskDisplayName string   `json:"task_display_name"`
	DagID           string   `json:"dag_id"`
	DagRunID        string   `json:"dag_run_id"`
	ExecutionDate   string   `json:"execution_date"`
	StartDate       string   `json:"start_date"`
	EndDate         string   `json:"end_date"`
	Duration        *float64 `json:"duration"`
	State           *string  `json:"state"` // nullable
	TryNumber       int      `json:"try_number"`
	MapIndex        int      `json:"map_index"`
	MaxTries        int      `json:"max_tries"`
	Hostname        string   `json:"hostname"`
	Pool            string   `json:"pool"`
	PoolSlots       int      `json:"pool_slots"`
	Queue           string   `json:"queue"`
	PriorityWeight  int      `json:"priority_weight"`
	Operator        string   `json:"operator"`
	QueuedWhen      string   `json:"queued_when"`
	Pid             int      `json:"pid"`
	Note            string   `json:"note"`
}

type ClearTaskInstancesResponse struct {
	TaskInstances []TaskInstance `json:"task_instances"`
	TotalEntries  int            `json:"total_entries"`
}

func unmarshalAs[T any](data []byte) (*T, error) {
	var ti T
	if err := json.Unmarshal(data, &ti); err != nil {
		return nil, fmt.Errorf("failed to unmarshal err: %w", err)
	}
	return &ti, nil
}

func (ti *TaskInstance) toSchedulerOperatorRunInstance() (*scheduler.OperatorRunInstance, error) {
	startTime, err := time.Parse(time.RFC3339, ti.StartDate)
	if err != nil {
		return nil, err
	}
	var endTime *time.Time
	if ti.EndDate != "" {
		parsedEndTime, err := time.Parse(time.RFC3339, ti.EndDate)
		if err != nil {
			return nil, err
		}
		endTime = &parsedEndTime
	}
	opr := &scheduler.OperatorRunInstance{
		MaxTries:     ti.MaxTries,
		OperatorName: ti.TaskID,
		StartTime:    startTime,
		OperatorKey:  ti.TaskDisplayName,
		TryNumber:    ti.TryNumber,
		EndTime:      endTime,
		LogURL:       "",
	}
	if ti.State != nil {
		opr.State = *ti.State
	}
	return opr, nil
}

type Schedule struct {
	Type  string `json:"__type"`
	Value string `json:"value"`
}

type Tag struct {
	Name string `json:"name"`
}

type PermissionSet struct {
	Name    string   `json:"name"`
	Actions []Action `json:"actions"`
}

type Action struct {
	Action   ActionDetail   `json:"action"`
	Resource ResourceDetail `json:"resource"`
}

func (a Action) String() string {
	return fmt.Sprintf("%s on %s", a.Action.Name, a.Resource.Name)
}

type ActionDetail struct {
	Name string `json:"name"`
}

type ResourceDetail struct {
	Name string `json:"name"`
}

var jobUploadMetric = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "job_upload_total",
}, []string{"project", "namespace", "status"})

var jobRemovalMetric = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "job_removal_total",
}, []string{"project", "namespace", "status"})

type Bucket interface {
	WriteAll(ctx context.Context, key string, p []byte, opts *blob.WriterOptions) error
	List(opts *blob.ListOptions) *blob.ListIterator
	Delete(ctx context.Context, key string) error
	Close() error
}

type BucketFactory interface {
	New(ctx context.Context, tenant tenant.Tenant) (Bucket, error)
}

type DagCompiler interface {
	Compile(project *tenant.Project, job *scheduler.JobWithDetails) ([]byte, error)
}

type Client interface {
	Invoke(ctx context.Context, r airflowRequest, auth SchedulerAuth) ([]byte, error)
}

type SecretGetter interface {
	Get(ctx context.Context, projName tenant.ProjectName, namespaceName, name string) (*tenant.PlainTextSecret, error)
}

type ProjectGetter interface {
	Get(context.Context, tenant.ProjectName) (*tenant.Project, error)
}

type Scheduler struct {
	l         log.Logger
	bucketFac BucketFactory
	client    Client
	compiler  DagCompiler

	projectGetter ProjectGetter
	secretGetter  SecretGetter

	hasLibSyncedMap sync.Map
}

func (s *Scheduler) DeployJobs(ctx context.Context, tenant tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	spanCtx, span := startChildSpan(ctx, "DeployJobs")
	defer span.End()

	bucket, err := s.bucketFac.New(spanCtx, tenant)
	if err != nil {
		return errors.AddErrContext(err, EntityAirflow, "error in creating storage client instance")
	}
	defer bucket.Close()

	err = s.uploadCommonLib(ctx, tenant, bucket)
	if err != nil {
		s.l.Error("failed to upload __lib.py file:", err)
		return errors.AddErrContext(err, EntityAirflow, "error in writing __lib.py file")
	}

	multiError := errors.NewMultiError("ErrorsInDeployJobs")
	runner := parallel.NewRunner(parallel.WithTicket(concurrentTicketPerSec), parallel.WithLimit(concurrentLimit))
	project, err := s.projectGetter.Get(ctx, tenant.ProjectName())
	if err != nil {
		s.l.Error("failed fetch project details")
		return errors.AddErrContext(err, EntityAirflow, "error in getting project details")
	}
	for _, job := range jobs {
		runner.Add(func(currentJob *scheduler.JobWithDetails) func() (interface{}, error) {
			return func() (interface{}, error) {
				return nil, s.compileAndUpload(ctx, project, currentJob, bucket)
			}
		}(job))
	}

	countDeploySucceed := 0
	countDeployFailed := 0
	for _, result := range runner.Run() {
		if result.Err != nil {
			countDeployFailed++
			multiError.Append(result.Err)
			continue
		}
		countDeploySucceed++
	}

	projectName := project.Name().String()
	namespace := tenant.NamespaceName().String()
	jobUploadMetric.WithLabelValues(projectName, namespace, metricJobStateSuccess).Add(float64(countDeploySucceed))
	jobUploadMetric.WithLabelValues(projectName, namespace, metricJobStateFailed).Add(float64(countDeployFailed))

	return multiError.ToErr()
}

// uploadCommonLib is a function to upload the shared lib file to the bucket, encapsulated in a sync process.
// we need to ensure that the file is uploaded only once, not on every job upload.
func (s *Scheduler) uploadCommonLib(ctx context.Context, tenant tenant.Tenant, bucket Bucket) error {
	if _, isSynced := s.hasLibSyncedMap.LoadOrStore(tenant.ProjectName().String(), true); isSynced {
		return nil
	}

	err := bucket.WriteAll(ctx, filepath.Join(jobsDir, baseLibFileName), SharedLib, nil)
	if err != nil {
		// on failure, flag is removed so that the upload is retried in the next job upload
		s.hasLibSyncedMap.Delete(tenant.ProjectName().String())
		return err
	}

	return nil
}

// TODO list jobs should not refer from the scheduler, rather should list from db and it has nothing to do with scheduler.
func (s *Scheduler) ListJobs(ctx context.Context, t tenant.Tenant) ([]string, error) {
	spanCtx, span := startChildSpan(ctx, "ListJobs")
	defer span.End()

	bucket, err := s.bucketFac.New(spanCtx, t)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	var jobNames []string
	// get all items under namespace directory
	it := bucket.List(&blob.ListOptions{
		Prefix: pathForJobDirectory(jobsDir, t.NamespaceName().String()),
	})
	for {
		obj, err := it.Next(spanCtx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if strings.HasSuffix(obj.Key, jobsExtension) {
			jobNames = append(jobNames, jobNameFromPath(obj.Key, jobsExtension))
		}
	}
	return jobNames, nil
}

func (s *Scheduler) DeleteJobs(ctx context.Context, t tenant.Tenant, jobNames []string) error {
	spanCtx, span := startChildSpan(ctx, "DeleteJobs")
	defer span.End()

	bucket, err := s.bucketFac.New(spanCtx, t)
	if err != nil {
		return err
	}
	me := errors.NewMultiError("ErrorsInDeleteJobs")
	countDeleteJobsSucceed := 0
	countDeleteJobsFailed := 0
	for _, jobName := range jobNames {
		if strings.TrimSpace(jobName) == "" {
			me.Append(errors.InvalidArgument(EntityAirflow, "job name cannot be an empty string"))
			continue
		}
		blobKey := pathFromJobName(jobsDir, t.NamespaceName().String(), jobName, jobsExtension)
		if err := bucket.Delete(spanCtx, blobKey); err != nil {
			// ignore missing files
			if gcerrors.Code(err) != gcerrors.NotFound {
				countDeleteJobsFailed++
				me.Append(err)
			}
			continue
		}
		countDeleteJobsSucceed++
	}

	projectName := t.ProjectName().String()
	namespace := t.NamespaceName().String()
	jobRemovalMetric.WithLabelValues(projectName, namespace, metricJobStateSuccess).Add(float64(countDeleteJobsSucceed))
	jobRemovalMetric.WithLabelValues(projectName, namespace, metricJobStateFailed).Add(float64(countDeleteJobsFailed))

	err = deleteDirectoryIfEmpty(ctx, t.NamespaceName().String(), bucket)
	if err != nil {
		if gcerrors.Code(err) != gcerrors.NotFound {
			me.Append(err)
		}
	}
	return me.ToErr()
}

// deleteDirectoryIfEmpty remove jobs Folder if it exists
func deleteDirectoryIfEmpty(ctx context.Context, nsDirectoryIdentifier string, bucket Bucket) error {
	spanCtx, span := startChildSpan(ctx, "deleteDirectoryIfEmpty")
	defer span.End()

	jobsDir := pathForJobDirectory(jobsDir, nsDirectoryIdentifier)

	it := bucket.List(&blob.ListOptions{
		Prefix: jobsDir,
	})
	_, err := it.Next(spanCtx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return bucket.Delete(ctx, jobsDir)
		}
	}
	return nil
}

func (s *Scheduler) compileAndUpload(ctx context.Context, project *tenant.Project, job *scheduler.JobWithDetails, bucket Bucket) error {
	namespaceName := job.Job.Tenant.NamespaceName().String()
	blobKey := pathFromJobName(jobsDir, namespaceName, job.Name.String(), jobsExtension)

	compiledJob, err := s.compiler.Compile(project, job)
	if err != nil {
		s.l.Error(fmt.Sprintf("failed compilation %s:%s, err:%s", namespaceName, blobKey, err.Error()))
		return errors.AddErrContext(err, EntityAirflow, "job:"+job.Name.String())
	}
	if err := bucket.WriteAll(ctx, blobKey, compiledJob, nil); err != nil {
		s.l.Error(fmt.Sprintf("failed to upload %s:%s, err:%s", namespaceName, blobKey, err.Error()))
		return errors.AddErrContext(err, EntityAirflow, "job: "+job.Name.String())
	}
	return nil
}

func pathFromJobName(prefix, namespace, jobName, suffix string) string {
	if len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	return fmt.Sprintf("%s%s", path.Join(prefix, namespace, jobName), suffix)
}

func pathForJobDirectory(prefix, namespace string) string {
	if len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	return path.Join(prefix, namespace)
}

func jobNameFromPath(filePath, suffix string) string {
	jobFileName := path.Base(filePath)
	return strings.TrimSuffix(jobFileName, suffix)
}

func (s *Scheduler) fetchJobRunBatch(ctx context.Context, tnnt tenant.Tenant, jobQuery *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]byte, error) {
	dagRunRequest := getDagRunRequest(jobQuery, jobCron)
	reqBody, err := json.Marshal(dagRunRequest)
	if err != nil {
		return nil, errors.Wrap(EntityAirflow, "unable to marshal dag run request", err)
	}

	req := airflowRequest{path: dagStatusBatchURL, method: http.MethodPost, body: reqBody}

	schdAuth, err := s.getSchedulerAuth(ctx, tnnt.ProjectName())
	if err != nil {
		return nil, err
	}

	return s.client.Invoke(ctx, req, schdAuth)
}

func (s *Scheduler) GetJobRunsWithDetails(ctx context.Context, tnnt tenant.Tenant, jobQuery *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunWithDetails, error) {
	spanCtx, span := startChildSpan(ctx, "GetJobRuns")
	defer span.End()

	resp, err := s.fetchJobRunBatch(spanCtx, tnnt, jobQuery, jobCron)
	if err != nil {
		return nil, errors.Wrap(EntityAirflow, "failure while fetching airflow dag runs", err)
	}
	var dagRunList DagRunListResponse
	if err := json.Unmarshal(resp, &dagRunList); err != nil {
		return nil, errors.Wrap(EntityAirflow, fmt.Sprintf("json error on parsing airflow dag runs: %s", string(resp)), err)
	}

	return getJobRunsWithDetails(dagRunList, jobCron)
}

func (s *Scheduler) GetJobRuns(ctx context.Context, tnnt tenant.Tenant, jobQuery *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	spanCtx, span := startChildSpan(ctx, "GetJobRuns")
	defer span.End()

	resp, err := s.fetchJobRunBatch(spanCtx, tnnt, jobQuery, jobCron)
	if err != nil {
		return nil, errors.Wrap(EntityAirflow, "failure while fetching airflow dag runs", err)
	}

	var dagRunList DagRunListResponse
	if err := json.Unmarshal(resp, &dagRunList); err != nil {
		return nil, errors.Wrap(EntityAirflow, fmt.Sprintf("json error on parsing airflow dag runs: %s", string(resp)), err)
	}

	return getJobRuns(dagRunList, jobCron)
}

func (s *Scheduler) GetJobRunsForReplay(ctx context.Context, tnnt tenant.Tenant, jobQuery *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	spanCtx, span := startChildSpan(ctx, "GetJobRuns")
	defer span.End()

	resp, err := s.fetchJobRunBatch(spanCtx, tnnt, jobQuery, jobCron)
	if err != nil {
		return nil, errors.Wrap(EntityAirflow, "failure while fetching airflow dag runs", err)
	}

	// var dagRunList DagRunListResponse
	dagRunList, err := unmarshalAs[DagRunListResponse](resp)
	if err != nil {
		return nil, errors.Wrap(EntityAirflow, fmt.Sprintf("json error on parsing airflow dag runs: %s", string(resp)), err)
	}

	return getJobRunsForReplay(dagRunList, jobCron)
}

func (s *Scheduler) fetchJobs(ctx context.Context, schdAuth SchedulerAuth, offset int) (*DAGs, error) {
	params := url.Values{}
	params.Add("limit", "100") // Default and max is 100
	params.Add("order_by", "dag_id")
	params.Add("offset", strconv.Itoa(offset))
	req := airflowRequest{
		path:   dagURL,
		method: http.MethodGet,
		query:  params.Encode(),
	}

	resp, err := s.client.Invoke(ctx, req, schdAuth)
	if err != nil {
		return nil, err
	}

	var dagsInfo DAGs
	err = json.Unmarshal(resp, &dagsInfo)
	if err != nil {
		return nil, err
	}
	return &dagsInfo, nil
}

func (s *Scheduler) fetchAllJobs(ctx context.Context, schdAuth SchedulerAuth) (*DAGs, error) {
	var offset int
	var allDags DAGs
	for {
		fetchResp, err := s.fetchJobs(ctx, schdAuth, offset)
		if err != nil {
			return nil, err
		}
		allDags.DAGS = append(allDags.DAGS, fetchResp.DAGS...)
		if len(allDags.DAGS) < fetchResp.TotalEntries {
			offset = len(allDags.DAGS)
			continue
		}
		break
	}
	return &allDags, nil
}

// GetJobState sets the state of jobs disabled on scheduler
func (s *Scheduler) GetJobState(ctx context.Context, projectName tenant.ProjectName) (map[string]bool, error) {
	spanCtx, span := startChildSpan(ctx, "GetJobState")
	defer span.End()

	schdAuth, err := s.getSchedulerAuth(spanCtx, projectName)
	if err != nil {
		return nil, err
	}
	dagsInfo, err := s.fetchAllJobs(spanCtx, schdAuth)
	if err != nil {
		return nil, err
	}
	jobToStatusMap := make(map[string]bool)
	for _, dag := range dagsInfo.DAGS {
		jobToStatusMap[dag.DAGID] = dag.IsPaused
	}

	return jobToStatusMap, nil
}

// UpdateJobState set the state of jobs as enabled / disabled on scheduler
func (s *Scheduler) UpdateJobState(ctx context.Context, project tenant.ProjectName, jobNames []job.Name, state string) error {
	spanCtx, span := startChildSpan(ctx, "UpdateJobState")
	defer span.End()

	var data []byte
	switch state {
	case "enabled":
		data = []byte(`{"is_paused": false}`)
	case "disabled":
		data = []byte(`{"is_paused": true}`)
	}

	schdAuth, err := s.getSchedulerAuth(ctx, project)
	if err != nil {
		return err
	}
	ch := make(chan error, len(jobNames))
	for _, jobName := range jobNames {
		go func(jobName job.Name) {
			req := airflowRequest{
				path:   path.Join(dagURL, jobName.String()),
				method: http.MethodPatch,
				body:   data,
			}
			_, err := s.client.Invoke(spanCtx, req, schdAuth)
			ch <- err
		}(jobName)
	}
	me := errors.NewMultiError("update job state on scheduler")
	for i := 0; i < len(jobNames); i++ {
		me.Append(<-ch)
	}

	if len(me.Errors) > 0 {
		return errors.Wrap(EntityAirflow, "failure while updating dag status", me.ToErr())
	}

	return nil
}

func getDagRunRequest(jobQuery *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) DagRunRequest {
	if jobQuery.OnlyLastRun {
		return DagRunRequest{
			OrderBy:    "-execution_date",
			PageOffset: 0,
			PageLimit:  1,
			DagIds:     []string{jobQuery.Name},
		}
	}
	startDate := jobQuery.ExecutionStart(jobCron)
	endDate := jobQuery.ExecutionEndDate(jobCron)
	return DagRunRequest{
		OrderBy:          "execution_date",
		PageOffset:       0,
		PageLimit:        pageLimit,
		DagIds:           []string{jobQuery.Name},
		ExecutionDateGte: startDate.Format(airflowDateFormat),
		ExecutionDateLte: endDate.Format(airflowDateFormat),
	}
}

func (s *Scheduler) getSchedulerAuth(ctx context.Context, projectName tenant.ProjectName) (SchedulerAuth, error) {
	project, err := s.projectGetter.Get(ctx, projectName)
	if err != nil {
		return SchedulerAuth{}, err
	}

	host, err := project.GetConfig(schedulerHostKey)
	if err != nil {
		return SchedulerAuth{}, err
	}

	auth, err := s.secretGetter.Get(ctx, projectName, "", tenant.SecretSchedulerAuth)
	if err != nil {
		return SchedulerAuth{}, err
	}

	schdHost := strings.ReplaceAll(host, "http://", "")
	return SchedulerAuth{
		host:  schdHost,
		token: auth.Value(),
	}, nil
}

func (s *Scheduler) Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, executionTime time.Time) error {
	return s.ClearBatch(ctx, t, jobName, executionTime, executionTime)
}

func (s *Scheduler) GetRolePermissions(ctx context.Context, t tenant.Tenant, roleName string) ([]string, error) {
	schAuth, err := s.getSchedulerAuth(ctx, t.ProjectName())
	if err != nil {
		return nil, err
	}
	req := airflowRequest{
		path:   fmt.Sprintf(rolesPermissionURL, roleName),
		method: http.MethodGet,
	}

	resp, err := s.client.Invoke(ctx, req, schAuth)
	if err != nil {
		return nil, err
	}

	var permissionSets PermissionSet
	err = json.Unmarshal(resp, &permissionSets)
	if err != nil {
		return nil, err
	}

	var permissions []string
	if strings.EqualFold(permissionSets.Name, t.NamespaceName().String()) {
		for _, a := range permissionSets.Actions {
			permissions = append(permissions, a.String())
		}
	}
	return permissions, nil
}

func (s *Scheduler) AddRole(ctx context.Context, tnnt tenant.Tenant, roleName string, ifNotExist bool) error {
	schAuth, err := s.getSchedulerAuth(ctx, tnnt.ProjectName())
	if err != nil {
		return errors.Wrap(EntityAirflow, "secret not registered", err)
	}
	data := []byte(fmt.Sprintf(`{"name": %q,"actions": []}`, roleName))
	req := airflowRequest{
		path:   rolesURL,
		method: http.MethodPost,
		body:   data,
	}

	resp, err := s.client.Invoke(ctx, req, schAuth)
	if err != nil {
		if ifNotExist && strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return err
	}

	if string(resp) != "{}\n" {
		return errors.InternalError(EntityAirflow, fmt.Sprintf("got Airflow Response: %s", string(resp)), nil)
	}
	return nil
}

func (s *Scheduler) ClearBatch(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, startExecutionTime, endExecutionTime time.Time) error {
	spanCtx, span := startChildSpan(ctx, "Clear")
	defer span.End()

	data := []byte(fmt.Sprintf(`{"start_date": %q, "end_date": %q, "dry_run": false, "reset_dag_runs": true, "only_failed": false}`,
		startExecutionTime.UTC().Format(airflowDateFormat),
		endExecutionTime.UTC().Format(airflowDateFormat)))
	req := airflowRequest{
		path:   fmt.Sprintf(dagRunClearURL, jobName.String()),
		method: http.MethodPost,
		body:   data,
	}
	schdAuth, err := s.getSchedulerAuth(ctx, tnnt.ProjectName())
	if err != nil {
		return err
	}
	respBytes, err := s.client.Invoke(spanCtx, req, schdAuth)
	if err != nil {
		return errors.Wrap(EntityAirflow, "failure while clearing airflow dag runs", err)
	}

	resp, err := unmarshalAs[ClearTaskInstancesResponse](respBytes)
	if err != nil {
		return errors.Wrap(EntityAirflow, "failure while un-marshaling createTaskInstance API response", err)
	}
	if resp != nil && resp.TotalEntries == 0 {
		return fmt.Errorf("unbale to clear job:%s with execution date range [%s -> %s]", jobName, startExecutionTime, endExecutionTime)
	}

	return nil
}

func (s *Scheduler) CancelRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, dagRunID string) error {
	spanCtx, span := startChildSpan(ctx, "CancelRun")
	defer span.End()
	data := []byte(`{"state": "failed"}`)
	req := airflowRequest{
		path:   fmt.Sprintf(dagRunModifyURL, jobName.String(), dagRunID),
		method: http.MethodPatch,
		body:   data,
	}
	schdAuth, err := s.getSchedulerAuth(ctx, tnnt.ProjectName())
	if err != nil {
		return err
	}
	_, err = s.client.Invoke(spanCtx, req, schdAuth)
	if err != nil {
		return errors.Wrap(EntityAirflow, "failure while canceling airflow dag run", err)
	}
	return nil
}

func (s *Scheduler) CreateRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, executionTime time.Time, dagRunIDPrefix string) error {
	spanCtx, span := startChildSpan(ctx, "CreateRun")
	defer span.End()

	data := []byte(fmt.Sprintf(`{"dag_run_id": %q, "execution_date": %q}`,
		fmt.Sprintf("%s__%s", dagRunIDPrefix, executionTime.UTC().Format(airflowDateFormat)),
		executionTime.UTC().Format(airflowDateFormat)),
	)
	req := airflowRequest{
		path:   fmt.Sprintf(dagRunCreateURL, jobName.String()),
		method: http.MethodPost,
		body:   data,
	}
	schdAuth, err := s.getSchedulerAuth(ctx, tnnt.ProjectName())
	if err != nil {
		return err
	}
	_, err = s.client.Invoke(spanCtx, req, schdAuth)
	if err != nil {
		return errors.Wrap(EntityAirflow, "failure while creating airflow dag run", err)
	}
	return nil
}

func (s *Scheduler) GetOperatorInstance(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, dagRunID, operatorID string) (*scheduler.OperatorRunInstance, error) {
	spanCtx, span := startChildSpan(ctx, "GetOperatorInstance")
	defer span.End()

	req := airflowRequest{
		path:   fmt.Sprintf(getTaskInstanceURL, jobName.String(), dagRunID, operatorID),
		method: http.MethodGet,
	}
	schdAuth, err := s.getSchedulerAuth(ctx, tnnt.ProjectName())
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Invoke(spanCtx, req, schdAuth)
	if err != nil {
		return nil, err
	}
	taskInstance, err := unmarshalAs[TaskInstance](resp)
	if err != nil {
		return nil, errors.Wrap(EntityAirflow, "failure while creating airflow dag run", err)
	}

	return taskInstance.toSchedulerOperatorRunInstance()
}

func NewScheduler(l log.Logger, bucketFac BucketFactory, client Client, compiler DagCompiler, projectGetter ProjectGetter, secretGetter SecretGetter) *Scheduler {
	return &Scheduler{
		l:             l,
		bucketFac:     bucketFac,
		compiler:      compiler,
		client:        client,
		projectGetter: projectGetter,
		secretGetter:  secretGetter,
	}
}
