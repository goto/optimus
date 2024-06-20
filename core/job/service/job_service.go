package service

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/goto/salt/log"
	"github.com/kushsharma/parallel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/event"
	"github.com/goto/optimus/core/event/moderator"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/dto"
	"github.com/goto/optimus/core/job/service/filter"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/compiler"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/tree"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/telemetry"
	"github.com/goto/optimus/internal/writer"
	"github.com/goto/optimus/sdk/plugin"
)

const (
	ConcurrentTicketPerSec = 50
	ConcurrentLimit        = 100

	// projectConfigPrefix will be used to prefix all the config variables of
	// a project, i.e. registered entities
	projectConfigPrefix = "GLOBAL__"
)

var (

	// right now this is done to capture the feature adoption
	getChangelogFeatureAdoption = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "get_changelog_total",
		Help: "number of requests received for viewing changelog",
	}, []string{"project", "job"})

	getChangelogFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "get_changelog_errors",
		Help: "errors occurred in get changelog",
	}, []string{"project", "job", "error"})
)

type JobService struct {
	jobRepo        JobRepository
	upstreamRepo   UpstreamRepository
	downstreamRepo DownstreamRepository

	pluginService    PluginService
	upstreamResolver UpstreamResolver
	eventHandler     EventHandler

	tenantDetailsGetter TenantDetailsGetter

	jobDeploymentService JobDeploymentService
	engine               Engine

	jobRunInputCompiler JobRunInputCompiler
	resourceChecker     ResourceExistenceChecker

	logger log.Logger
}

func NewJobService(
	jobRepo JobRepository, upstreamRepo UpstreamRepository, downstreamRepo DownstreamRepository,
	pluginService PluginService, upstreamResolver UpstreamResolver,
	tenantDetailsGetter TenantDetailsGetter, eventHandler EventHandler, logger log.Logger,
	jobDeploymentService JobDeploymentService, engine Engine,
	jobInputCompiler JobRunInputCompiler, resourceChecker ResourceExistenceChecker,
) *JobService {
	return &JobService{
		jobRepo:              jobRepo,
		upstreamRepo:         upstreamRepo,
		downstreamRepo:       downstreamRepo,
		pluginService:        pluginService,
		upstreamResolver:     upstreamResolver,
		eventHandler:         eventHandler,
		tenantDetailsGetter:  tenantDetailsGetter,
		logger:               logger,
		jobDeploymentService: jobDeploymentService,
		engine:               engine,
		jobRunInputCompiler:  jobInputCompiler,
		resourceChecker:      resourceChecker,
	}
}

type Engine interface {
	Compile(templateMap map[string]string, context map[string]any) (map[string]string, error)
	CompileString(input string, context map[string]any) (string, error)
}

type PluginService interface {
	Info(ctx context.Context, taskName string) (*plugin.Info, error)
	IdentifyUpstreams(ctx context.Context, taskName string, compiledConfig, assets map[string]string) (resourceURNs []resource.URN, err error)
	ConstructDestinationURN(ctx context.Context, taskName string, compiledConfig map[string]string) (destinationURN resource.URN, err error)
}

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, jobTenant tenant.Tenant) (*tenant.WithDetails, error)
}

type JobDeploymentService interface {
	UploadJobs(ctx context.Context, jobTenant tenant.Tenant, toUpdate, toDelete []string) error
	UpdateJobScheduleState(ctx context.Context, tnnt tenant.Tenant, jobName []job.Name, state string) error
}

type JobRepository interface {
	Add(context.Context, []*job.Job) (addedJobs []*job.Job, err error)
	Update(context.Context, []*job.Job) (updatedJobs []*job.Job, err error)
	Delete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, cleanHistory bool) error
	SetDirty(ctx context.Context, jobsTenant tenant.Tenant, jobNames []job.Name, isDirty bool) error
	ChangeJobNamespace(ctx context.Context, jobName job.Name, tenant, newTenant tenant.Tenant) error

	GetChangelog(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.ChangeLog, error)
	GetByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error)
	GetAllByResourceDestination(ctx context.Context, resourceDestination resource.URN) ([]*job.Job, error)
	GetAllByTenant(ctx context.Context, jobTenant tenant.Tenant) ([]*job.Job, error)
	GetAllByProjectName(ctx context.Context, projectName tenant.ProjectName) ([]*job.Job, error)
	SyncState(ctx context.Context, jobTenant tenant.Tenant, disabledJobNames, enabledJobNames []job.Name) error
	UpdateState(ctx context.Context, jobTenant tenant.Tenant, jobNames []job.Name, jobState job.State, remark string) error
}

type UpstreamRepository interface {
	ResolveUpstreams(context.Context, tenant.ProjectName, []job.Name) (map[job.Name][]*job.Upstream, error)
	ReplaceUpstreams(context.Context, []*job.WithUpstream) error
	GetUpstreams(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.Upstream, error)
}

type DownstreamRepository interface {
	GetDownstreamByDestination(ctx context.Context, projectName tenant.ProjectName, destination resource.URN) ([]*job.Downstream, error)
	GetDownstreamByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.Downstream, error)
	GetDownstreamBySources(ctx context.Context, sources []resource.URN) ([]*job.Downstream, error)
}

type EventHandler interface {
	HandleEvent(moderator.Event)
}

type UpstreamResolver interface {
	CheckStaticResolvable(ctx context.Context, tnnt tenant.Tenant, jobs []*job.Job, logWriter writer.LogWriter) error
	BulkResolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job, logWriter writer.LogWriter) (jobWithUpstreams []*job.WithUpstream, err error)
	Resolve(ctx context.Context, subjectJob *job.Job, logWriter writer.LogWriter) ([]*job.Upstream, error)
}

type JobRunInputCompiler interface {
	Compile(ctx context.Context, job *scheduler.JobWithDetails, config scheduler.RunConfig, executedAt time.Time) (*scheduler.ExecutorInput, error)
}

type ResourceExistenceChecker interface {
	GetByURN(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (*resource.Resource, error)
	ExistInStore(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (bool, error)
}

func (j *JobService) Add(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) ([]job.Name, error) {
	logWriter := writer.NewLogWriter(j.logger)
	me := errors.NewMultiError("add specs errors")

	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		j.logger.Error("error getting tenant details: %s", err)
		return []job.Name{}, err
	}

	jobs, err := j.generateJobs(ctx, tenantWithDetails, specs, logWriter)
	me.Append(err)

	addedJobs, err := j.jobRepo.Add(ctx, jobs)
	me.Append(err)

	jobsWithUpstreams, err := j.upstreamResolver.BulkResolve(ctx, jobTenant.ProjectName(), addedJobs, logWriter)
	me.Append(err)

	err = j.upstreamRepo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	err = j.uploadJobs(ctx, jobTenant, addedJobs, nil, nil)
	me.Append(err)

	for _, addedJob := range addedJobs {
		j.raiseCreateEvent(addedJob)
		if addedJob.Spec().Schedule().CatchUp() {
			msg := fmt.Sprintf("catchup for job %s is enabled", addedJob.GetName())
			j.logger.Warn(msg)
			logWriter.Write(writer.LogLevelWarning, msg)
		}
	}
	raiseJobEventMetric(jobTenant, job.MetricJobEventStateAdded, len(addedJobs))

	if len(addedJobs) < len(specs) {
		totalFailed := len(specs) - len(addedJobs)
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateUpsertFailed, totalFailed)
	}

	if addedJobs == nil {
		return nil, me.ToErr()
	}

	return job.Jobs(addedJobs).GetJobNames(), me.ToErr()
}

func (j *JobService) Update(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) ([]job.Name, error) {
	logWriter := writer.NewLogWriter(j.logger)
	me := errors.NewMultiError("update specs errors")

	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		j.logger.Error("error getting tenant details: %s", err)
		return []job.Name{}, err
	}
	existingJobs := make(map[job.Name]*job.Job)
	for _, spec := range specs {
		existingJob, err := j.jobRepo.GetByJobName(ctx, jobTenant.ProjectName(), spec.Name())
		if err != nil {
			me.Append(err)
			continue
		}
		existingJobs[spec.Name()] = existingJob
	}

	jobs, err := j.generateJobs(ctx, tenantWithDetails, specs, logWriter)
	me.Append(err)

	updatedJobs, err := j.jobRepo.Update(ctx, jobs)
	me.Append(err)

	jobsWithUpstreams, err := j.upstreamResolver.BulkResolve(ctx, jobTenant.ProjectName(), updatedJobs, logWriter)
	me.Append(err)

	err = j.upstreamRepo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	err = j.uploadJobs(ctx, jobTenant, nil, updatedJobs, nil)
	me.Append(err)

	if len(updatedJobs) > 0 {
		for _, updatedJob := range updatedJobs {
			j.raiseUpdateEvent(updatedJob, getUpdateImpactType(existingJobs[updatedJob.Spec().Name()], updatedJob))
		}
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateUpdated, len(updatedJobs))
	}

	if len(updatedJobs) < len(specs) {
		totalFailed := len(specs) - len(updatedJobs)
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateUpsertFailed, totalFailed)
	}

	if updatedJobs == nil {
		return nil, me.ToErr()
	}

	return job.Jobs(updatedJobs).GetJobNames(), me.ToErr()
}

func (j *JobService) Upsert(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) ([]job.Name, error) {
	logWriter := writer.NewLogWriter(j.logger)
	me := errors.NewMultiError("upsert specs errors")

	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		j.logger.Error("error getting tenant details: %s", err)
		return []job.Name{}, err
	}

	jobs, err := j.generateJobs(ctx, tenantWithDetails, specs, logWriter)
	me.Append(err)

	existingJobs := make(map[job.Name]*job.Job)
	var jobsToAdd, jobsToUpdate []*job.Job
	for _, jobToUpsert := range jobs {
		existingJob, err := j.jobRepo.GetByJobName(ctx, jobTenant.ProjectName(), jobToUpsert.Spec().Name())
		if err != nil {
			if !errors.IsErrorType(err, errors.ErrNotFound) {
				me.Append(err)
				continue
			}
			jobsToAdd = append(jobsToAdd, jobToUpsert)
			continue
		}
		jobsToUpdate = append(jobsToUpdate, jobToUpsert)
		existingJobs[jobToUpsert.Spec().Name()] = existingJob
	}

	var upsertedJobs, addedJobs, updatedJobs []*job.Job
	if len(jobsToAdd) > 0 {
		addedJobs, err = j.jobRepo.Add(ctx, jobsToAdd)
		upsertedJobs = append(upsertedJobs, addedJobs...)
		me.Append(err)
	}
	if len(jobsToUpdate) > 0 {
		updatedJobs, err = j.jobRepo.Update(ctx, jobsToUpdate)
		upsertedJobs = append(upsertedJobs, updatedJobs...)
		me.Append(err)
	}

	if len(upsertedJobs) == 0 {
		return []job.Name{}, err
	}

	jobsWithUpstreams, err := j.upstreamResolver.BulkResolve(ctx, jobTenant.ProjectName(), upsertedJobs, logWriter)
	me.Append(err)

	err = j.upstreamRepo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	err = j.uploadJobs(ctx, jobTenant, addedJobs, updatedJobs, nil)
	me.Append(err)

	if len(addedJobs) > 0 {
		logWriter.Write(writer.LogLevelInfo, fmt.Sprintf("[%s] added %d jobs", tenantWithDetails.Namespace().Name().String(), len(addedJobs)))
		for _, addedJob := range addedJobs {
			j.raiseCreateEvent(addedJob)
			if addedJob.Spec().Schedule().CatchUp() {
				msg := fmt.Sprintf("catchup for job %s is enabled", addedJob.GetName())
				logWriter.Write(writer.LogLevelWarning, msg)
			}
		}
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateAdded, len(addedJobs))
	}

	if len(updatedJobs) > 0 {
		logWriter.Write(writer.LogLevelInfo, fmt.Sprintf("[%s] updated %d jobs", tenantWithDetails.Namespace().Name().String(), len(updatedJobs)))
		for _, updatedJob := range updatedJobs {
			j.raiseUpdateEvent(updatedJob, getUpdateImpactType(existingJobs[updatedJob.Spec().Name()], updatedJob))
		}
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateUpdated, len(updatedJobs))
	}

	if len(upsertedJobs) < len(specs) {
		totalFailed := len(specs) - len(upsertedJobs)
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateUpsertFailed, totalFailed)
	}

	return job.Jobs(upsertedJobs).GetJobNames(), me.ToErr()
}

func (j *JobService) UpdateState(ctx context.Context, jobTenant tenant.Tenant, jobNames []job.Name, jobState job.State, remark string) error {
	err := j.jobDeploymentService.UpdateJobScheduleState(ctx, jobTenant, jobNames, jobState.String())
	if err != nil {
		return err
	}

	if err := j.jobRepo.UpdateState(ctx, jobTenant, jobNames, jobState, remark); err != nil {
		return err
	}

	var metricName string
	switch jobState {
	case job.ENABLED:
		metricName = job.MetricJobEventEnabled
	case job.DISABLED:
		metricName = job.MetricJobEventDisabled
	}

	raiseJobEventMetric(jobTenant, metricName, len(jobNames))
	for _, jobName := range jobNames {
		j.raiseStateChangeEvent(jobTenant, jobName, jobState)
	}
	return nil
}

func (j *JobService) SyncState(ctx context.Context, jobTenant tenant.Tenant, disabledJobNames, enabledJobNames []job.Name) error {
	return j.jobRepo.SyncState(ctx, jobTenant, disabledJobNames, enabledJobNames)
}

func (j *JobService) Delete(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, cleanFlag, forceFlag bool) (affectedDownstream []job.FullName, err error) {
	downstreamList, err := j.downstreamRepo.GetDownstreamByJobName(ctx, jobTenant.ProjectName(), jobName)
	if err != nil {
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleteFailed, 1)
		j.logger.Error("error getting downstream jobs for [%s]: %s", jobName, err)
		return nil, err
	}

	downstreamFullNames := job.DownstreamList(downstreamList).GetDownstreamFullNames()

	if len(downstreamList) > 0 && !forceFlag {
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleteFailed, 1)
		errorMsg := fmt.Sprintf("%s depends on this job. consider do force delete to proceed.", downstreamFullNames)
		j.logger.Error(errorMsg)
		return nil, errors.NewError(errors.ErrFailedPrecond, job.EntityJob, errorMsg)
	}

	if err := j.jobRepo.Delete(ctx, jobTenant.ProjectName(), jobName, cleanFlag); err != nil {
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleteFailed, 1)
		j.logger.Error("error deleting job [%s]: %s", jobName, err)
		return downstreamFullNames, err
	}

	raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleted, 1)

	if err := j.uploadJobs(ctx, jobTenant, nil, nil, []job.Name{jobName}); err != nil {
		j.logger.Error("error uploading job [%s]: %s", jobName, err)
		return downstreamFullNames, err
	}

	j.raiseDeleteEvent(jobTenant, jobName)

	return downstreamFullNames, nil
}

func (j *JobService) ChangeNamespace(ctx context.Context, jobTenant, jobNewTenant tenant.Tenant, jobName job.Name) error {
	err := j.jobRepo.ChangeJobNamespace(ctx, jobName, jobTenant, jobNewTenant)
	if err != nil {
		errorsMsg := fmt.Sprintf("unable to successfully finish job namespace change transaction : %s", err.Error())
		return errors.NewError(errors.ErrInternalError, job.EntityJob, errorsMsg)
	}

	newJobSpec, err := j.jobRepo.GetByJobName(ctx, jobNewTenant.ProjectName(), jobName)
	if err != nil {
		errorsMsg := fmt.Sprintf(" unable fetch jobSpecs for newly modified job : %s, namespace: %s, err: %s", jobName, jobNewTenant.NamespaceName(), err.Error())
		return errors.NewError(errors.ErrInternalError, job.EntityJob, errorsMsg)
	}

	err = j.uploadJobs(ctx, jobTenant, nil, nil, []job.Name{jobName})
	if err != nil {
		errorsMsg := fmt.Sprintf(" unable to remove old job : %s", err.Error())
		return errors.NewError(errors.ErrInternalError, job.EntityJob, errorsMsg)
	}

	err = j.uploadJobs(ctx, jobNewTenant, []*job.Job{newJobSpec}, nil, nil)
	if err != nil {
		errorsMsg := fmt.Sprintf(" unable to create new job on scheduler : %s", err.Error())
		return errors.NewError(errors.ErrInternalError, job.EntityJob, errorsMsg)
	}
	j.raiseUpdateEvent(newJobSpec, job.UnspecifiedImpactChange)
	return nil
}

func (j *JobService) GetChangelog(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.ChangeLog, error) {
	changelog, err := j.jobRepo.GetChangelog(ctx, projectName, jobName)
	if err != nil {
		getChangelogFailures.WithLabelValues(
			projectName.String(),
			jobName.String(),
			err.Error(),
		).Inc()
	}
	getChangelogFeatureAdoption.WithLabelValues(
		projectName.String(),
		jobName.String(),
	).Inc()

	return changelog, err
}

func (j *JobService) Get(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name) (*job.Job, error) {
	jobs, err := j.GetByFilter(ctx,
		filter.WithString(filter.ProjectName, jobTenant.ProjectName().String()),
		filter.WithString(filter.JobName, jobName.String()),
	)
	if err != nil {
		j.logger.Error("error getting job specified by the filter: %s", err)
		return nil, err
	}
	if len(jobs) == 0 {
		j.logger.Error("job [%s] is not found", jobName)
		return nil, errors.NotFound(job.EntityJob, fmt.Sprintf("job %s is not found", jobName))
	}
	return jobs[0], nil
}

func (j *JobService) GetTaskInfo(ctx context.Context, task job.Task) (*plugin.Info, error) {
	return j.pluginService.Info(ctx, task.Name().String())
}

func (j *JobService) GetByFilter(ctx context.Context, filters ...filter.FilterOpt) ([]*job.Job, error) {
	f := filter.NewFilter(filters...)

	// when resource destination exist, filter by destination
	if f.Contains(filter.ResourceDestination) {
		j.logger.Debug("getting all jobs by resource destination [%s]", f.GetStringValue(filter.ResourceDestination))

		resourceDestinationURN, err := resource.ParseURN(f.GetStringValue(filter.ResourceDestination))
		if err != nil {
			return nil, err
		}

		return j.jobRepo.GetAllByResourceDestination(ctx, resourceDestinationURN)
	}

	// when project name and job names exist, filter by project and job names
	if f.Contains(filter.ProjectName, filter.JobNames) {
		j.logger.Debug("getting all jobs by project name [%s] and job names", f.GetStringValue(filter.ProjectName))

		me := errors.NewMultiError("get all job specs errors")

		projectName, _ := tenant.ProjectNameFrom(f.GetStringValue(filter.ProjectName))
		jobNames := f.GetStringArrayValue(filter.JobNames)

		var jobs []*job.Job
		for _, jobNameStr := range jobNames {
			jobName, _ := job.NameFrom(jobNameStr)
			fetchedJob, err := j.jobRepo.GetByJobName(ctx, projectName, jobName)
			if err != nil {
				if !errors.IsErrorType(err, errors.ErrNotFound) {
					j.logger.Error("error getting job [%s] from db: %s", jobName, err)
					me.Append(err)
				}
				continue
			}
			jobs = append(jobs, fetchedJob)
		}
		return jobs, me.ToErr()
	}

	// when project name and job name exist, filter by project name and job name
	if f.Contains(filter.ProjectName, filter.JobName) {
		j.logger.Debug("getting all jobs by project name [%s] and job name [%s]", f.GetStringValue(filter.ProjectName), f.GetStringValue(filter.JobName))

		projectName, _ := tenant.ProjectNameFrom(f.GetStringValue(filter.ProjectName))
		jobName, _ := job.NameFrom(f.GetStringValue(filter.JobName))
		fetchedJob, err := j.jobRepo.GetByJobName(ctx, projectName, jobName)
		if err != nil {
			if errors.IsErrorType(err, errors.ErrNotFound) {
				return []*job.Job{}, nil
			}
			j.logger.Error("error getting job [%s] from db: %s", jobName, err)
			return nil, err
		}
		return []*job.Job{fetchedJob}, nil
	}

	// when project name and namespace names exist, filter by tenant
	if f.Contains(filter.ProjectName, filter.NamespaceNames) {
		j.logger.Debug("getting all jobs by project name [%s] and namespace names", f.GetStringValue(filter.ProjectName))

		var jobs []*job.Job
		namespaceNames := f.GetStringArrayValue(filter.NamespaceNames)
		for _, namespaceName := range namespaceNames {
			jobTenant, err := tenant.NewTenant(f.GetStringValue(filter.ProjectName), namespaceName)
			if err != nil {
				j.logger.Error("invalid tenant request information project [%s] namespace [%s]: %s", f.GetStringValue(filter.ProjectName), f.GetStringValue(filter.NamespaceName), err)
				return nil, err
			}
			tenantJobs, err := j.jobRepo.GetAllByTenant(ctx, jobTenant)
			if err != nil {
				j.logger.Error("error getting all jobs under project [%s] namespace [%s]: %s", jobTenant.ProjectName().String(), jobTenant.NamespaceName().String(), err)
				return nil, err
			}
			jobs = append(jobs, tenantJobs...)
		}
		return jobs, nil
	}

	// when project name and namespace name exist, filter by tenant
	if f.Contains(filter.ProjectName, filter.NamespaceName) {
		j.logger.Debug("getting all jobs by project name [%s] and namespace name [%s]", f.GetStringValue(filter.ProjectName), f.GetStringValue(filter.NamespaceName))

		jobTenant, err := tenant.NewTenant(f.GetStringValue(filter.ProjectName), f.GetStringValue(filter.NamespaceName))
		if err != nil {
			j.logger.Error("invalid tenant request information project [%s] namespace [%s]: %s", f.GetStringValue(filter.ProjectName), f.GetStringValue(filter.NamespaceName), err)
			return nil, err
		}
		return j.jobRepo.GetAllByTenant(ctx, jobTenant)
	}

	// when project name exist, filter by project name
	if f.Contains(filter.ProjectName) {
		j.logger.Debug("getting all jobs by project name [%s]", f.GetStringValue(filter.ProjectName))

		projectName, _ := tenant.ProjectNameFrom(f.GetStringValue(filter.ProjectName))
		return j.jobRepo.GetAllByProjectName(ctx, projectName)
	}

	j.logger.Error("filter combination is not recognized")
	return nil, fmt.Errorf("no filter matched")
}

func (j *JobService) bulkJobCleanup(ctx context.Context, jobTenant tenant.Tenant, toDelete []*job.Spec, logWriter writer.LogWriter) error {
	me := errors.NewMultiError("bulk Job Cleanup errors")
	deletedJobNames, err := j.bulkDelete(ctx, jobTenant, toDelete, logWriter)
	me.Append(err)
	err = j.uploadJobs(ctx, jobTenant, nil, nil, deletedJobNames)
	if err != nil {
		me.Append(err)
		return errors.Wrap(job.EntityJob, "critical error deleting DAGS from scheduler storage, consider deleting the dags manually and report this incident to administrators, this wont fix on a retry", me.ToErr())
	}
	return me.ToErr()
}

func (j *JobService) bulkJobPersist(ctx context.Context, tenantWithDetails *tenant.WithDetails, toAdd, toUpdate []*job.Spec, logWriter writer.LogWriter) ([]*job.Job, []*job.Job, error) {
	me := errors.NewMultiError("replace all specs errors")
	addedJobs, err := j.bulkAdd(ctx, tenantWithDetails, toAdd, logWriter)
	me.Append(err)
	failedToAdd := len(toAdd) - len(addedJobs)

	updatedJobs, err := j.bulkUpdate(ctx, tenantWithDetails, toUpdate, logWriter)
	me.Append(err)
	failedToUpdate := len(toUpdate) - len(updatedJobs)

	raiseJobEventMetric(tenantWithDetails.ToTenant(), job.MetricJobEventStateUpsertFailed, failedToAdd+failedToUpdate)
	return addedJobs, updatedJobs, me.ToErr()
}

func (j *JobService) markJobsAsDirty(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Job) ([]job.Name, error) {
	if len(jobs) == 0 {
		return []job.Name{}, nil
	}
	jobsNamesToMarkDirty := make([]job.Name, len(jobs))
	for i, jobObj := range jobs {
		jobsNamesToMarkDirty[i] = jobObj.Spec().Name()
	}

	err := j.jobRepo.SetDirty(ctx, jobTenant, jobsNamesToMarkDirty, true)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "critical, failed to mark incoming jobs as dirty, this can not be fixed on retry", err)
	}

	return jobsNamesToMarkDirty, nil
}

func (j *JobService) logFoundDirtySpecs(tnnt tenant.Tenant, unmodifiedDirtySpecs []*job.Spec, logWriter writer.LogWriter) {
	dirtyJobsName := make([]string, len(unmodifiedDirtySpecs))
	for i, dirtyJob := range unmodifiedDirtySpecs {
		dirtyJobsName[i] = dirtyJob.Name().String()
	}
	raiseJobEventMetric(tnnt, job.MetricJobEventFoundDirty, len(dirtyJobsName))
	infoMsg := fmt.Sprintf("[%s] Found %d unprocessed Jobs: %s, reprocessing them.", tnnt.NamespaceName(), len(unmodifiedDirtySpecs), strings.Join(dirtyJobsName, ", "))
	j.logger.Info(infoMsg)
	logWriter.Write(writer.LogLevelInfo, infoMsg)
}

func getUpdateImpactType(currentJob, incomingJob *job.Job) job.UpdateImpact {
	if currentJob.Spec().DiffBehaviorally(incomingJob.Spec()) {
		return job.JobBehaviourImpact
	}
	return job.JobInternalImpact
}

func (j *JobService) raiseUpdateEvents(existingJobs, addedJobs, updatedJobs []*job.Job) {
	if len(updatedJobs) > 0 {
		existingJobsMap := job.Jobs(existingJobs).GetNameMap()
		for _, job := range updatedJobs {
			j.raiseUpdateEvent(job, getUpdateImpactType(existingJobsMap[job.Spec().Name()], job))
		}
	}
	for _, job := range addedJobs {
		j.raiseCreateEvent(job)
	}
}

func (j *JobService) ReplaceAll(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec, jobNamesWithInvalidSpec []job.Name, logWriter writer.LogWriter) error {
	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		j.logger.Error("error getting tenant details: %s", err)
		return err
	}
	existingJobs, err := j.jobRepo.GetAllByTenant(ctx, jobTenant)
	if err != nil {
		j.logger.Error("error getting all jobs for tenant: %s/%s, details: %s", jobTenant.ProjectName(), jobTenant.NamespaceName(), err)
		return err
	}

	toAdd, toUpdate, toDelete, _, unmodifiedDirtySpecs := j.differentiateSpecs(jobTenant, existingJobs, specs, jobNamesWithInvalidSpec)
	logWriter.Write(writer.LogLevelInfo, fmt.Sprintf("[%s] found %d new, %d modified, and %d deleted job specs", jobTenant.NamespaceName().String(), len(toAdd), len(toUpdate), len(toDelete)))
	if len(unmodifiedDirtySpecs) > 0 {
		j.logFoundDirtySpecs(tenantWithDetails.ToTenant(), unmodifiedDirtySpecs, logWriter)
	}

	me := errors.NewMultiError("persist job error")

	toUpdate = append(toUpdate, unmodifiedDirtySpecs...)

	addedJobs, updatedJobs, err := j.bulkJobPersist(ctx, tenantWithDetails, toAdd, toUpdate, logWriter)
	j.raiseUpdateEvents(existingJobs, addedJobs, updatedJobs)

	me.Append(err)
	jobsMarkedDirty, err := j.markJobsAsDirty(ctx, jobTenant, append(addedJobs, updatedJobs...))
	me.Append(err)
	if me.ToErr() != nil {
		return errors.Wrap(job.EntityJob, "error in persisting jobs to DB", me.ToErr())
	}

	if err := j.bulkJobCleanup(ctx, jobTenant, toDelete, logWriter); err != nil {
		return err
	}

	if err := j.resolveAndSaveUpstreams(ctx, jobTenant, logWriter, addedJobs, updatedJobs); err != nil {
		return errors.Wrap(job.EntityJob, "failed resolving job upstreams", err)
	}

	if err := j.uploadJobs(ctx, jobTenant, addedJobs, updatedJobs, nil); err != nil {
		return errors.Wrap(job.EntityJob, "failed uploading compiled dags", err)
	}

	if len(jobsMarkedDirty) != 0 {
		if err := j.jobRepo.SetDirty(ctx, jobTenant, jobsMarkedDirty, false); err != nil {
			return errors.Wrap(job.EntityJob, "failed to mark jobs as cleaned, these jobs will be reprocessed in next attempt", err)
		}
	}

	return nil
}

func (j *JobService) uploadJobs(ctx context.Context, jobTenant tenant.Tenant, addedJobs, updatedJobs []*job.Job, deletedJobNames []job.Name) error {
	if len(addedJobs) == 0 && len(updatedJobs) == 0 && len(deletedJobNames) == 0 {
		j.logger.Warn("no jobs to be uploaded")
		return nil
	}

	var jobNamesToUpload, jobNamesToRemove []string
	for _, addedJob := range append(addedJobs, updatedJobs...) {
		jobNamesToUpload = append(jobNamesToUpload, addedJob.GetName())
	}

	for _, deletedJobName := range deletedJobNames {
		jobNamesToRemove = append(jobNamesToRemove, deletedJobName.String())
	}

	return j.jobDeploymentService.UploadJobs(ctx, jobTenant, jobNamesToUpload, jobNamesToRemove)
}

func (j *JobService) Refresh(ctx context.Context, projectName tenant.ProjectName, namespaceNames, jobNames []string, logWriter writer.LogWriter) (err error) {
	projectFilter := filter.WithString(filter.ProjectName, projectName.String())
	namespacesFilter := filter.WithStringArray(filter.NamespaceNames, namespaceNames)
	jobNamesFilter := filter.WithStringArray(filter.JobNames, jobNames)

	allJobs, err := j.GetByFilter(ctx, projectFilter, namespacesFilter, jobNamesFilter)
	if err != nil {
		j.logger.Error("error getting jobs by filter: %s", err)
		return err
	}

	me := errors.NewMultiError("refresh all specs errors")
	namespaceAndJobsMap := job.Jobs(allJobs).GetNamespaceNameAndJobsMap()
	for namespaceName, jobs := range namespaceAndJobsMap {
		jobTenant, err := tenant.NewTenant(projectName.String(), namespaceName.String())
		if err != nil {
			j.logger.Error("invalid tenant information requet project [%s] namespace [%s]: %s", projectName.String(), namespaceName.String(), err)
			me.Append(err)
			continue
		}

		tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
		if err != nil {
			j.logger.Error("error getting tenant details: %s", err)
			me.Append(err)
			continue
		}

		specs := job.Jobs(jobs).GetSpecs()
		updatedJobs, err := j.bulkUpdate(ctx, tenantWithDetails, specs, logWriter)
		me.Append(err)
		for _, updatedJob := range updatedJobs {
			// can this be avoided, as we may not want to raise an event on refresh
			j.raiseUpdateEvent(updatedJob, job.JobInternalImpact)
		}

		j.logger.Debug("resolving upstreams for [%d] jobs of project [%s] namespace [%s]", len(updatedJobs), projectName, namespaceName)
		jobsWithUpstreams, err := j.upstreamResolver.BulkResolve(ctx, projectName, updatedJobs, logWriter)
		me.Append(err)

		err = j.upstreamRepo.ReplaceUpstreams(ctx, jobsWithUpstreams)
		me.Append(err)

		j.logger.Debug("uploading [%d] jobs of project [%s] namespace [%s] to scheduler", len(jobs), projectName, namespaceName)
		err = j.uploadJobs(ctx, jobTenant, jobs, nil, nil)
		me.Append(err)
	}

	return me.ToErr()
}

func (j *JobService) RefreshResourceDownstream(ctx context.Context, resourceURNs []resource.URN, logWriter writer.LogWriter) error {
	downstreams, err := j.downstreamRepo.GetDownstreamBySources(ctx, resourceURNs)
	if err != nil {
		j.logger.Error("error identifying job downstream for given resources: %s", err)
		return err
	}

	groupedDownstreams := j.groupDownstreamPerProject(downstreams)

	me := errors.NewMultiError("refresh downstream errors")
	for projectName, downstreams := range groupedDownstreams {
		jobNames := make([]string, len(downstreams))
		for i, d := range downstreams {
			jobNames[i] = d.Name().String()
		}

		status := "successful"
		if err := j.Refresh(ctx, projectName, nil, jobNames, logWriter); err != nil {
			j.logger.Error("error refreshing downstream jobs for project [%s]: %s", projectName, err)
			me.Append(err)

			status = "failed"
		}

		counter := telemetry.NewCounter(job.MetricJobRefreshResourceDownstream, map[string]string{
			"project": projectName.String(),
			"status":  status,
		})
		counter.Add(float64(len(jobNames)))
	}

	return me.ToErr()
}

func validateDeleteJob(jobTenant tenant.Tenant, downstreams []*job.Downstream, toDeleteMap map[job.FullName]*job.Spec, jobToDelete *job.Spec, logWriter writer.LogWriter, me *errors.MultiError) bool {
	notDeleted, safeToDelete := isJobSafeToDelete(toDeleteMap, job.DownstreamList(downstreams).GetDownstreamFullNames())

	if !safeToDelete {
		// TODO: refactor to put the log writer outside
		errorMsg := fmt.Sprintf("deletion of job %s will fail. job is being used by %s", jobToDelete.Name().String(), job.FullNames(notDeleted).String())
		logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] %s", jobTenant.NamespaceName().String(), errorMsg))
		me.Append(errors.NewError(errors.ErrFailedPrecond, job.EntityJob, errorMsg))
		return false
	}

	return true
}

func isJobSafeToDelete(toDeleteMap map[job.FullName]*job.Spec, downstreamFullNames []job.FullName) ([]job.FullName, bool) {
	notDeleted := []job.FullName{}
	for _, downstreamFullName := range downstreamFullNames {
		if _, ok := toDeleteMap[downstreamFullName]; !ok {
			notDeleted = append(notDeleted, downstreamFullName)
		}
	}

	return notDeleted, len(notDeleted) == 0
}

func (j *JobService) getAllDownstreams(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, visited map[job.FullName]bool) ([]*job.Downstream, error) {
	currentJobFullName := job.FullNameFrom(projectName, jobName)
	downstreams := []*job.Downstream{}
	visited[currentJobFullName] = true
	childJobs, err := j.downstreamRepo.GetDownstreamByJobName(ctx, projectName, jobName)
	if err != nil {
		j.logger.Error("error getting downstream jobs for job [%s]: %s", jobName, err)
		return nil, err
	}
	for _, childJob := range childJobs {
		downstreams = append(downstreams, childJob)
		if visited[childJob.FullName()] {
			continue
		}
		childDownstreams, err := j.getAllDownstreams(ctx, childJob.ProjectName(), childJob.Name(), visited)
		if err != nil {
			j.logger.Error("error getting all downstreams for job [%s]: %s", childJob.Name(), err)
			return nil, err
		}
		downstreams = append(downstreams, childDownstreams...)
	}
	return downstreams, nil
}

func (*JobService) getIdentifierToJobsMap(jobsToValidateMap map[job.Name]*job.WithUpstream) map[string][]*job.WithUpstream {
	identifierToJobsMap := make(map[string][]*job.WithUpstream)
	for _, jobEntity := range jobsToValidateMap {
		jobIdentifiers := []string{jobEntity.Job().FullName()}
		if jobDestination := jobEntity.Job().Destination().String(); jobDestination != "" {
			jobIdentifiers = append(jobIdentifiers, jobDestination)
		}
		for _, jobIdentifier := range jobIdentifiers {
			if _, ok := identifierToJobsMap[jobIdentifier]; !ok {
				identifierToJobsMap[jobIdentifier] = []*job.WithUpstream{}
			}
			identifierToJobsMap[jobIdentifier] = append(identifierToJobsMap[jobIdentifier], jobEntity)
		}
	}
	return identifierToJobsMap
}

func (j *JobService) resolveAndSaveUpstreams(ctx context.Context, jobTenant tenant.Tenant, logWriter writer.LogWriter, jobsToResolve ...[]*job.Job) error {
	var allJobsToResolve []*job.Job
	for _, group := range jobsToResolve {
		allJobsToResolve = append(allJobsToResolve, group...)
	}
	if len(allJobsToResolve) == 0 {
		j.logger.Warn("no jobs to be resolved")
		return nil
	}

	me := errors.NewMultiError("resolve and save upstream errors")

	j.logger.Debug("resolving upstreams for %d jobs of project [%s] namespace [%s]", len(allJobsToResolve), jobTenant.ProjectName(), jobTenant.NamespaceName())
	jobsWithUpstreams, err := j.upstreamResolver.BulkResolve(ctx, jobTenant.ProjectName(), allJobsToResolve, logWriter)
	me.Append(err)

	j.logger.Debug("replacing upstreams for %d jobs of project [%s] namespace [%s]", len(jobsWithUpstreams), jobTenant.ProjectName(), jobTenant.NamespaceName())
	err = j.upstreamRepo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	return me.ToErr()
}

func (j *JobService) bulkAdd(ctx context.Context, tenantWithDetails *tenant.WithDetails, specsToAdd []*job.Spec, logWriter writer.LogWriter) ([]*job.Job, error) {
	me := errors.NewMultiError("bulk add specs errors")

	jobsToAdd, err := j.generateJobs(ctx, tenantWithDetails, specsToAdd, logWriter)
	me.Append(err)

	if len(jobsToAdd) == 0 {
		j.logger.Warn("no jobs to be added")
		return nil, me.ToErr()
	}

	// TODO: consider do add inside parallel
	addedJobs, err := j.jobRepo.Add(ctx, jobsToAdd)
	if err != nil {
		j.logger.Error("error adding jobs for namespace [%s]: %s", tenantWithDetails.Namespace().Name(), err)
		logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] add jobs failure found: %s", tenantWithDetails.Namespace().Name().String(), err.Error()))
		me.Append(err)
	}
	if len(addedJobs) > 0 {
		logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] successfully added %d jobs", tenantWithDetails.Namespace().Name().String(), len(addedJobs)))
		raiseJobEventMetric(tenantWithDetails.ToTenant(), job.MetricJobEventStateAdded, len(addedJobs))
	}
	for _, addedJob := range addedJobs {
		if addedJob.Spec().Schedule().CatchUp() {
			msg := fmt.Sprintf("catchup for job %s is enabled", addedJob.GetName())
			j.logger.Warn(msg)
			logWriter.Write(writer.LogLevelWarning, msg)
		}
	}

	return addedJobs, me.ToErr()
}

func (j *JobService) bulkUpdate(ctx context.Context, tenantWithDetails *tenant.WithDetails, specsToUpdate []*job.Spec, logWriter writer.LogWriter) ([]*job.Job, error) {
	me := errors.NewMultiError("bulk update specs errors")

	jobsToUpdate, err := j.generateJobs(ctx, tenantWithDetails, specsToUpdate, logWriter)
	me.Append(err)
	if len(jobsToUpdate) == 0 {
		j.logger.Warn("no jobs to be updated")
		return nil, me.ToErr()
	}
	updatedJobs, err := j.jobRepo.Update(ctx, jobsToUpdate)
	if err != nil {
		j.logger.Error("error updating jobs for namespace [%s]: %s", tenantWithDetails.Namespace().Name(), err)
		logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] update jobs failure found: %s", tenantWithDetails.Namespace().Name().String(), err.Error()))
		me.Append(err)
	}
	if len(updatedJobs) > 0 {
		logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] successfully updated %d jobs", tenantWithDetails.Namespace().Name().String(), len(updatedJobs)))
		raiseJobEventMetric(tenantWithDetails.ToTenant(), job.MetricJobEventStateUpdated, len(updatedJobs))
	}

	return updatedJobs, me.ToErr()
}

func (j *JobService) bulkDelete(ctx context.Context, jobTenant tenant.Tenant, toDelete []*job.Spec, logWriter writer.LogWriter) ([]job.Name, error) {
	me := errors.NewMultiError("bulk delete specs errors")
	var deletedJobNames []job.Name
	toDeleteMap := job.Specs(toDelete).ToFullNameAndSpecMap(jobTenant.ProjectName())

	alreadyDeleted := map[job.FullName]bool{}
	for _, spec := range toDelete {
		// TODO: reuse Delete method and pass forceFlag as false
		fullName := job.FullNameFrom(jobTenant.ProjectName(), spec.Name())
		downstreams, err := j.getAllDownstreams(ctx, jobTenant.ProjectName(), spec.Name(), map[job.FullName]bool{})
		if err != nil {
			j.logger.Error("error getting downstreams for job [%s]: %s", spec.Name(), err)
			logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] pre-delete check for job %s failed: %s", jobTenant.NamespaceName().String(), spec.Name().String(), err.Error()))
			me.Append(err)
			continue
		}

		isSafeToDelete := validateDeleteJob(jobTenant, downstreams, toDeleteMap, spec, logWriter, me)
		if !isSafeToDelete {
			j.logger.Warn("job [%s] is not safe to be deleted", spec.Name())
			continue
		}

		logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] deleting job %s", jobTenant.NamespaceName().String(), spec.Name().String()))

		isDeletionFail := false
		for i := len(downstreams) - 1; i >= 0 && !isDeletionFail; i-- {
			if alreadyDeleted[downstreams[i].FullName()] {
				continue
			}
			if err = j.jobRepo.Delete(ctx, downstreams[i].ProjectName(), downstreams[i].Name(), false); err != nil {
				j.logger.Error("error deleting [%s] as downstream of [%s]", downstreams[i].Name(), spec.Name())
				logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] deleting job %s failed: %s", downstreams[i].NamespaceName().String(), downstreams[i].Name().String(), err.Error()))
				me.Append(err)
				isDeletionFail = true
			} else {
				alreadyDeleted[downstreams[i].FullName()] = true
				j.raiseDeleteEvent(jobTenant, spec.Name())
				raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleted, 1)
				deletedJobNames = append(deletedJobNames, downstreams[i].Name())
			}
		}

		if alreadyDeleted[fullName] || isDeletionFail {
			j.logger.Warn("job [%s] deletion is skipped [already deleted or failure in deleting downstreams]", spec.Name())
			continue
		}
		if err = j.jobRepo.Delete(ctx, jobTenant.ProjectName(), spec.Name(), false); err != nil {
			j.logger.Error("error deleting job [%s]", spec.Name())
			logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] deleting job %s failed: %s", jobTenant.NamespaceName().String(), spec.Name().String(), err.Error()))
			me.Append(err)
		} else {
			alreadyDeleted[fullName] = true
			j.raiseDeleteEvent(jobTenant, spec.Name())
			raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleted, 1)
			deletedJobNames = append(deletedJobNames, spec.Name())
		}
	}

	if len(deletedJobNames) > 0 {
		logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] successfully deleted %d jobs", jobTenant.NamespaceName().String(), len(deletedJobNames)))
	}

	if len(deletedJobNames) < len(toDelete) {
		totalFailed := len(toDelete) - len(deletedJobNames)
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleteFailed, totalFailed)
	}
	return deletedJobNames, me.ToErr()
}

func (j *JobService) differentiateSpecs(jobTenant tenant.Tenant, existingJobs []*job.Job, incomingSpecs []*job.Spec, jobNamesWithInvalidSpec []job.Name) (added, modified, deleted, unmodified, dirty []*job.Spec) {
	var addedSpecs, modifiedSpecs, unmodifiedSpecs, deletedSpecs, unmodifiedDirtySpecs []*job.Spec

	existingJobsMap := job.Jobs(existingJobs).GetNameMap()
	for _, jobNameToSkip := range jobNamesWithInvalidSpec {
		j.logger.Error("[%s] received invalid spec for %s", jobTenant.NamespaceName().String(), jobNameToSkip)
		delete(existingJobsMap, jobNameToSkip)
	}

	for _, incomingSpec := range incomingSpecs {
		jobObj, ok := existingJobsMap[incomingSpec.Name()]
		if !ok {
			addedSpecs = append(addedSpecs, incomingSpec)
			continue
		}

		if !reflect.DeepEqual(jobObj.Spec(), incomingSpec) {
			modifiedSpecs = append(modifiedSpecs, incomingSpec) // if a spec is modified then do not consider its dirty past
			continue
		}

		if existingJobsMap[incomingSpec.Name()].IsDirty() {
			unmodifiedDirtySpecs = append(unmodifiedDirtySpecs, jobObj.Spec())
		}
		unmodifiedSpecs = append(unmodifiedSpecs, incomingSpec)
	}

	incomingSpecsMap := job.Specs(incomingSpecs).ToNameAndSpecMap()
	for existingJobName, existingJob := range existingJobsMap {
		if _, ok := incomingSpecsMap[existingJobName]; !ok {
			deletedSpecs = append(deletedSpecs, existingJob.Spec())
		}
	}
	return addedSpecs, modifiedSpecs, deletedSpecs, unmodifiedSpecs, unmodifiedDirtySpecs
}

func (j *JobService) generateJobs(ctx context.Context, tenantWithDetails *tenant.WithDetails, specs []*job.Spec, logWriter writer.LogWriter) ([]*job.Job, error) {
	me := errors.NewMultiError("bulk generate jobs errors")

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, spec := range specs {
		runner.Add(func(currentSpec *job.Spec, lw writer.LogWriter) func() (interface{}, error) {
			return func() (interface{}, error) {
				generatedJob, err := j.generateJob(ctx, tenantWithDetails, currentSpec)
				if err != nil {
					errorMsg := fmt.Sprintf("error generating job [%s]: %s", currentSpec.Name().String(), err.Error())
					j.logger.Error(errorMsg)
					lw.Write(writer.LogLevelError, fmt.Sprintf("[%s] unable to generate job %s: %s", tenantWithDetails.Namespace().Name().String(), currentSpec.Name().String(), err.Error()))
					return nil, fmt.Errorf("%s", errorMsg)
				}
				lw.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] processing job %s", tenantWithDetails.Namespace().Name().String(), currentSpec.Name().String()))
				return generatedJob, nil
			}
		}(spec, logWriter))
	}

	var generatedJobs []*job.Job
	for _, result := range runner.Run() {
		if result.Err != nil {
			me.Append(result.Err)
		} else {
			specVal := result.Val.(*job.Job)
			generatedJobs = append(generatedJobs, specVal)
		}
	}
	return generatedJobs, me.ToErr()
}

func (j *JobService) compileConfigs(configs job.Config, tnnt *tenant.WithDetails) map[string]string {
	tmplCtx := compiler.PrepareContext(
		compiler.From(tnnt.GetConfigs()).WithName("proj").WithKeyPrefix(projectConfigPrefix),
		compiler.From(tnnt.SecretsMap()).WithName("secret"),
	)

	compiledConfigs := map[string]string{}
	for key, val := range configs {
		compiledConf, err := j.engine.CompileString(val, tmplCtx)
		if err != nil {
			j.logger.Warn("template compilation encountered suppressed error: %s", err.Error())
			compiledConf = val
		}
		compiledConfigs[key] = compiledConf
	}
	return compiledConfigs
}

func (j *JobService) generateJob(ctx context.Context, tenantWithDetails *tenant.WithDetails, spec *job.Spec) (*job.Job, error) {
	if windowConfig := spec.WindowConfig(); windowConfig.Type() == window.Preset {
		if _, err := tenantWithDetails.Project().GetPreset(windowConfig.Preset); err != nil {
			errorMsg := fmt.Sprintf("error getting preset for job [%s]: %s", spec.Name(), err)
			return nil, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg)
		}
	}

	destination, err := j.generateDestinationURN(ctx, tenantWithDetails, spec)
	if err != nil {
		return nil, err
	}
	sources, err := j.identifyUpstreamURNs(ctx, tenantWithDetails, spec)
	if err != nil {
		return nil, err
	}

	return job.NewJob(tenantWithDetails.ToTenant(), spec, destination, sources, false), nil
}

func (*JobService) buildDAGTree(rootName job.Name, jobMap map[job.Name]*job.WithUpstream, identifierToJobMap map[string][]*job.WithUpstream) *tree.MultiRootTree {
	rootJob := jobMap[rootName]

	dagTree := tree.NewMultiRootTree()
	dagTree.AddNode(tree.NewTreeNode(rootJob))

	for _, childJob := range jobMap {
		childNode := findOrCreateDAGNode(dagTree, childJob)
		for _, upstream := range childJob.Upstreams() {
			identifier := upstream.Resource().String()
			if _, ok := identifierToJobMap[identifier]; !ok {
				identifier = upstream.FullName()
				if _, ok := identifierToJobMap[identifier]; !ok {
					// resource maybe from external optimus or outside project,
					// as of now, we're not providing the capability to build tree from external optimus or outside project. skip
					continue
				}
			}

			parents := identifierToJobMap[identifier]
			for _, parentJob := range parents {
				parentNode := findOrCreateDAGNode(dagTree, parentJob)
				parentNode.AddDependent(childNode)
				dagTree.AddNode(parentNode)
			}
		}

		if len(childJob.Upstreams()) == 0 {
			dagTree.MarkRoot(childNode)
		}
	}

	return dagTree
}

// sources: https://github.com/goto/optimus/blob/a6dafbc1fbeb8e1f1eb8d4a6e9582ada4a7f639e/job/replay.go#L101
func findOrCreateDAGNode(dagTree *tree.MultiRootTree, dag tree.TreeData) *tree.TreeNode {
	node, ok := dagTree.GetNodeByName(dag.GetName())
	if !ok {
		node = tree.NewTreeNode(dag)
		dagTree.AddNode(node)
	}
	return node
}

func (j *JobService) GetJobBasicInfo(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, spec *job.Spec) (*job.Job, writer.BufferedLogger) {
	var subjectJob *job.Job
	var logger writer.BufferedLogger
	var err error
	if spec != nil {
		tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
		if err != nil {
			j.logger.Info("error getting tenant details: %s", err)
			logger.Write(writer.LogLevelError, fmt.Sprintf("unable to get tenant detail, err: %v", err))
			return nil, logger
		}
		subjectJob, err = j.generateJob(ctx, tenantWithDetails, spec)
		if err != nil {
			j.logger.Info("error generating job for [%s]: %s", spec.Name(), err)
			logger.Write(writer.LogLevelError, fmt.Sprintf("unable to generate job, err: %v", err))
			return nil, logger
		}
	} else {
		subjectJob, err = j.Get(ctx, jobTenant, jobName)
		if err != nil {
			j.logger.Info("error getting job [%s]: %s", jobName, err)
			logger.Write(writer.LogLevelError, fmt.Sprintf("unable to get job, err: %v", err))
			return nil, logger
		}
	}

	if len(subjectJob.Sources()) == 0 {
		j.logger.Warn("no job sources detected")
		logger.Write(writer.LogLevelInfo, "no job sources detected")
	}

	if subjectJob.Spec().Schedule().CatchUp() {
		logger.Write(writer.LogLevelWarning, "catchup is enabled")
	}

	if dupDestJobNames, err := j.getJobNamesWithSameDestination(ctx, subjectJob); err != nil {
		logger.Write(writer.LogLevelError, "could not perform duplicate job destination check, err: "+err.Error())
	} else if dupDestJobNames != "" {
		logger.Write(writer.LogLevelWarning, "job already exists with same Destination: "+subjectJob.Destination().String()+" existing jobNames: "+dupDestJobNames)
	}

	return subjectJob, logger
}

func (j *JobService) getJobNamesWithSameDestination(ctx context.Context, subjectJob *job.Job) (string, error) {
	sameDestinationJobs, err := j.jobRepo.GetAllByResourceDestination(ctx, subjectJob.Destination())
	if err != nil {
		j.logger.Error("error getting all jobs by destination [%s]: %s", subjectJob.Destination(), err)
		return "", err
	}
	var jobNames []string
	for _, sameDestinationJob := range sameDestinationJobs {
		if sameDestinationJob.FullName() == subjectJob.FullName() {
			continue
		}
		jobNames = append(jobNames, sameDestinationJob.GetName())
	}
	return strings.Join(jobNames, ", "), nil
}

func (j *JobService) GetUpstreamsToInspect(ctx context.Context, subjectJob *job.Job, localJob bool) ([]*job.Upstream, error) {
	logWriter := writer.NewLogWriter(j.logger)
	if localJob {
		return j.upstreamResolver.Resolve(ctx, subjectJob, logWriter)
	}
	return j.upstreamRepo.GetUpstreams(ctx, subjectJob.ProjectName(), subjectJob.Spec().Name())
}

func (j *JobService) GetDownstream(ctx context.Context, subjectJob *job.Job, localJob bool) ([]*job.Downstream, error) {
	if localJob {
		return j.downstreamRepo.GetDownstreamByDestination(ctx, subjectJob.ProjectName(), subjectJob.Destination())
	}
	return j.downstreamRepo.GetDownstreamByJobName(ctx, subjectJob.ProjectName(), subjectJob.Spec().Name())
}

func (j *JobService) raiseCreateEvent(job *job.Job) {
	jobEvent, err := event.NewJobCreatedEvent(job)
	if err != nil {
		j.logger.Error("error creating event for job create: %s", err)
		return
	}
	j.eventHandler.HandleEvent(jobEvent)
}

func (j *JobService) raiseUpdateEvent(job *job.Job, impactType job.UpdateImpact) {
	jobEvent, err := event.NewJobUpdateEvent(job, impactType)
	if err != nil {
		j.logger.Error("error creating event for job update: %s", err)
		return
	}
	j.eventHandler.HandleEvent(jobEvent)
}

func (j *JobService) raiseStateChangeEvent(tnnt tenant.Tenant, jobName job.Name, state job.State) {
	jobEvent, err := event.NewJobStateChangeEvent(tnnt, jobName, state)
	if err != nil {
		j.logger.Error("error creating event for job state change: %s", err)
		return
	}
	j.eventHandler.HandleEvent(jobEvent)
}

func (j *JobService) raiseDeleteEvent(tnnt tenant.Tenant, jobName job.Name) {
	jobEvent, err := event.NewJobDeleteEvent(tnnt, jobName)
	if err != nil {
		j.logger.Error("error creating event for job delete: %s", err)
		return
	}
	j.eventHandler.HandleEvent(jobEvent)
}

func (*JobService) groupDownstreamPerProject(downstreams []*job.Downstream) map[tenant.ProjectName][]*job.Downstream {
	output := make(map[tenant.ProjectName][]*job.Downstream)
	for _, d := range downstreams {
		output[d.ProjectName()] = append(output[d.ProjectName()], d)
	}
	return output
}

func raiseJobEventMetric(jobTenant tenant.Tenant, state string, metricValue int) {
	telemetry.NewCounter(job.MetricJobEvent, map[string]string{
		"project":   jobTenant.ProjectName().String(),
		"namespace": jobTenant.NamespaceName().String(),
		"status":    state,
	}).Add(float64(metricValue))
}

func (j *JobService) identifyUpstreamURNs(ctx context.Context, tenantWithDetails *tenant.WithDetails, spec *job.Spec) ([]resource.URN, error) {
	taskName := spec.Task().Name().String()
	taskConfig := spec.Task().Config()
	compileConfigs := j.compileConfigs(taskConfig, tenantWithDetails)
	assets := spec.Asset()

	return j.pluginService.IdentifyUpstreams(ctx, taskName, compileConfigs, assets)
}

func (j *JobService) generateDestinationURN(ctx context.Context, tenantWithDetails *tenant.WithDetails, spec *job.Spec) (resource.URN, error) {
	taskName := spec.Task().Name().String()
	taskConfig := spec.Task().Config()
	compileConfigs := j.compileConfigs(taskConfig, tenantWithDetails)

	return j.pluginService.ConstructDestinationURN(ctx, taskName, compileConfigs)
}

func (j *JobService) Validate(ctx context.Context, request dto.ValidateRequest) (map[job.Name][]dto.ValidateResult, error) {
	if err := j.validateRequest(request); err != nil {
		registerJobValidationMetric(request.Tenant, dto.StagePreparation, false)
		return nil, err
	}

	if err := j.validateDuplication(request); err != nil {
		registerJobValidationMetric(request.Tenant, dto.StagePreparation, false)
		return nil, err
	}

	tenantDetails, err := j.tenantDetailsGetter.GetDetails(ctx, request.Tenant)
	if err != nil {
		registerJobValidationMetric(request.Tenant, dto.StagePreparation, false)
		return nil, err
	}

	jobsToValidate, err := j.getJobsToValidate(ctx, tenantDetails, request)
	if err != nil {
		registerJobValidationMetric(request.Tenant, dto.StagePreparation, false)
		return nil, err
	}

	registerJobValidationMetric(request.Tenant, dto.StagePreparation, true)

	if output := j.validateTenantOnEachJob(request.Tenant, jobsToValidate); len(output) > 0 {
		return output, nil
	}

	if request.DeletionMode {
		return j.validateJobsForDeletion(ctx, request.Tenant, jobsToValidate), nil
	}

	if result, err := j.validateCyclic(ctx, request.Tenant, jobsToValidate); err != nil {
		return nil, err
	} else if len(result) > 0 {
		return result, nil
	}

	return j.validateJobs(ctx, tenantDetails, jobsToValidate)
}

func (*JobService) validateRequest(request dto.ValidateRequest) error {
	if len(request.JobNames) > 0 && len(request.JobSpecs) > 0 {
		return errors.NewError(errors.ErrInvalidArgument, job.EntityJob, "job names and specs can not be specified together")
	}

	if len(request.JobNames) == 0 && len(request.JobSpecs) == 0 {
		return errors.NewError(errors.ErrInvalidArgument, job.EntityJob, "job names and job specs are both empty")
	}

	if request.DeletionMode && len(request.JobNames) == 0 {
		return errors.NewError(errors.ErrInvalidArgument, job.EntityJob, "deletion job only accepts job names")
	}

	return nil
}

func (*JobService) validateDuplication(request dto.ValidateRequest) error {
	jobNameCountMap := make(map[string]int)
	if len(request.JobNames) > 0 {
		for _, name := range request.JobNames {
			jobNameCountMap[name]++
		}
	} else {
		for _, spec := range request.JobSpecs {
			jobNameCountMap[spec.Name().String()]++
		}
	}

	var duplicated []string
	for name, count := range jobNameCountMap {
		if count > 1 {
			duplicated = append(duplicated, name)
		}
	}

	if len(duplicated) > 0 {
		message := fmt.Sprintf("the following jobs are duplicated: [%s]", strings.Join(duplicated, ", "))
		return errors.NewError(errors.ErrInvalidArgument, job.EntityJob, message)
	}

	return nil
}

func (j *JobService) getJobsToValidate(ctx context.Context, tenantDetails *tenant.WithDetails, request dto.ValidateRequest) ([]*job.Job, error) {
	if len(request.JobNames) > 0 {
		return j.getJobByNames(ctx, request.Tenant, request.JobNames)
	}

	return j.generateJobs(ctx, tenantDetails, request.JobSpecs, writer.NewSafeBufferedLogger())
}

func (j *JobService) getJobByNames(ctx context.Context, tnnt tenant.Tenant, jobNames []string) ([]*job.Job, error) {
	me := errors.NewMultiError("getting job by name")

	var retrievedJobs []*job.Job
	for _, name := range jobNames {
		jobName, err := job.NameFrom(name)
		if err != nil {
			me.Append(err)
			continue
		}

		subjectJob, err := j.jobRepo.GetByJobName(ctx, tnnt.ProjectName(), jobName)
		if err != nil {
			me.Append(err)
			continue
		}

		retrievedJobs = append(retrievedJobs, subjectJob)
	}

	return retrievedJobs, me.ToErr()
}

func (j *JobService) validateJobsForDeletion(ctx context.Context, jobTenant tenant.Tenant, jobsToDelete []*job.Job) map[job.Name][]dto.ValidateResult {
	specByFullName := job.Jobs(jobsToDelete).GetFullNameToSpecMap()

	output := make(map[job.Name][]dto.ValidateResult)
	for _, job := range jobsToDelete {
		output[job.Spec().Name()] = j.validateOneJobForDeletion(ctx, jobTenant, job.Spec(), specByFullName)
	}

	return output
}

func (j *JobService) validateOneJobForDeletion(
	ctx context.Context,
	jobTenant tenant.Tenant, spec *job.Spec,
	specByFullName map[job.FullName]*job.Spec,
) []dto.ValidateResult {
	downstreams, err := j.getAllDownstreams(ctx, jobTenant.ProjectName(), spec.Name(), make(map[job.FullName]bool))
	if err != nil {
		result := dto.ValidateResult{
			Stage: dto.StageDeletionValidation,
			Messages: []string{
				"downstreams can not be fetched",
				err.Error(),
			},
			Success: false,
		}

		registerJobValidationMetric(jobTenant, dto.StageDeletionValidation, false)

		return []dto.ValidateResult{result}
	}

	me := errors.NewMultiError("validating job for deletion errors")
	safeToDelete := validateDeleteJob(jobTenant, downstreams, specByFullName, spec, writer.NewSafeBufferedLogger(), me)

	var messages []string
	success := true
	if !safeToDelete {
		messages = []string{"job is not safe for deletion"}
		if err := me.ToErr(); err != nil {
			messages = append(messages, err.Error())
		}

		success = false
	} else {
		messages = []string{"job is safe for deletion"}
	}

	registerJobValidationMetric(jobTenant, dto.StageDeletionValidation, success)

	return []dto.ValidateResult{
		{
			Stage:    dto.StageDeletionValidation,
			Messages: messages,
			Success:  success,
		},
	}
}

func (*JobService) validateTenantOnEachJob(rootTnnt tenant.Tenant, jobsToValidate []*job.Job) map[job.Name][]dto.ValidateResult {
	output := make(map[job.Name][]dto.ValidateResult)
	for _, subjectJob := range jobsToValidate {
		tnnt := subjectJob.Tenant()
		if tnnt.ProjectName() != rootTnnt.ProjectName() || tnnt.NamespaceName() != rootTnnt.NamespaceName() {
			result := dto.ValidateResult{
				Stage: dto.StageTenantValidation,
				Messages: []string{
					fmt.Sprintf("current tenant is [%s.%s]", tnnt.ProjectName(), tnnt.NamespaceName()),
					fmt.Sprintf("expected tenant is [%s.%s]", rootTnnt.ProjectName(), rootTnnt.NamespaceName()),
				},
				Success: false,
			}

			output[subjectJob.Spec().Name()] = []dto.ValidateResult{result}

			registerJobValidationMetric(rootTnnt, dto.StageTenantValidation, false)
		} else {
			registerJobValidationMetric(rootTnnt, dto.StageTenantValidation, true)
		}
	}

	return output
}

func (j *JobService) validateJobs(ctx context.Context, tenantDetails *tenant.WithDetails, jobsToValidate []*job.Job) (map[job.Name][]dto.ValidateResult, error) {
	output := make(map[job.Name][]dto.ValidateResult)
	for _, subjectJob := range jobsToValidate {
		output[subjectJob.Spec().Name()] = j.validateOneJob(ctx, tenantDetails, subjectJob)
	}

	return output, nil
}

func (j *JobService) validateOneJob(ctx context.Context, tenantDetails *tenant.WithDetails, subjectJob *job.Job) []dto.ValidateResult {
	destination, err := j.generateDestinationURN(ctx, tenantDetails, subjectJob.Spec())
	if err != nil {
		result := dto.ValidateResult{
			Stage: dto.StageDestinationValidation,
			Messages: []string{
				"can not generate destination resource",
				err.Error(),
			},
			Success: false,
		}

		registerJobValidationMetric(tenantDetails.ToTenant(), dto.StageDestinationValidation, false)

		return []dto.ValidateResult{result}
	}

	var output []dto.ValidateResult

	result := j.validateDestination(ctx, tenantDetails.ToTenant(), destination)
	output = append(output, result)

	result = j.validateSource(ctx, tenantDetails, subjectJob.Spec())
	output = append(output, result)

	result = j.validateWindow(tenantDetails, subjectJob.Spec().WindowConfig())
	output = append(output, result)

	result = j.validateRun(ctx, subjectJob, destination)
	output = append(output, result)

	result = j.validateUpstream(ctx, subjectJob)
	output = append(output, result)

	return output
}

func (j *JobService) validateCyclic(ctx context.Context, tnnt tenant.Tenant, incomingJobs []*job.Job) (map[job.Name][]dto.ValidateResult, error) {
	existingJobs, err := j.jobRepo.GetAllByTenant(ctx, tnnt)
	if err != nil {
		return nil, err
	}

	if err := j.upstreamResolver.CheckStaticResolvable(ctx, tnnt, incomingJobs, writer.NewSafeBufferedLogger()); err != nil {
		return nil, err
	}

	jobsToValidate := j.getCombinedJobsToValidate(incomingJobs, existingJobs)

	jobsWithUpstream, err := j.upstreamResolver.BulkResolve(ctx, tnnt.ProjectName(), jobsToValidate, writer.NewSafeBufferedLogger())
	if err != nil {
		return nil, err
	}

	// NOTE: only check cyclic deps across internal upstreams (sources), need further discussion to check cyclic deps for external upstream
	// assumption, all job specs from input are also the job within same project
	jobWithUpstreamPerJobName := j.getJobWithUpstreamPerJobName(jobsWithUpstream)
	identifierToJobsMap := j.getIdentifierToJobsMap(jobWithUpstreamPerJobName)

	output := make(map[job.Name][]dto.ValidateResult)

	isJobNameCyclic := make(map[string]bool)
	for _, subjectJob := range incomingJobs {
		dagTree := j.buildDAGTree(subjectJob.Spec().Name(), jobWithUpstreamPerJobName, identifierToJobsMap)
		cyclicNames, err := dagTree.ValidateCyclic()

		for _, name := range cyclicNames {
			isJobNameCyclic[name] = true
		}

		if err != nil && isJobNameCyclic[subjectJob.GetName()] {
			output[subjectJob.Spec().Name()] = []dto.ValidateResult{
				{
					Stage: dto.StageCyclicValidation,
					Messages: append([]string{
						"cyclic dependency is detected",
					}, cyclicNames...),
					Success: false,
				},
			}

			registerJobValidationMetric(tnnt, dto.StageCyclicValidation, false)
		} else {
			registerJobValidationMetric(tnnt, dto.StageCyclicValidation, true)
		}
	}

	return output, nil
}

func (*JobService) getCombinedJobsToValidate(incoming, existing []*job.Job) []*job.Job {
	uniqueJobs := make(map[job.Name]*job.Job)
	for _, j := range existing {
		uniqueJobs[j.Spec().Name()] = j
	}

	for _, j := range incoming {
		uniqueJobs[j.Spec().Name()] = j
	}

	var output []*job.Job
	for _, j := range uniqueJobs {
		output = append(output, j)
	}

	return output
}

func (*JobService) getJobWithUpstreamPerJobName(jobsWithUpstream []*job.WithUpstream) map[job.Name]*job.WithUpstream {
	jobsToValidateMap := make(map[job.Name]*job.WithUpstream)
	for _, jobWithUpstream := range jobsWithUpstream {
		jobsToValidateMap[jobWithUpstream.Name()] = jobWithUpstream
	}

	return jobsToValidateMap
}

func (j *JobService) validateUpstream(ctx context.Context, subjectJob *job.Job) dto.ValidateResult {
	if _, err := j.upstreamResolver.Resolve(ctx, subjectJob, writer.NewSafeBufferedLogger()); err != nil {
		registerJobValidationMetric(subjectJob.Tenant(), dto.StageUpstreamValidation, false)

		return dto.ValidateResult{
			Stage: dto.StageUpstreamValidation,
			Messages: []string{
				"can not resolve upstream",
				err.Error(),
			},
			Success: false,
		}
	}

	registerJobValidationMetric(subjectJob.Tenant(), dto.StageUpstreamValidation, true)

	return dto.ValidateResult{
		Stage:    dto.StageUpstreamValidation,
		Messages: []string{"no issue"},
		Success:  true,
	}
}

func (j *JobService) validateDestination(ctx context.Context, tnnt tenant.Tenant, destination resource.URN) dto.ValidateResult {
	if destination.IsZero() {
		registerJobValidationMetric(tnnt, dto.StageDestinationValidation, true)

		return dto.ValidateResult{
			Stage:    dto.StageDestinationValidation,
			Messages: []string{"no issue"},
			Success:  true,
		}
	}

	message, success := j.validateResourceURN(ctx, tnnt, destination)
	registerJobValidationMetric(tnnt, dto.StageDestinationValidation, success)

	return dto.ValidateResult{
		Stage:    dto.StageDestinationValidation,
		Messages: []string{fmt.Sprintf("%s: %s", destination.String(), message)},
		Success:  success,
	}
}

func (j *JobService) validateSource(ctx context.Context, tenantWithDetails *tenant.WithDetails, spec *job.Spec) dto.ValidateResult {
	sourceURNs, err := j.identifyUpstreamURNs(ctx, tenantWithDetails, spec)
	if err != nil {
		registerJobValidationMetric(tenantWithDetails.ToTenant(), dto.StageSourceValidation, false)

		return dto.ValidateResult{
			Stage: dto.StageSourceValidation,
			Messages: []string{
				"can not identify the resource sources of the job",
				err.Error(),
			},
			Success: false,
		}
	}

	if len(sourceURNs) == 0 {
		return dto.ValidateResult{
			Stage:    dto.StageSourceValidation,
			Messages: []string{"no issue"},
			Success:  true,
		}
	}

	messages := make([]string, len(sourceURNs))
	success := true

	for i, urn := range sourceURNs {
		currentMessage, currentSuccess := j.validateResourceURN(ctx, tenantWithDetails.ToTenant(), urn)
		if !currentSuccess {
			success = false
		}

		messages[i] = fmt.Sprintf("%s: %s", urn.String(), currentMessage)
	}

	registerJobValidationMetric(tenantWithDetails.ToTenant(), dto.StageSourceValidation, success)

	return dto.ValidateResult{
		Stage:    dto.StageSourceValidation,
		Messages: messages,
		Success:  success,
	}
}

func (j *JobService) validateResourceURN(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (string, bool) {
	activeInDB := true
	if rsc, err := j.resourceChecker.GetByURN(ctx, tnnt, urn); err != nil {
		j.logger.Warn("suppress error is encountered when reading resource from db: %v", err)
		activeInDB = false
	} else {
		switch rsc.Status() {
		case resource.StatusToDelete, resource.StatusDeleted:
			activeInDB = false
		}
	}

	existInStore, err := j.resourceChecker.ExistInStore(ctx, tnnt, urn)
	if err != nil {
		return err.Error(), false
	}

	if activeInDB && existInStore {
		return "no issue", true
	}

	if existInStore {
		return "resource exists in store but not in db", true
	}

	if activeInDB {
		return "resource exists in db but not in store", false
	}

	return "resource does not exist in both db and store", false
}

func (j *JobService) validateRun(ctx context.Context, subjectJob *job.Job, destination resource.URN) dto.ValidateResult {
	referenceTime := time.Now()
	runConfigs, err := j.getRunConfigs(referenceTime, subjectJob.Spec())
	if err != nil {
		registerJobValidationMetric(subjectJob.Tenant(), dto.StageRunCompileValidation, false)

		return dto.ValidateResult{
			Stage: dto.StageRunCompileValidation,
			Messages: []string{
				"can not get run config",
				err.Error(),
			},
			Success: false,
		}
	}

	var messages []string
	success := true

	jobWithDetails := j.getSchedulerJobWithDetail(subjectJob, destination)
	for _, config := range runConfigs {
		var msg string
		if _, err := j.jobRunInputCompiler.Compile(ctx, jobWithDetails, config, referenceTime); err != nil {
			success = false

			msg = fmt.Sprintf("compiling [%s] with type [%s] failed with error: %v", config.Executor.Name, config.Executor.Type.String(), err)
		} else {
			msg = fmt.Sprintf("compiling [%s] with type [%s] contains no issue", config.Executor.Name, config.Executor.Type.String())
		}

		messages = append(messages, msg)
	}

	registerJobValidationMetric(subjectJob.Tenant(), dto.StageRunCompileValidation, success)

	return dto.ValidateResult{
		Stage:    dto.StageRunCompileValidation,
		Messages: messages,
		Success:  success,
	}
}

func (*JobService) getSchedulerJobWithDetail(subjectJob *job.Job, destination resource.URN) *scheduler.JobWithDetails {
	hooks := make([]*scheduler.Hook, len(subjectJob.Spec().Hooks()))
	for i, hook := range subjectJob.Spec().Hooks() {
		hooks[i] = &scheduler.Hook{
			Name:   hook.Name(),
			Config: hook.Config(),
		}
	}

	return &scheduler.JobWithDetails{
		Name: scheduler.JobName(subjectJob.GetName()),
		Job: &scheduler.Job{
			Name:        scheduler.JobName(subjectJob.GetName()),
			Tenant:      subjectJob.Tenant(),
			Destination: destination,
			Task: &scheduler.Task{
				Name:   string(subjectJob.Spec().Task().Name()),
				Config: subjectJob.Spec().Task().Config(),
			},
			Hooks:        hooks,
			WindowConfig: subjectJob.Spec().WindowConfig(),
			Assets:       subjectJob.Spec().Asset(),
		},
		JobMetadata: &scheduler.JobMetadata{
			Version:     subjectJob.Spec().Version(),
			Owner:       subjectJob.Spec().Owner(),
			Description: subjectJob.Spec().Description(),
			Labels:      subjectJob.Spec().Labels(),
		},
		Schedule: &scheduler.Schedule{
			DependsOnPast: subjectJob.Spec().Schedule().DependsOnPast(),
			CatchUp:       subjectJob.Spec().Schedule().CatchUp(),
			Interval:      subjectJob.Spec().Schedule().Interval(),
		},
	}
}

func (*JobService) getRunConfigs(referenceTime time.Time, spec *job.Spec) ([]scheduler.RunConfig, error) {
	var runConfigs []scheduler.RunConfig

	executor, err := scheduler.ExecutorFromEnum(spec.Task().Name().String(), scheduler.ExecutorTask.String())
	if err != nil {
		return nil, err
	}

	runConfig, err := scheduler.RunConfigFrom(executor, referenceTime, "")
	if err != nil {
		return nil, err
	}

	runConfigs = append(runConfigs, runConfig)

	for _, hook := range spec.Hooks() {
		executor, err := scheduler.ExecutorFromEnum(hook.Name(), scheduler.ExecutorHook.String())
		if err != nil {
			return nil, err
		}

		runConfig, err := scheduler.RunConfigFrom(executor, referenceTime, "")
		if err != nil {
			return nil, err
		}

		runConfigs = append(runConfigs, runConfig)
	}

	return runConfigs, nil
}

func (*JobService) validateWindow(tenantDetails *tenant.WithDetails, windowConfig window.Config) dto.ValidateResult {
	if windowType := windowConfig.Type(); windowType == window.Preset {
		preset := windowConfig.Preset
		if _, err := tenantDetails.Project().GetPreset(preset); err != nil {
			registerJobValidationMetric(tenantDetails.ToTenant(), dto.StageWindowValidation, false)

			return dto.ValidateResult{
				Stage: dto.StageWindowValidation,
				Messages: []string{
					fmt.Sprintf("window preset [%s] is not found within project", preset),
					err.Error(),
				},
				Success: false,
			}
		}
	}

	registerJobValidationMetric(tenantDetails.ToTenant(), dto.StageWindowValidation, true)

	return dto.ValidateResult{
		Stage:    dto.StageWindowValidation,
		Messages: []string{"no issue"},
		Success:  true,
	}
}

func registerJobValidationMetric(tnnt tenant.Tenant, stage dto.ValidateStage, success bool) {
	counter := telemetry.NewCounter(job.MetricJobValidation, map[string]string{
		"project":   tnnt.ProjectName().String(),
		"namespace": tnnt.NamespaceName().String(),
		"stage":     stage.String(),
		"success":   fmt.Sprintf("%t", success),
	})

	counter.Add(1)
}

func (j *JobService) GetDownstreamByResourceURN(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (job.DownstreamList, error) {
	var dependentJobs []*job.Downstream

	jobs, err := j.downstreamRepo.GetDownstreamBySources(ctx, []resource.URN{urn})
	if err != nil {
		return nil, err
	}

	for i := range jobs {
		if jobs[i].ProjectName() != tnnt.ProjectName() || jobs[i].NamespaceName() != tnnt.NamespaceName() {
			continue
		}

		dependentJobs = append(dependentJobs, jobs[i])
	}

	return dependentJobs, nil
}
