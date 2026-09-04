package service

import (
	"context"
	"fmt"
	"time"

	"github.com/kushsharma/parallel"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	schedulerService "github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cache"
	"github.com/goto/optimus/internal/lib/cron"
)

const (
	EntityCompleteness = "completeness"

	// maxResolvedResources bounds a single request's fan-out (cf.
	// resolver.ConcurrentLimit in core/job/resolver/upstream_resolver.go).
	maxResolvedResources = 200
	fanOutTicketPerSec   = 50
	fanOutConcurrency    = 100
)

// UpstreamIdentifier resolves the tables/views an ad hoc query reads from, recursively
// through views down to base tables. Implemented by plugin.PluginService.
type UpstreamIdentifier interface {
	IdentifyUpstreamsFromQuery(ctx context.Context, datastoreName, svcAcc, query string) ([]resource.URN, error)
}

// JobDestinationRepository Implemented by internal/store/postgres/job.JobRepository.
type JobDestinationRepository interface {
	GetAllByResourceDestination(ctx context.Context, resourceDestination resource.URN) ([]*job.Job, error)
}

type JobRunRepository interface {
	GetByScheduledAt(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error)
}

type ThirdPartyClient = schedulerService.ThirdPartyClient

type Config struct {
	MaxcomputeServiceAccount string
	BigqueryServiceAccount   string

	// A zero cache TTL disables that cache (see internal/lib/cache.New)
	ResolutionCacheTTL time.Duration
	RunStatusCacheTTL  time.Duration
}

// managedJobRef is a resolved table's producing job, everything needed to build a
// ManagedTable except its live run status (looked up separately, with a much shorter
// cache TTL, since it changes far more often than which job owns a table).
type managedJobRef struct {
	tableName        string
	optimusProject   string
	optimusNamespace string
	jobName          string
	tenant           tenant.Tenant
	cronInterval     string
	isActive         bool
}

type resolutionEntry struct {
	managed      []managedJobRef
	managedByDex bool
}

type runStatusKey struct {
	ProjectName string
	JobName     string
	ScheduledAt time.Time
}

type Service struct {
	upstreamIdentifier UpstreamIdentifier
	jobRepository      JobDestinationRepository
	jobRunRepo         JobRunRepository
	thirdPartyClient   ThirdPartyClient // nil if no third-party resolver is configured
	conf               Config

	resolutionCache *cache.Cache[resource.URN, resolutionEntry]   // who owns a table; long TTL
	runStatusCache  *cache.Cache[runStatusKey, *scheduler.JobRun] // one run's state; short TTL
}

func NewService(
	upstreamIdentifier UpstreamIdentifier,
	jobRepository JobDestinationRepository,
	jobRunRepo JobRunRepository,
	thirdPartyClient ThirdPartyClient,
	conf Config,
) *Service {
	return &Service{
		upstreamIdentifier: upstreamIdentifier,
		jobRepository:      jobRepository,
		jobRunRepo:         jobRunRepo,
		thirdPartyClient:   thirdPartyClient,
		conf:               conf,
		resolutionCache:    cache.New[resource.URN, resolutionEntry](conf.ResolutionCacheTTL),
		runStatusCache:     cache.New[runStatusKey, *scheduler.JobRun](conf.RunStatusCacheTTL),
	}
}

func (s *Service) Close() {
	s.resolutionCache.Close()
	s.runStatusCache.Close()
}

type OverallStatus string

const (
	OverallStatusComplete    OverallStatus = "COMPLETE"
	OverallStatusNotComplete OverallStatus = "NOT_COMPLETE"
)

// RunStatus is the run selected by SelectScheduledAt. A nil *RunStatus on ManagedTable
// means the job hasn't reached its relevant occurrence yet, or no run was recorded for
// it -- both map to NOT_COMPLETE.
type RunStatus struct {
	State       scheduler.State
	ScheduledAt time.Time
	StartTime   time.Time
	EndTime     *time.Time
}

type ManagedTable struct {
	TableName        string
	OptimusProject   string
	OptimusNamespace string
	JobName          string
	Run              *RunStatus
	IsActive         bool // false if the job is currently disabled/paused
}

type UnmanagedTable struct {
	TableName    string
	ManagedByDex bool
}

type Result struct {
	OverallStatus   OverallStatus
	UnmanagedTables []UnmanagedTable
	ManagedTables   []ManagedTable
}

func (s *Service) CheckQueryCompleteness(ctx context.Context, datastoreName, query string) (*Result, error) {
	svcAcc := s.conf.MaxcomputeServiceAccount
	if datastoreName == "bigquery" {
		svcAcc = s.conf.BigqueryServiceAccount
	}

	urns, err := s.upstreamIdentifier.IdentifyUpstreamsFromQuery(ctx, datastoreName, svcAcc, query)
	if err != nil {
		return nil, errors.InternalError(EntityCompleteness, "failed to resolve tables from query", err)
	}
	if len(urns) == 0 {
		return nil, errors.InvalidArgument(EntityCompleteness, "no tables found in query")
	}
	if len(urns) > maxResolvedResources {
		return nil, errors.InvalidArgument(EntityCompleteness,
			fmt.Sprintf("query resolves to %d tables, exceeding the %d limit per request", len(urns), maxResolvedResources))
	}

	// exactly one of managedTables/unmanaged is populated on success
	type perURNResult struct {
		managedTables []ManagedTable
		unmanaged     *UnmanagedTable
	}

	runner := parallel.NewRunner(parallel.WithTicket(fanOutTicketPerSec), parallel.WithLimit(fanOutConcurrency))
	for _, urn := range urns {
		urn := urn
		runner.Add(func() (interface{}, error) {
			res, err := s.resolutionCache.GetOrLoad(ctx, urn, func(ctx context.Context) (resolutionEntry, error) {
				return s.resolveDestination(ctx, urn)
			})
			if err != nil {
				return nil, err
			}

			if len(res.managed) == 0 {
				return &perURNResult{unmanaged: &UnmanagedTable{
					TableName:    urn.GetName(),
					ManagedByDex: res.managedByDex,
				}}, nil
			}

			// destination has no unique constraint, so more than one job can claim a
			// table; surface all of them rather than picking one
			result := &perURNResult{}
			for _, ref := range res.managed {
				run, err := s.getRunStatus(ctx, ref)
				if err != nil {
					return nil, err
				}
				result.managedTables = append(result.managedTables, ManagedTable{
					TableName:        ref.tableName,
					OptimusProject:   ref.optimusProject,
					OptimusNamespace: ref.optimusNamespace,
					JobName:          ref.jobName,
					Run:              run,
					IsActive:         ref.isActive,
				})
			}
			return result, nil
		})
	}

	me := errors.NewMultiError("check query completeness errors")
	var managedTables []ManagedTable
	var unmanagedTables []UnmanagedTable
	for _, state := range runner.Run() {
		if state.Err != nil {
			me.Append(state.Err)
			continue
		}
		result := state.Val.(*perURNResult)
		if result.unmanaged != nil {
			unmanagedTables = append(unmanagedTables, *result.unmanaged)
		}
		managedTables = append(managedTables, result.managedTables...)
	}
	if me.ToErr() != nil {
		return nil, me.ToErr()
	}

	return &Result{
		OverallStatus:   overallStatus(managedTables),
		UnmanagedTables: unmanagedTables,
		ManagedTables:   managedTables,
	}, nil
}

// overallStatus is COMPLETE only if every managed table's selected run succeeded;
// vacuously COMPLETE when there are no managed tables at all.
func overallStatus(managedTables []ManagedTable) OverallStatus {
	for _, mt := range managedTables {
		if mt.Run == nil || mt.Run.State != scheduler.StateSuccess {
			return OverallStatusNotComplete
		}
	}
	return OverallStatusComplete
}

func (s *Service) checkManagedByDex(ctx context.Context, urn resource.URN) bool {
	if s.thirdPartyClient == nil {
		return false
	}
	managed, err := s.thirdPartyClient.IsManaged(ctx, urn)
	if err != nil {
		return false // best-effort: a lookup failure just means "not confirmed", not a request failure
	}
	return managed
}

// resolveDestination is the loader behind resolutionCache.
func (s *Service) resolveDestination(ctx context.Context, urn resource.URN) (resolutionEntry, error) {
	jobs, err := s.jobRepository.GetAllByResourceDestination(ctx, urn)
	if err != nil {
		return resolutionEntry{}, errors.Wrap(EntityCompleteness, "resolving destination for "+urn.String(), err)
	}

	if len(jobs) == 0 {
		return resolutionEntry{managedByDex: s.checkManagedByDex(ctx, urn)}, nil
	}

	refs := make([]managedJobRef, 0, len(jobs))
	for _, j := range jobs {
		refs = append(refs, managedJobRef{
			tableName:        urn.GetName(),
			optimusProject:   j.Tenant().ProjectName().String(),
			optimusNamespace: j.Tenant().NamespaceName().String(),
			jobName:          j.GetName(),
			tenant:           j.Tenant(),
			cronInterval:     j.Spec().Schedule().Interval(),
			isActive:         j.IsEnabled(),
		})
	}
	return resolutionEntry{managed: refs}, nil
}

// getRunStatus computes the relevant scheduled_at fresh (depends on "now", so it's
// never itself cached) and looks up that run through runStatusCache.
func (s *Service) getRunStatus(ctx context.Context, ref managedJobRef) (*RunStatus, error) {
	if ref.cronInterval == "" {
		return nil, nil //nolint:nilnil // no schedule to evaluate against
	}

	jobCron, err := cron.ParseCronSchedule(ref.cronInterval)
	if err != nil {
		return nil, errors.InternalError(EntityCompleteness, "unable to parse cron interval for job "+ref.jobName, err)
	}

	scheduledAt, hasSchedule := SelectScheduledAt(jobCron, time.Now().In(JKT))
	if !hasSchedule {
		return nil, nil //nolint:nilnil // scheduled today but hasn't fired yet
	}

	jobName, err := scheduler.JobNameFrom(ref.jobName)
	if err != nil {
		return nil, errors.InternalError(EntityCompleteness, "invalid job name "+ref.jobName, err)
	}

	key := runStatusKey{ProjectName: ref.optimusProject, JobName: ref.jobName, ScheduledAt: scheduledAt}
	run, err := s.runStatusCache.GetOrLoad(ctx, key, func(ctx context.Context) (*scheduler.JobRun, error) {
		run, err := s.jobRunRepo.GetByScheduledAt(ctx, ref.tenant, jobName, scheduledAt)
		if err != nil {
			if errors.IsErrorType(err, errors.ErrNotFound) {
				return nil, nil //nolint:nilnil // cache "no run yet" too, to absorb repeated misses during a burst
			}
			return nil, err
		}
		return run, nil
	})
	if err != nil {
		return nil, errors.Wrap(EntityCompleteness, "fetching run for job "+ref.jobName, err)
	}
	if run == nil {
		return nil, nil //nolint:nilnil
	}

	return &RunStatus{
		State:       run.State,
		ScheduledAt: run.ScheduledAt,
		StartTime:   run.StartTime,
		EndTime:     run.EndTime,
	}, nil
}
