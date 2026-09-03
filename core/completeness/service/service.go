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
	// EntityCompleteness names this domain for error wrapping, consistent with
	// other domains' EntityX conventions (e.g. scheduler.EntityJobRun).
	EntityCompleteness = "completeness"

	// maxResolvedResources bounds a single request's fan-out, matching the order of
	// magnitude of resolver.ConcurrentLimit (core/job/resolver/upstream_resolver.go).
	maxResolvedResources = 200

	// Concurrency bounds mirror resolver.ConcurrentTicketPerSec/ConcurrentLimit
	// (core/job/resolver/upstream_resolver.go), reusing the same parallel.Runner
	// already used for this shape of fan-out elsewhere in this codebase (e.g.
	// core/job/resolver/dex_upstream_resolver.go).
	fanOutTicketPerSec = 50
	fanOutConcurrency  = 100
)

// UpstreamIdentifier resolves the tables/views an ad hoc query reads from, recursively
// through views down to base tables. Implemented by plugin.PluginService via its
// IdentifyUpstreamsFromQuery method.
type UpstreamIdentifier interface {
	IdentifyUpstreamsFromQuery(ctx context.Context, datastoreName, svcAcc, query string) ([]resource.URN, error)
}

// JobDestinationRepository resolves a resource URN back to the job(s) that produce it.
// Implemented by internal/store/postgres/job.JobRepository.
type JobDestinationRepository interface {
	GetAllByResourceDestination(ctx context.Context, resourceDestination resource.URN) ([]*job.Job, error)
}

// JobRunRepository fetches the run recorded for a job at an exact scheduled_at.
// Implemented by internal/store/postgres/scheduler.JobRunRepository.
type JobRunRepository interface {
	GetByScheduledAt(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error)
}

// ThirdPartyClient reports whether a resource is managed outside Optimus (e.g. Dex).
// Aliased from core/scheduler/service so callers don't need a second import for the
// same interface; ext/dex.Client is the concrete implementation wired in production.
type ThirdPartyClient = schedulerService.ThirdPartyClient

// Config holds the credentials the upstream identifiers need to fetch view DDL for an
// ad hoc query that has no job/task context to source a per-job secret from, plus the
// two cache TTLs (see resolutionCache/runStatusCache on Service below).
//
// MaxcomputeServiceAccount is sourced from serve.global_mc_service_account (see
// config.GlobalMcServiceAccount). BigqueryServiceAccount has no confirmed source yet --
// every other caller of the upstream identifiers (job compilation) sources it from
// per-job compiled secrets (plugin/plugin_service.go), and there is no global BQ
// credential equivalent to the MC one today. Wire it in once confirmed; do not ship
// with a placeholder value.
type Config struct {
	MaxcomputeServiceAccount string
	BigqueryServiceAccount   string

	// ResolutionCacheTTL and RunStatusCacheTTL come from
	// serve.completeness.{resolution_cache_ttl,run_status_cache_ttl}
	// (config.CompletenessConfig). A zero value disables that cache entirely
	// (cache.New's documented behavior) rather than crashing, so this is safe to leave
	// unset, just uncached.
	ResolutionCacheTTL time.Duration
	RunStatusCacheTTL  time.Duration
}

// managedJobRef is the destination-resolution outcome for one job producing a table --
// everything needed to build a ManagedTable except its live run status, which is
// looked up separately (and cached with a much shorter TTL) since it changes far more
// often than which job owns a table.
type managedJobRef struct {
	tableName        string
	optimusProject   string
	optimusNamespace string
	jobName          string
	tenant           tenant.Tenant
	cronInterval     string
}

// resolutionEntry is what resolutionCache stores per resource URN: either the job(s)
// that produce it, or (when unmanaged) whether Dex manages it instead.
type resolutionEntry struct {
	managed      []managedJobRef
	managedByDex bool
}

// runStatusKey identifies exactly one selected scheduled run. ScheduledAt is part of
// the key so that a cadence rollover to a new slot (SelectScheduledAt returning a
// different time) is automatically a cache miss on a fresh key, rather than needing
// explicit invalidation when the relevant slot changes.
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

	// resolutionCache holds "who owns this table" (job/project/namespace, or
	// Dex-managed) -- long TTL, since this only changes on deploy.
	resolutionCache *cache.Cache[resource.URN, resolutionEntry]
	// runStatusCache holds the run state for one exact scheduled_at -- short TTL,
	// since this is the part callers need close to live.
	runStatusCache *cache.Cache[runStatusKey, *scheduler.JobRun]
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

// Close stops both caches' background janitors. Call it once, at server shutdown.
func (s *Service) Close() {
	s.resolutionCache.Close()
	s.runStatusCache.Close()
}

// OverallStatus mirrors the eventual proto enum without depending on the generated
// package, so this domain layer compiles independently of the proton bump landing.
type OverallStatus string

const (
	OverallStatusComplete    OverallStatus = "COMPLETE"
	OverallStatusNotComplete OverallStatus = "NOT_COMPLETE"
)

// RunStatus is the run selected by SelectScheduledAt for a managed table's job. A nil
// *RunStatus on ManagedTable means either the job hasn't reached its next scheduled
// occurrence yet, or no run was recorded for the selected scheduled_at -- both map to
// NOT_COMPLETE at the handler layer.
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

// CheckQueryCompleteness parses query, resolves every table/view it reads from
// (recursively through views), and classifies each as Optimus-managed (with its
// selected run's status) or unmanaged (optionally Dex-managed).
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

	// perURNResult carries one resolved table's outcome back from the parallel runner;
	// exactly one of managedTables/unmanaged is populated on success.
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

			// Destination has no DB-level uniqueness guarantee (plain index, not
			// unique -- see migration 000004_update_job_table_add_destination_column),
			// so more than one job can legitimately claim the same table. Surface all
			// of them rather than silently picking one, unlike
			// internal_upstream_resolver.go's resolveInferredUpstream which takes [0].
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

// overallStatus is COMPLETE only if every managed table's selected run succeeded.
// Tables Optimus doesn't manage never affect it -- Optimus can't attest to what it
// doesn't manage. Vacuously COMPLETE when there are no managed tables at all.
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
		// Best-effort classification: a lookup failure shouldn't fail the whole
		// request, it just means this table can't be confirmed as Dex-managed.
		return false
	}
	return managed
}

// resolveDestination is the loader behind resolutionCache: which job(s), if any,
// produce urn, or (if none) whether Dex manages it instead.
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
		})
	}
	return resolutionEntry{managed: refs}, nil
}

// getRunStatus computes the currently-relevant scheduled_at for ref (fresh on every
// call -- this is cheap and depends on "now", so it's never itself cached) and looks up
// that run through runStatusCache. A nil *RunStatus means either the job hasn't reached
// its relevant scheduled occurrence yet, or no run was recorded for it -- both map to
// NOT_COMPLETE at the aggregation layer.
func (s *Service) getRunStatus(ctx context.Context, ref managedJobRef) (*RunStatus, error) {
	if ref.cronInterval == "" {
		return nil, nil //nolint:nilnil // no schedule to evaluate against, report as never-run
	}

	jobCron, err := cron.ParseCronSchedule(ref.cronInterval)
	if err != nil {
		return nil, errors.InternalError(EntityCompleteness, "unable to parse cron interval for job "+ref.jobName, err)
	}

	scheduledAt, hasSchedule := SelectScheduledAt(jobCron, time.Now().In(JKT))
	if !hasSchedule {
		return nil, nil //nolint:nilnil // scheduled today but hasn't fired yet -> NOT_COMPLETE via nil Run
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
				return nil, nil //nolint:nilnil // cache "no run yet" too -- avoids repeated misses during a burst
			}
			return nil, err
		}
		return run, nil
	})
	if err != nil {
		return nil, errors.Wrap(EntityCompleteness, "fetching run for job "+ref.jobName, err)
	}
	if run == nil {
		return nil, nil //nolint:nilnil // no run recorded for the selected scheduled_at -> NOT_COMPLETE via nil Run
	}

	return &RunStatus{
		State:       run.State,
		ScheduledAt: run.ScheduledAt,
		StartTime:   run.StartTime,
		EndTime:     run.EndTime,
	}, nil
}
