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
// ad hoc query that has no job/task context to source a per-job secret from.
//
// TODO: neither field has a confirmed source yet. Every other caller of the upstream
// identifiers (job compilation) sources this from per-job compiled secrets
// (plugin/plugin_service.go:112,126) -- there is no existing global/admin service
// account in this repo's config today (checked config/config_server.go). Wire in
// whatever credential your team confirms is appropriate before this goes live; do not
// ship with a placeholder value.
type Config struct {
	MaxcomputeServiceAccount string
	BigqueryServiceAccount   string
}

type Service struct {
	upstreamIdentifier UpstreamIdentifier
	jobDestinationRepo JobDestinationRepository
	jobRunRepo         JobRunRepository
	thirdPartyClient   ThirdPartyClient // nil if no third-party resolver is configured
	conf               Config
}

func NewService(
	upstreamIdentifier UpstreamIdentifier,
	jobDestinationRepo JobDestinationRepository,
	jobRunRepo JobRunRepository,
	thirdPartyClient ThirdPartyClient,
	conf Config,
) *Service {
	return &Service{
		upstreamIdentifier: upstreamIdentifier,
		jobDestinationRepo: jobDestinationRepo,
		jobRunRepo:         jobRunRepo,
		thirdPartyClient:   thirdPartyClient,
		conf:               conf,
	}
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
			jobs, err := s.jobDestinationRepo.GetAllByResourceDestination(ctx, urn)
			if err != nil {
				return nil, errors.Wrap(EntityCompleteness, "resolving destination for "+urn.String(), err)
			}

			if len(jobs) == 0 {
				return &perURNResult{unmanaged: &UnmanagedTable{
					TableName:    urn.GetName(),
					ManagedByDex: s.checkManagedByDex(ctx, urn),
				}}, nil
			}

			// Destination has no DB-level uniqueness guarantee (plain index, not
			// unique -- see migration 000004_update_job_table_add_destination_column),
			// so more than one job can legitimately claim the same table. Surface all
			// of them rather than silently picking one, unlike
			// internal_upstream_resolver.go's resolveInferredUpstream which takes [0].
			result := &perURNResult{}
			for _, j := range jobs {
				managed, err := s.resolveManagedTable(ctx, urn, j)
				if err != nil {
					return nil, err
				}
				result.managedTables = append(result.managedTables, *managed)
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

func (s *Service) resolveManagedTable(ctx context.Context, urn resource.URN, j *job.Job) (*ManagedTable, error) {
	managed := &ManagedTable{
		TableName:        urn.GetName(),
		OptimusProject:   j.Tenant().ProjectName().String(),
		OptimusNamespace: j.Tenant().NamespaceName().String(),
		JobName:          j.GetName(),
	}

	interval := j.Spec().Schedule().Interval()
	if interval == "" {
		return managed, nil // no schedule to evaluate against, report as never-run
	}

	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		return nil, errors.InternalError(EntityCompleteness, "unable to parse cron interval for job "+j.GetName(), err)
	}

	scheduledAt, hasSchedule := SelectScheduledAt(jobCron, time.Now().In(JKT))
	if !hasSchedule {
		return managed, nil // scheduled today but hasn't fired yet -> NOT_COMPLETE via nil Run
	}

	jobName, err := scheduler.JobNameFrom(j.GetName())
	if err != nil {
		return nil, errors.InternalError(EntityCompleteness, "invalid job name "+j.GetName(), err)
	}

	run, err := s.jobRunRepo.GetByScheduledAt(ctx, j.Tenant(), jobName, scheduledAt)
	if err != nil {
		if errors.IsErrorType(err, errors.ErrNotFound) {
			return managed, nil // no run recorded yet at that scheduled_at -> NOT_COMPLETE via nil Run
		}
		return nil, errors.Wrap(EntityCompleteness, "fetching run for job "+j.GetName(), err)
	}

	managed.Run = &RunStatus{
		State:       run.State,
		ScheduledAt: run.ScheduledAt,
		StartTime:   run.StartTime,
		EndTime:     run.EndTime,
	}
	return managed, nil
}
