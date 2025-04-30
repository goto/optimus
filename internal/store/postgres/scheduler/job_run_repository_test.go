//go:build !unit_test

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/interval"
	postgres "github.com/goto/optimus/internal/store/postgres/scheduler"
)

func TestPostgresJobRunRepository(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("test-proj", "test-ns")
	currentTime := time.Now().UTC()
	scheduledAt := currentTime.Add(-time.Hour)
	slaDefinitionInSec := int64(3600) // seconds
	start := currentTime.Truncate(time.Hour * 24)
	end := start.Add(time.Hour * 24)
	intr := interval.NewInterval(start, end)

	t.Run("Create", func(t *testing.T) {
		t.Run("creates a job run", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)
			assert.Equal(t, jobAName, jobRun.JobName.String())
		})
	})
	t.Run("GetByID", func(t *testing.T) {
		t.Run("gets a specific job run by ID", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			jobRunByID, err := jobRunRepo.GetByID(ctx, scheduler.JobRunID(jobRun.ID))
			assert.Nil(t, err)
			assert.EqualValues(t, jobRunByID, jobRun)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("updates a specific job run by id", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			jobEndTime := currentTime.Add(-time.Minute)
			err = jobRunRepo.Update(ctx, jobRun.ID, jobEndTime, scheduler.StateSuccess)
			assert.Nil(t, err)

			jobRunByID, err := jobRunRepo.GetByID(ctx, scheduler.JobRunID(jobRun.ID))
			assert.Nil(t, err)
			assert.EqualValues(t, scheduler.StateSuccess, jobRunByID.State)
			assert.Equal(t, jobEndTime.UTC().Format(time.RFC1123), jobRunByID.EndTime.UTC().Format(time.RFC1123))
		})
	})
	t.Run("UpdateSLA", func(t *testing.T) {
		t.Run("updates jobs sla alert firing status", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			scheduleTimes := []time.Time{scheduledAt}

			err = jobRunRepo.UpdateSLA(ctx, jobAName, tnnt.ProjectName(), scheduleTimes)
			assert.Nil(t, err)

			jobRunByID, err := jobRunRepo.GetByID(ctx, scheduler.JobRunID(jobRun.ID))
			assert.Nil(t, err)
			assert.True(t, jobRunByID.SLAAlert)
		})
	})
	t.Run("UpdateMonitoring", func(t *testing.T) {
		t.Run("updates job run monitoring", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.NoError(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.NoError(t, err)

			monitoring := map[string]any{
				"slot_millis":           float64(5000),
				"total_bytes_processed": float64(2500),
			}

			err = jobRunRepo.UpdateMonitoring(ctx, jobRun.ID, monitoring)
			assert.NoError(t, err)

			jobRunByID, err := jobRunRepo.GetByID(ctx, scheduler.JobRunID(jobRun.ID))
			assert.NoError(t, err)
			assert.EqualValues(t, monitoring, jobRunByID.Monitoring)
		})
	})
	t.Run("GetLatestRun", func(t *testing.T) {
		t.Run("gets the latest job run", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			latestJobRun, err := jobRunRepo.GetLatestRun(ctx, tnnt.ProjectName(), jobAName, nil)
			assert.Nil(t, err)
			assert.EqualValues(t, jobRun, latestJobRun)
		})

		t.Run("returns error if no job run found", func(t *testing.T) {
			db := dbSetup()
			jobRunRepo := postgres.NewJobRunRepository(db)

			_, err := jobRunRepo.GetLatestRun(ctx, tnnt.ProjectName(), jobAName, nil)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "no record for job")
		})

		t.Run("gets the latest job run with specific state", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			jobEndTime := currentTime.Add(-time.Minute)
			err = jobRunRepo.Update(ctx, jobRun.ID, jobEndTime, scheduler.StateSuccess)
			assert.Nil(t, err)
			successState := scheduler.StateSuccess
			latestJobRun, err := jobRunRepo.GetLatestRun(ctx, tnnt.ProjectName(), jobAName, &successState)
			assert.Nil(t, err)
			assert.EqualValues(t, scheduler.StateSuccess, latestJobRun.State)
		})
	})

	t.Run("GetRunsByTimeRange", func(t *testing.T) {
		t.Run("gets job runs within a specific time range", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			since := scheduledAt.Add(-2 * time.Hour)
			until := scheduledAt.Add(2 * time.Hour)
			jobRuns, err := jobRunRepo.GetRunsByTimeRange(ctx, tnnt.ProjectName(), jobAName, nil, since, until)
			assert.Nil(t, err)
			assert.Len(t, jobRuns, 1)
			assert.Equal(t, jobRun.ID, jobRuns[0].ID)
		})

		t.Run("returns empty list if no job runs found in the time range", func(t *testing.T) {
			db := dbSetup()
			jobRunRepo := postgres.NewJobRunRepository(db)

			since := scheduledAt.Add(-2 * time.Hour)
			until := scheduledAt.Add(-1 * time.Hour)
			jobRuns, err := jobRunRepo.GetRunsByTimeRange(ctx, tnnt.ProjectName(), jobAName, nil, since, until)
			assert.Nil(t, err)
			assert.Len(t, jobRuns, 0)
		})

		t.Run("gets job runs within a specific time range and state", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			jobEndTime := currentTime.Add(-time.Minute)
			err = jobRunRepo.Update(ctx, jobRun.ID, jobEndTime, scheduler.StateSuccess)
			assert.Nil(t, err)

			since := scheduledAt.Add(-2 * time.Hour)
			until := scheduledAt.Add(2 * time.Hour)
			filterState := scheduler.StateSuccess
			jobRuns, err := jobRunRepo.GetRunsByTimeRange(ctx, tnnt.ProjectName(), jobAName, &filterState, since, until)
			assert.Nil(t, err)
			assert.Len(t, jobRuns, 1)
			assert.Equal(t, scheduler.StateSuccess, jobRuns[0].State)
		})
	})

	t.Run("GetRunsByInterval", func(t *testing.T) {
		t.Run("gets job runs within a specific interval", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			since := scheduledAt.Add(-2 * time.Hour)
			until := scheduledAt.Add(2 * time.Hour)
			jobRuns, err := jobRunRepo.GetRunsByTimeRange(ctx, tnnt.ProjectName(), jobAName, nil, since, until)
			assert.Nil(t, err)
			assert.Len(t, jobRuns, 1)
			assert.Equal(t, jobRun.ID, jobRuns[0].ID)
		})

		t.Run("returns empty list if no job runs found in the time range", func(t *testing.T) {
			db := dbSetup()
			jobRunRepo := postgres.NewJobRunRepository(db)

			since := scheduledAt.Add(-2 * time.Hour)
			until := scheduledAt.Add(-1 * time.Hour)
			jobRuns, err := jobRunRepo.GetRunsByTimeRange(ctx, tnnt.ProjectName(), jobAName, nil, since, until)
			assert.Nil(t, err)
			assert.Len(t, jobRuns, 0)
		})

		t.Run("gets job runs within a specific time range and state", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			jobEndTime := currentTime.Add(-time.Minute)
			err = jobRunRepo.Update(ctx, jobRun.ID, jobEndTime, scheduler.StateSuccess)
			assert.Nil(t, err)

			since := scheduledAt.Add(-2 * time.Hour)
			until := scheduledAt.Add(2 * time.Hour)
			filterState := scheduler.StateSuccess
			jobRuns, err := jobRunRepo.GetRunsByTimeRange(ctx, tnnt.ProjectName(), jobAName, &filterState, since, until)
			assert.Nil(t, err)
			assert.Len(t, jobRuns, 1)
			assert.Equal(t, scheduler.StateSuccess, jobRuns[0].State)
		})
	})
}
