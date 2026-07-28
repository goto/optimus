//go:build !unit_test

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	postgres "github.com/goto/optimus/internal/store/postgres/scheduler"
)

// airflow_sync_state has no foreign keys to project/job (see
// docs/docs/rfcs/20260727_manual_state_override_reconciliation.md), so these tests use
// bare project names without needing addJobs' project/namespace/job fixtures.
func TestPostgresAirflowSyncStateRepository(t *testing.T) {
	ctx := context.Background()
	currentTime := time.Now().UTC().Truncate(time.Second)

	t.Run("ClaimWindow", func(t *testing.T) {
		t.Run("claims a fresh window", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-claim-fresh")
			workerID := uuid.New()

			id, claimed, err := repo.ClaimWindow(ctx, projectName, currentTime, currentTime.Add(time.Minute), workerID, time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)
			assert.NotEqual(t, uuid.Nil, id)
		})

		t.Run("second claim on the same window is rejected, not errored", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-claim-conflict")
			start, end := currentTime, currentTime.Add(time.Minute)

			id1, claimed1, err := repo.ClaimWindow(ctx, projectName, start, end, uuid.New(), time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed1)

			id2, claimed2, err := repo.ClaimWindow(ctx, projectName, start, end, uuid.New(), time.Minute)
			assert.NoError(t, err)
			assert.False(t, claimed2)
			assert.Equal(t, uuid.Nil, id2)
			assert.NotEqual(t, id1, id2)
		})

		t.Run("different projects can claim overlapping windows independently", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			start, end := currentTime, currentTime.Add(time.Minute)

			_, claimed1, err := repo.ClaimWindow(ctx, tenant.ProjectName("proj-a"), start, end, uuid.New(), time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed1)

			_, claimed2, err := repo.ClaimWindow(ctx, tenant.ProjectName("proj-b"), start, end, uuid.New(), time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed2)
		})
	})

	t.Run("GetWatermark", func(t *testing.T) {
		t.Run("returns nil when the project has no windows yet", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)

			watermark, err := repo.GetWatermark(ctx, tenant.ProjectName("proj-no-history"))
			assert.NoError(t, err)
			assert.Nil(t, watermark)
		})

		t.Run("an in_progress window does not move the watermark", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-inprogress-only")

			_, claimed, err := repo.ClaimWindow(ctx, projectName, currentTime, currentTime.Add(time.Minute), uuid.New(), time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)

			watermark, err := repo.GetWatermark(ctx, projectName)
			assert.NoError(t, err)
			assert.Nil(t, watermark)
		})

		t.Run("a completed window advances the watermark to its end_time", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-completed")
			end := currentTime.Add(time.Minute)
			workerID := uuid.New()

			id, claimed, err := repo.ClaimWindow(ctx, projectName, currentTime, end, workerID, time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)

			completed, err := repo.CompleteWindow(ctx, id, workerID, nil, 0, 0)
			assert.NoError(t, err)
			assert.True(t, completed)

			watermark, err := repo.GetWatermark(ctx, projectName)
			assert.NoError(t, err)
			if assert.NotNil(t, watermark) {
				assert.True(t, watermark.Equal(end), "expected watermark %s to equal window end_time %s", watermark, end)
			}
		})

		t.Run("a failed window still advances the watermark, so it does not block later windows", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-failed-advances")
			end := currentTime.Add(time.Minute)

			// negative lock duration => locked_until is already in the past, so the claimed
			// window is immediately eligible for FailExhaustedWindows with maxAttempts=1
			// (attempt_count is 1 right after ClaimWindow).
			_, claimed, err := repo.ClaimWindow(ctx, projectName, currentTime, end, uuid.New(), -time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)

			failedCount, err := repo.FailExhaustedWindows(ctx, projectName, 1, "boom")
			assert.NoError(t, err)
			assert.EqualValues(t, 1, failedCount)

			watermark, err := repo.GetWatermark(ctx, projectName)
			assert.NoError(t, err)
			if assert.NotNil(t, watermark) {
				assert.True(t, watermark.Equal(end))
			}
		})
	})

	t.Run("ReclaimStaleWindow", func(t *testing.T) {
		t.Run("returns nil when there is nothing stale", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-nothing-stale")

			_, claimed, err := repo.ClaimWindow(ctx, projectName, currentTime, currentTime.Add(time.Minute), uuid.New(), time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)

			reclaimed, err := repo.ReclaimStaleWindow(ctx, projectName, uuid.New(), time.Minute, 3)
			assert.NoError(t, err)
			assert.Nil(t, reclaimed)
		})

		t.Run("reclaims a window whose lock already expired, bumping attempt_count", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-stale-reclaim")
			start, end := currentTime, currentTime.Add(time.Minute)

			// negative lock duration => already stale the moment it's claimed
			id, claimed, err := repo.ClaimWindow(ctx, projectName, start, end, uuid.New(), -time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)

			newWorkerID := uuid.New()
			reclaimed, err := repo.ReclaimStaleWindow(ctx, projectName, newWorkerID, time.Minute, 3)
			assert.NoError(t, err)
			if assert.NotNil(t, reclaimed) {
				assert.Equal(t, id, reclaimed.ID)
				assert.Equal(t, newWorkerID, reclaimed.WorkerID)
				assert.Equal(t, 2, reclaimed.AttemptCount) // 1 at claim, +1 on reclaim
				assert.Equal(t, scheduler.AirflowSyncInProgress, reclaimed.Status)
			}
		})

		t.Run("does not reclaim a window that already exhausted max attempts", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-exhausted-no-reclaim")

			_, claimed, err := repo.ClaimWindow(ctx, projectName, currentTime, currentTime.Add(time.Minute), uuid.New(), -time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)

			// maxAttempts=1: attempt_count is already 1 right after claiming, so this must not reclaim.
			reclaimed, err := repo.ReclaimStaleWindow(ctx, projectName, uuid.New(), time.Minute, 1)
			assert.NoError(t, err)
			assert.Nil(t, reclaimed)
		})
	})

	t.Run("CompleteWindow", func(t *testing.T) {
		t.Run("succeeds when the worker still holds the lease", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-complete-ok")
			workerID := uuid.New()

			id, claimed, err := repo.ClaimWindow(ctx, projectName, currentTime, currentTime.Add(time.Minute), workerID, time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)

			logID := int64(42)
			completed, err := repo.CompleteWindow(ctx, id, workerID, &logID, 3, 2)
			assert.NoError(t, err)
			assert.True(t, completed)
		})

		t.Run("fails when a different worker attempts to complete it", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-complete-wrong-worker")

			id, claimed, err := repo.ClaimWindow(ctx, projectName, currentTime, currentTime.Add(time.Minute), uuid.New(), time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)

			completed, err := repo.CompleteWindow(ctx, id, uuid.New(), nil, 0, 0)
			assert.NoError(t, err)
			assert.False(t, completed)
		})

		t.Run("fails when the lease already expired, even for the original worker", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-complete-expired-lease")
			workerID := uuid.New()

			// negative lock duration => the lease is already expired by the time we try to complete it,
			// simulating a worker that took too long and got its window reclaimed by someone else.
			id, claimed, err := repo.ClaimWindow(ctx, projectName, currentTime, currentTime.Add(time.Minute), workerID, -time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)

			completed, err := repo.CompleteWindow(ctx, id, workerID, nil, 0, 0)
			assert.NoError(t, err)
			assert.False(t, completed)
		})
	})

	t.Run("RecordAttemptError", func(t *testing.T) {
		t.Run("annotates last_error while the lease is still valid, visible after it later becomes reclaimable", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-record-error")
			workerID := uuid.New()

			// claim with a still-valid lease so the fenced write below has something to succeed
			// against -- a negative lock duration here (as elsewhere in this file, to simulate
			// staleness) would make RecordAttemptError's own fence reject it before we even get
			// to observe it, testing the wrong thing.
			id, claimed, err := repo.ClaimWindow(ctx, projectName, currentTime, currentTime.Add(time.Minute), workerID, time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)

			err = repo.RecordAttemptError(ctx, id, workerID, "boom: airflow unreachable")
			assert.NoError(t, err)

			// backdate the lease directly (bypassing the repository) to deterministically simulate
			// "time passed and the lease expired", so ReclaimStaleWindow can pick it up and we can
			// observe the annotated LastError through its return value.
			_, err = db.Exec(ctx, "UPDATE airflow_sync_state SET locked_until = now() - interval '1 minute' WHERE id = $1", id)
			assert.NoError(t, err)

			reclaimed, err := repo.ReclaimStaleWindow(ctx, projectName, uuid.New(), time.Minute, 5)
			assert.NoError(t, err)
			if assert.NotNil(t, reclaimed) {
				assert.Equal(t, "boom: airflow unreachable", reclaimed.LastError)
				assert.Equal(t, scheduler.AirflowSyncInProgress, reclaimed.Status)
			}
		})

		t.Run("is a no-op when a different worker no longer holds the lease", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewAirflowSyncStateRepository(db)
			projectName := tenant.ProjectName("proj-record-error-wrong-worker")
			workerID := uuid.New()

			id, claimed, err := repo.ClaimWindow(ctx, projectName, currentTime, currentTime.Add(time.Minute), workerID, time.Minute)
			assert.NoError(t, err)
			assert.True(t, claimed)

			// a different worker id should not be able to stamp an error on a lease it doesn't hold
			err = repo.RecordAttemptError(ctx, id, uuid.New(), "should not stick")
			assert.NoError(t, err)

			_, err = db.Exec(ctx, "UPDATE airflow_sync_state SET locked_until = now() - interval '1 minute' WHERE id = $1", id)
			assert.NoError(t, err)

			reclaimed, err := repo.ReclaimStaleWindow(ctx, projectName, uuid.New(), time.Minute, 5)
			assert.NoError(t, err)
			if assert.NotNil(t, reclaimed) {
				assert.Empty(t, reclaimed.LastError)
			}
		})
	})
}
