//go:build !unit_test

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/interval"
	postgres "github.com/goto/optimus/internal/store/postgres/scheduler"
)

func TestPostgresJobOperatorRepository(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("test-proj", "test-ns")
	currentTime := time.Now().UTC()
	scheduledAt := currentTime.Add(-time.Hour)
	operatorStartTime := currentTime
	operatorEndTime := currentTime.Add(time.Hour)
	slaDefinitionInSec := int64(3600) // seconds
	start := currentTime.Truncate(time.Hour * 24)
	end := start.Add(time.Hour * 24)
	intr := interval.NewInterval(start, end)

	t.Run("CreateOperatorRun", func(t *testing.T) {
		t.Run("creates a operator run", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db, nil)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)

			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			operatorRunRepo := postgres.NewOperatorRunRepository(db)
			err = operatorRunRepo.CreateOperatorRun(ctx, "some-operator-name", scheduler.OperatorSensor, jobRun.ID, operatorStartTime)
			assert.Nil(t, err)

			operatorRun, err := operatorRunRepo.GetOperatorRun(ctx, "some-operator-name", scheduler.OperatorSensor, jobRun.ID)
			assert.Nil(t, err)
			assert.Equal(t, operatorStartTime.UTC().Format(time.RFC1123), operatorRun.StartTime.UTC().Format(time.RFC1123))
		})
	})
	t.Run("GetOperatorRun", func(t *testing.T) {
		t.Run("should return not found error", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db, nil)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)

			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			operatorRunRepo := postgres.NewOperatorRunRepository(db)
			operatorRun, err := operatorRunRepo.GetOperatorRun(ctx, "some-operator-name", scheduler.OperatorHook, jobRun.ID)
			assert.True(t, errors.IsErrorType(err, errors.ErrNotFound))
			assert.Nil(t, operatorRun)
		})
		t.Run("should return InvalidArgument error when wrong operator name", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db, nil)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)

			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			operatorRunRepo := postgres.NewOperatorRunRepository(db)
			operatorRun, err := operatorRunRepo.GetOperatorRun(ctx, "some-operator-name", "some-other-operator", jobRun.ID)
			assert.True(t, errors.IsErrorType(err, errors.ErrInvalidArgument))
			assert.Nil(t, operatorRun)
		})
	})

	t.Run("ListLatestOperatorRunsByJobRunID", func(t *testing.T) {
		t.Run("returns the latest row per operator name, scoped to the given operator type", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db, nil)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)

			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			operatorRunRepo := postgres.NewOperatorRunRepository(db)

			// first attempt for "task-a": fails, then gets retried as a new row
			err = operatorRunRepo.CreateOperatorRun(ctx, "task-a", scheduler.OperatorTask, jobRun.ID, operatorStartTime)
			assert.Nil(t, err)
			firstAttempt, err := operatorRunRepo.GetOperatorRun(ctx, "task-a", scheduler.OperatorTask, jobRun.ID)
			assert.Nil(t, err)
			err = operatorRunRepo.UpdateOperatorRun(ctx, scheduler.OperatorTask, firstAttempt.ID, operatorEndTime, scheduler.StateFailed)
			assert.Nil(t, err)

			// sleep so the retry's created_at strictly sorts after the first attempt's
			time.Sleep(10 * time.Millisecond)
			err = operatorRunRepo.CreateOperatorRun(ctx, "task-a", scheduler.OperatorTask, jobRun.ID, operatorStartTime.Add(time.Minute))
			assert.Nil(t, err)

			// a different operator name in the same table
			err = operatorRunRepo.CreateOperatorRun(ctx, "task-b", scheduler.OperatorTask, jobRun.ID, operatorStartTime)
			assert.Nil(t, err)

			// same job run, different operator-type table -- must not leak into the task list
			err = operatorRunRepo.CreateOperatorRun(ctx, "wait_upstream", scheduler.OperatorSensor, jobRun.ID, operatorStartTime)
			assert.Nil(t, err)

			runs, err := operatorRunRepo.ListLatestOperatorRunsByJobRunID(ctx, scheduler.OperatorTask, jobRun.ID)
			assert.Nil(t, err)
			assert.Len(t, runs, 2)

			byName := map[string]*scheduler.OperatorRun{}
			for _, r := range runs {
				byName[r.Name] = r
			}

			taskA, ok := byName["task-a"]
			assert.True(t, ok)
			assert.Equal(t, scheduler.StateRunning, taskA.Status) // the retry, not the failed first attempt
			assert.Equal(t, scheduler.OperatorTask, taskA.OperatorType)

			taskB, ok := byName["task-b"]
			assert.True(t, ok)
			assert.Equal(t, scheduler.OperatorTask, taskB.OperatorType)
		})

		t.Run("returns an empty slice when the job run has no operator runs of that type", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db, nil)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)

			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			operatorRunRepo := postgres.NewOperatorRunRepository(db)
			runs, err := operatorRunRepo.ListLatestOperatorRunsByJobRunID(ctx, scheduler.OperatorTask, jobRun.ID)
			assert.Nil(t, err)
			assert.Empty(t, runs)
		})
	})

	t.Run("UpdateOperatorRun", func(t *testing.T) {
		t.Run("updates a specific operator run by job id", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db, nil)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec)
			assert.Nil(t, err)

			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			operatorRunRepo := postgres.NewOperatorRunRepository(db)
			err = operatorRunRepo.CreateOperatorRun(ctx, "some-operator-name", scheduler.OperatorTask, jobRun.ID, operatorStartTime)
			assert.Nil(t, err)

			operatorRun, err := operatorRunRepo.GetOperatorRun(ctx, "some-operator-name", scheduler.OperatorTask, jobRun.ID)
			assert.Nil(t, err)
			assert.Equal(t, operatorStartTime.UTC().Format(time.RFC1123), operatorRun.StartTime.UTC().Format(time.RFC1123))

			err = operatorRunRepo.UpdateOperatorRun(ctx, scheduler.OperatorTask, operatorRun.ID, operatorEndTime, scheduler.StateFailed)
			assert.Nil(t, err)

			operatorRun, err = operatorRunRepo.GetOperatorRun(ctx, "some-operator-name", scheduler.OperatorTask, jobRun.ID)
			assert.Nil(t, err)
			assert.Equal(t, operatorEndTime.UTC().Format(time.RFC1123), operatorRun.EndTime.UTC().Format(time.RFC1123))
			assert.Equal(t, scheduler.StateFailed, operatorRun.Status)
		})
	})
}
