//go:build unit_test

package notify_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/ext/notify"
	"github.com/goto/optimus/ext/notify/alertmanager"
)

// mockAlertLogRepo is a testify mock for notify.AlertLogRepository.
type mockAlertLogRepo struct {
	mock.Mock
}

func (m *mockAlertLogRepo) InsertWithStatus(ctx context.Context, payload *alertmanager.AlertPayload, status alertmanager.AlertStatus) (uuid.UUID, error) {
	args := m.Called(ctx, payload, status)
	return args.Get(0).(uuid.UUID), args.Error(1)
}

func (m *mockAlertLogRepo) UpdateStatus(ctx context.Context, recordID uuid.UUID, status alertmanager.AlertStatus, message string) error {
	args := m.Called(ctx, recordID, status, message)
	return args.Error(0)
}

func (m *mockAlertLogRepo) HasRecentAlert(ctx context.Context, alertType string, dedupValues map[string]string, since time.Time) (bool, error) {
	args := m.Called(ctx, alertType, dedupValues, since)
	return args.Bool(0), args.Error(1)
}

func TestAlertLogProvider(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNoop()
	testUUID := uuid.New()

	basePayload := func() *alertmanager.AlertPayload {
		return &alertmanager.AlertPayload{
			AlertType: "job_failure",
			Data: map[string]interface{}{
				"job_name": "test-job",
			},
			Labels: map[string]string{
				"team": "data-eng",
			},
		}
	}

	t.Run("should insert PENDING when no dedup config exists for alert type", func(t *testing.T) {
		repo := new(mockAlertLogRepo)
		repo.On("InsertWithStatus", ctx, mock.Anything, alertmanager.StatusPending).Return(testUUID, nil)
		defer repo.AssertExpectations(t)

		provider := notify.NewAlertLogProvider(repo, map[string]notify.AlertDeduplicationConfig{}, logger)

		id, isDeduplicated, err := provider.Insert(ctx, basePayload())

		assert.Nil(t, err)
		assert.Equal(t, testUUID, id)
		assert.False(t, isDeduplicated)
	})

	t.Run("should insert PENDING when alert type has config but is outside active window", func(t *testing.T) {
		repo := new(mockAlertLogRepo)
		repo.On("InsertWithStatus", ctx, mock.Anything, alertmanager.StatusPending).Return(testUUID, nil)
		defer repo.AssertExpectations(t)

		// ActiveWindowStartHour == ActiveWindowEndHour means no hour ever qualifies.
		cfg := map[string]notify.AlertDeduplicationConfig{
			"job_failure": {
				DedupKeys:             []string{"data.job_name"},
				WindowMinutes:         60,
				ActiveWindowStartHour: 0,
				ActiveWindowEndHour:   0,
			},
		}
		provider := notify.NewAlertLogProvider(repo, cfg, logger)

		id, isDeduplicated, err := provider.Insert(ctx, basePayload())

		assert.Nil(t, err)
		assert.Equal(t, testUUID, id)
		assert.False(t, isDeduplicated)
	})

	t.Run("should insert PENDING when within active window but HasRecentAlert returns false", func(t *testing.T) {
		repo := new(mockAlertLogRepo)
		// HasRecentAlert IS expected to be called with the extracted dedup values.
		repo.On("HasRecentAlert", ctx, "job_failure",
			map[string]string{"data.job_name": "test-job"},
			mock.AnythingOfType("time.Time"),
		).Return(false, nil)
		repo.On("InsertWithStatus", ctx, mock.Anything, alertmanager.StatusPending).Return(testUUID, nil)
		defer repo.AssertExpectations(t)

		cfg := map[string]notify.AlertDeduplicationConfig{
			"job_failure": {
				DedupKeys:             []string{"data.job_name"},
				WindowMinutes:         60,
				ActiveWindowStartHour: 0,
				ActiveWindowEndHour:   24, // whole day = always active
			},
		}
		provider := notify.NewAlertLogProvider(repo, cfg, logger)

		id, isDeduplicated, err := provider.Insert(ctx, basePayload())

		assert.Nil(t, err)
		assert.Equal(t, testUUID, id)
		assert.False(t, isDeduplicated)
	})

	t.Run("should insert DEDUPLICATED when within active window and HasRecentAlert returns true", func(t *testing.T) {
		repo := new(mockAlertLogRepo)
		repo.On("HasRecentAlert", ctx, "job_failure",
			map[string]string{"data.job_name": "test-job"},
			mock.AnythingOfType("time.Time"),
		).Return(true, nil)
		repo.On("InsertWithStatus", ctx, mock.Anything, alertmanager.StatusDeduplicated).Return(testUUID, nil)
		defer repo.AssertExpectations(t)

		cfg := map[string]notify.AlertDeduplicationConfig{
			"job_failure": {
				DedupKeys:             []string{"data.job_name"},
				WindowMinutes:         60,
				ActiveWindowStartHour: 0,
				ActiveWindowEndHour:   24,
			},
		}
		provider := notify.NewAlertLogProvider(repo, cfg, logger)

		id, isDeduplicated, err := provider.Insert(ctx, basePayload())

		assert.Nil(t, err)
		assert.Equal(t, testUUID, id)
		assert.True(t, isDeduplicated)
	})

	t.Run("should fall back to inserting PENDING when HasRecentAlert returns an error", func(t *testing.T) {
		repo := new(mockAlertLogRepo)
		repo.On("HasRecentAlert", ctx, "job_failure",
			map[string]string{"data.job_name": "test-job"},
			mock.AnythingOfType("time.Time"),
		).Return(false, errors.New("db error"))
		repo.On("InsertWithStatus", ctx, mock.Anything, alertmanager.StatusPending).Return(testUUID, nil)
		defer repo.AssertExpectations(t)

		cfg := map[string]notify.AlertDeduplicationConfig{
			"job_failure": {
				DedupKeys:             []string{"data.job_name"},
				WindowMinutes:         60,
				ActiveWindowStartHour: 0,
				ActiveWindowEndHour:   24,
			},
		}
		provider := notify.NewAlertLogProvider(repo, cfg, logger)

		id, isDeduplicated, err := provider.Insert(ctx, basePayload())

		// Error from HasRecentAlert is swallowed; the insert itself should succeed.
		assert.Nil(t, err)
		assert.Equal(t, testUUID, id)
		assert.False(t, isDeduplicated)
	})

	t.Run("should insert PENDING and skip HasRecentAlert when dedupKeys are empty", func(t *testing.T) {
		repo := new(mockAlertLogRepo)
		// HasRecentAlert must NOT be called when dedupKeys is empty.
		repo.On("InsertWithStatus", ctx, mock.Anything, alertmanager.StatusPending).Return(testUUID, nil)
		defer repo.AssertExpectations(t)

		cfg := map[string]notify.AlertDeduplicationConfig{
			"job_failure": {
				DedupKeys:             []string{},
				WindowMinutes:         60,
				ActiveWindowStartHour: 0,
				ActiveWindowEndHour:   24,
			},
		}
		provider := notify.NewAlertLogProvider(repo, cfg, logger)

		id, isDeduplicated, err := provider.Insert(ctx, basePayload())

		assert.Nil(t, err)
		assert.Equal(t, testUUID, id)
		assert.False(t, isDeduplicated)
	})

	t.Run("getDedupValues: keys with unknown prefix are skipped, resulting in no dedup", func(t *testing.T) {
		// "unknown.field" has no handler in getDedupValues, so dedupValues will be
		// empty and shouldDeduplicate will return false — HasRecentAlert is NOT called.
		repo := new(mockAlertLogRepo)
		repo.On("InsertWithStatus", ctx, mock.Anything, alertmanager.StatusPending).Return(testUUID, nil)
		defer repo.AssertExpectations(t)

		cfg := map[string]notify.AlertDeduplicationConfig{
			"job_failure": {
				DedupKeys:             []string{"unknown.field"},
				WindowMinutes:         60,
				ActiveWindowStartHour: 0,
				ActiveWindowEndHour:   24,
			},
		}
		provider := notify.NewAlertLogProvider(repo, cfg, logger)

		id, isDeduplicated, err := provider.Insert(ctx, basePayload())

		assert.Nil(t, err)
		assert.Equal(t, testUUID, id)
		assert.False(t, isDeduplicated)
	})

	t.Run("getDedupValues: missing data/labels key values are skipped, resulting in no dedup", func(t *testing.T) {
		// "data.missing_key" does not exist in the payload Data map, so it is
		// skipped, dedupValues is empty, and HasRecentAlert is NOT called.
		repo := new(mockAlertLogRepo)
		repo.On("InsertWithStatus", ctx, mock.Anything, alertmanager.StatusPending).Return(testUUID, nil)
		defer repo.AssertExpectations(t)

		cfg := map[string]notify.AlertDeduplicationConfig{
			"job_failure": {
				DedupKeys:             []string{"data.missing_key"},
				WindowMinutes:         60,
				ActiveWindowStartHour: 0,
				ActiveWindowEndHour:   24,
			},
		}
		provider := notify.NewAlertLogProvider(repo, cfg, logger)

		id, isDeduplicated, err := provider.Insert(ctx, basePayload())

		assert.Nil(t, err)
		assert.Equal(t, testUUID, id)
		assert.False(t, isDeduplicated)
	})

	t.Run("getDedupValues: happy path extracts both data and label values", func(t *testing.T) {
		// Both "data.job_name" and "labels.team" should be extracted and passed to
		// HasRecentAlert as the dedup values map.
		expectedDedupValues := map[string]string{
			"data.job_name": "test-job",
			"labels.team":   "data-eng",
		}
		repo := new(mockAlertLogRepo)
		repo.On("HasRecentAlert", ctx, "job_failure",
			expectedDedupValues,
			mock.AnythingOfType("time.Time"),
		).Return(true, nil)
		repo.On("InsertWithStatus", ctx, mock.Anything, alertmanager.StatusDeduplicated).Return(testUUID, nil)
		defer repo.AssertExpectations(t)

		cfg := map[string]notify.AlertDeduplicationConfig{
			"job_failure": {
				DedupKeys:             []string{"data.job_name", "labels.team"},
				WindowMinutes:         60,
				ActiveWindowStartHour: 0,
				ActiveWindowEndHour:   24,
			},
		}
		provider := notify.NewAlertLogProvider(repo, cfg, logger)

		id, isDeduplicated, err := provider.Insert(ctx, basePayload())

		assert.Nil(t, err)
		assert.Equal(t, testUUID, id)
		assert.True(t, isDeduplicated)
	})
}
