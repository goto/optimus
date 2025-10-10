package service

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
)

type DurationEstimatorRepo interface {
	GetP95DurationByJobNames(ctx context.Context, jobNames []scheduler.JobName, operators map[string][]string, lastNRuns int) (map[scheduler.JobName]*time.Duration, error)
}

type DurationEstimatorService struct {
	lastNRuns         int
	bufferPercentage  int
	minBufferDuration time.Duration
	maxBufferDuration time.Duration
	durationEstimator DurationEstimatorRepo
}

func NewDurationEstimatorService(durationEstimator DurationEstimatorRepo, lastNRuns, bufferPercentage, minBufferMinutes, maxBufferMinutes int) *DurationEstimatorService {
	return &DurationEstimatorService{
		durationEstimator: durationEstimator,
		lastNRuns:         lastNRuns,
		bufferPercentage:  bufferPercentage,
		minBufferDuration: time.Duration(minBufferMinutes) * time.Minute,
		maxBufferDuration: time.Duration(maxBufferMinutes) * time.Minute,
	}
}

func (s *DurationEstimatorService) GetP95DurationByJobNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error) {
	jobDurations, err := s.durationEstimator.GetP95DurationByJobNames(ctx, jobNames, nil, s.lastNRuns)
	if err != nil {
		return nil, err
	}
	// add buffer to the estimated duration
	return s.calculateBufferedDuration(jobDurations), nil
}

func (s *DurationEstimatorService) GetP95DurationByJobNamesByTask(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error) {
	jobDurations, err := s.durationEstimator.GetP95DurationByJobNames(ctx, jobNames, map[string][]string{"task": {}}, s.lastNRuns)
	if err != nil {
		return nil, err
	}
	// add buffer to the estimated duration
	return s.calculateBufferedDuration(jobDurations), nil
}

func (s *DurationEstimatorService) GetP95DurationByJobNamesByHookName(ctx context.Context, jobNames []scheduler.JobName, hookNames []string) (map[scheduler.JobName]*time.Duration, error) {
	jobDurations, err := s.durationEstimator.GetP95DurationByJobNames(ctx, jobNames, map[string][]string{"hook": hookNames}, s.lastNRuns)
	if err != nil {
		return nil, err
	}
	// add buffer to the estimated duration
	return s.calculateBufferedDuration(jobDurations), nil
}

func (s *DurationEstimatorService) calculateBufferedDuration(jobDurations map[scheduler.JobName]*time.Duration) map[scheduler.JobName]*time.Duration {
	for jobName, duration := range jobDurations {
		if duration != nil && s.bufferPercentage > 0 {
			bufferedDuration := time.Second * time.Duration(duration.Seconds()*(1+float64(s.bufferPercentage)/100))
			adjustedBuffer := time.Millisecond * time.Duration(min(max(s.minBufferDuration.Milliseconds(), bufferedDuration.Milliseconds()), s.maxBufferDuration.Milliseconds()))
			jobDurations[jobName] = &adjustedBuffer
		}
	}
	return jobDurations
}
