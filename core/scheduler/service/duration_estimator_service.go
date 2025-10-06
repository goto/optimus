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
	durationEstimator DurationEstimatorRepo
}

func NewDurationEstimatorService(durationEstimator DurationEstimatorRepo, lastNRuns, bufferPercentage int) *DurationEstimatorService {
	return &DurationEstimatorService{
		durationEstimator: durationEstimator,
		lastNRuns:         lastNRuns,
		bufferPercentage:  bufferPercentage,
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
			bufferedDuration := time.Duration(int(duration.Milliseconds()) * (100 + s.bufferPercentage) / 100)
			jobDurations[jobName] = &bufferedDuration
		}
	}
	return jobDurations
}
