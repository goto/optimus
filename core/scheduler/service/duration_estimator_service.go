package service

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
)

type DurationEstimatorRepo interface {
	GetPercentileDurationByJobNames(ctx context.Context, jobNames []scheduler.JobName, operators map[string][]string, lastNRuns, percentile int) (map[scheduler.JobName]*time.Duration, error)
}

type DurationEstimatorService struct {
	l                 log.Logger
	lastNRuns         int
	percentile        int
	bufferPercentage  int
	minBufferDuration time.Duration
	maxBufferDuration time.Duration
	durationEstimator DurationEstimatorRepo
}

func NewDurationEstimatorService(logger log.Logger, durationEstimator DurationEstimatorRepo, lastNRuns, percentile, bufferPercentage, minBufferMinutes, maxBufferMinutes int) *DurationEstimatorService {
	return &DurationEstimatorService{
		l:                 logger,
		durationEstimator: durationEstimator,
		lastNRuns:         lastNRuns,
		percentile:        percentile,
		bufferPercentage:  bufferPercentage,
		minBufferDuration: time.Duration(minBufferMinutes) * time.Minute,
		maxBufferDuration: time.Duration(maxBufferMinutes) * time.Minute,
	}
}

func (s *DurationEstimatorService) GetPercentileDurationByJobNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error) {
	jobDurations, err := s.durationEstimator.GetPercentileDurationByJobNames(ctx, jobNames, nil, s.lastNRuns, s.percentile)
	if err != nil {
		return nil, err
	}
	for jobName, duration := range jobDurations {
		s.l.Debug(fmt.Sprintf("GetPercentileDurationByJobNames jobName: %s, duration: %v", jobName, duration))
	}
	// add buffer to the estimated duration
	return s.calculateBufferedDuration(jobDurations), nil
}

func (s *DurationEstimatorService) GetPercentileDurationByJobNamesByTask(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error) {
	jobDurations, err := s.durationEstimator.GetPercentileDurationByJobNames(ctx, jobNames, map[string][]string{"task": {}}, s.lastNRuns, s.percentile)
	if err != nil {
		return nil, err
	}
	for jobName, duration := range jobDurations {
		s.l.Debug(fmt.Sprintf("GetPercentileDurationByJobNamesByTask jobName: %s, duration: %v", jobName, duration))
	}
	// add buffer to the estimated duration
	return s.calculateBufferedDuration(jobDurations), nil
}

func (s *DurationEstimatorService) GetPercentileDurationByJobNamesByHookName(ctx context.Context, jobNames []scheduler.JobName, hookNames []string) (map[scheduler.JobName]*time.Duration, error) {
	jobDurations, err := s.durationEstimator.GetPercentileDurationByJobNames(ctx, jobNames, map[string][]string{"hook": hookNames}, s.lastNRuns, s.percentile)
	if err != nil {
		return nil, err
	}
	for jobName, duration := range jobDurations {
		s.l.Debug(fmt.Sprintf("GetPercentileDurationByJobNamesByHookName jobName: %s, duration: %v", jobName, duration))
	}
	// add buffer to the estimated duration
	return s.calculateBufferedDuration(jobDurations), nil
}

func (s *DurationEstimatorService) calculateBufferedDuration(jobDurations map[scheduler.JobName]*time.Duration) map[scheduler.JobName]*time.Duration {
	for jobName, percentileDuration := range jobDurations {
		if percentileDuration != nil && s.bufferPercentage > 0 {
			adjustedBuffer := time.Second * time.Duration(
				percentileDuration.Seconds()+
					min(
						max(
							s.minBufferDuration.Seconds(),                                  // Minimum Buffer that must be added to each job
							percentileDuration.Seconds()*(float64(s.bufferPercentage)/100), // calculating relative percentage buffer
						),
						s.maxBufferDuration.Seconds(), // Maximum Buffer allowed
					),
			)
			jobDurations[jobName] = &adjustedBuffer
		}
	}
	return jobDurations
}
