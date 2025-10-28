package dex

import (
	"time"

	"github.com/goto/optimus/core/scheduler"
)

const dexStatusRunning = "RUNNING"
const dexStatusStopped = "STOPPED"

type dexTableStatsAPIResponse struct {
	Stats Stats `json:"stats"`
}

// Stats encapsulates the producer stats and completion details.
type Stats struct {
	ProducerType  string         `json:"producer_type"`
	Producer      Producer       `json:"producer"`
	IsComplete    bool           `json:"is_complete"`
	DateBreakdown []DateSnapshot `json:"date_breakdown"`
}

// Producer holds metadata about the producer’s runtime status.
type Producer struct {
	Status    string    `json:"status"`
	StoppedAt time.Time `json:"stopped_at"`
}

// DateSnapshot provides a daily breakdown of data completeness.
type DateSnapshot struct {
	Date       time.Time `json:"date"`
	IsComplete bool      `json:"is_complete"`
}

func (s Stats) IsManagedUntil(managedUntil time.Time) bool {
	switch s.Producer.Status {
	case dexStatusRunning:
		return true
	case dexStatusStopped:
		if managedUntil.Before(s.Producer.StoppedAt) {
			return true
		}
	}
	return false
}

func (s Stats) toSchedulerDataCompletenessStatus() *scheduler.DataCompletenessStatus {
	isComplete := true
	dataCompletenessByDate := make([]*scheduler.DataCompletenessByDate, len(s.DateBreakdown))
	for i, v := range s.DateBreakdown {
		dataCompletenessByDate[i] = &scheduler.DataCompletenessByDate{
			Date:       v.Date,
			IsComplete: v.IsComplete,
		}
		if v.IsComplete == false {
			isComplete = false
		}
	}
	return &scheduler.DataCompletenessStatus{
		IsComplete:             isComplete,
		DataCompletenessByDate: dataCompletenessByDate,
	}
}
