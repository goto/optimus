package filter

import "time"

type (
	FilterOpt func(*filter)
	Operand   uint64
)

const (
	bitOnProjectName         uint64 = 1 << 0
	bitOnJobName             uint64 = 1 << 1
	bitOnResourceDestination uint64 = 1 << 2
	bitOnNamespaceNames      uint64 = 1 << 3
	bitOnJobNames            uint64 = 1 << 4
	bitOnNamespaceName       uint64 = 1 << 5
	bitOnReplayStatus        uint64 = 1 << 6
	bitOnScheduledAt         uint64 = 1 << 7
	bitOnReplayID            uint64 = 1 << 8
	bitOnRunState            uint64 = 1 << 9
	bitOnStartDate           uint64 = 1 << 10
	bitOnEndDate             uint64 = 1 << 11
	bitOnTableName           uint64 = 1 << 12
	bitOnLabels              uint64 = 1 << 13
)

const (
	ProjectName         = Operand(bitOnProjectName)
	NamespaceName       = Operand(bitOnNamespaceName)
	NamespaceNames      = Operand(bitOnNamespaceNames)
	JobName             = Operand(bitOnJobName)
	JobNames            = Operand(bitOnJobNames)
	ResourceDestination = Operand(bitOnResourceDestination)
	ReplayStatus        = Operand(bitOnReplayStatus)
	ScheduledAt         = Operand(bitOnScheduledAt)
	ReplayID            = Operand(bitOnReplayID)
	RunState            = Operand(bitOnRunState)
	StartDate           = Operand(bitOnStartDate)
	EndDate             = Operand(bitOnEndDate)
	TableName           = Operand(bitOnTableName)
	Labels              = Operand(bitOnLabels)
)

func WithTime(operand Operand, value time.Time) FilterOpt {
	return func(f *filter) {
		if !(value.Equal(time.Unix(0, 0))) {
			f.bits |= uint64(operand)
			f.value[operand] = value
		}
	}
}

func WithString(operand Operand, value string) FilterOpt {
	return func(f *filter) {
		if value != "" {
			f.bits |= uint64(operand)
			f.value[operand] = value
		}
	}
}

func WithStringArray(operand Operand, value []string) FilterOpt {
	return func(f *filter) {
		if len(value) > 0 {
			f.bits |= uint64(operand)
			f.value[operand] = value
		}
	}
}

func WithMapArray(operand Operand, values []map[string]string) FilterOpt {
	return func(f *filter) {
		if len(values) > 0 {
			f.bits |= uint64(operand)
			f.value[operand] = values
		}
	}
}
