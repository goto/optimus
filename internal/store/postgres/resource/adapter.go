package resource

import (
	"time"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
	"github.com/jackc/pgx/v5"
)

type Change struct {
	Property string `json:"attribute_name"`
	Diff     string `json:"diff"`
}

type ChangeLog struct {
	Change []Change
	Type   string
	Time   time.Time
}

func FromChangelogRow(row pgx.Row) (*ChangeLog, error) {
	var cl ChangeLog
	err := row.Scan(&cl.Change, &cl.Type, &cl.Time)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(job.EntityJobChangeLog, "changelog not found")
		}
		return nil, errors.Wrap(job.EntityJob, "error in reading row for changelog", err)
	}
	return &cl, nil
}

func fromStorageChangelog(changeLog *ChangeLog) *resource.ChangeLog {
	jobChangeLog := resource.ChangeLog{
		Type: changeLog.Type,
		Time: changeLog.Time,
	}

	jobChangeLog.Change = make([]resource.Change, len(changeLog.Change))
	for i, change := range changeLog.Change {
		jobChangeLog.Change[i].Property = change.Property
		jobChangeLog.Change[i].Diff = change.Diff
	}
	return &jobChangeLog
}
