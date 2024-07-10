package resource

import (
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
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
			return nil, errors.NotFound(resource.EntityResourceChangelog, "changelog not found")
		}
		return nil, errors.Wrap(resource.EntityResourceChangelog, "error in reading row for changelog", err)
	}
	return &cl, nil
}

func fromStorageChangelog(changeLog *ChangeLog) *resource.ChangeLog {
	resourceChangeLog := resource.ChangeLog{
		Type: changeLog.Type,
		Time: changeLog.Time,
	}

	resourceChangeLog.Change = make([]resource.Change, len(changeLog.Change))
	for i, change := range changeLog.Change {
		resourceChangeLog.Change[i].Property = change.Property
		resourceChangeLog.Change[i].Diff = change.Diff
	}
	return &resourceChangeLog
}
