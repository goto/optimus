package resource

import "time"

const (
	EntityResourceChangelog = "resource_changelog"

	ChangelogChangeTypeCreate = "create"
	ChangelogChangeTypeUpdate = "update"
	ChangelogChangeTypeDelete = "delete"
)

type Change struct {
	Property string
	Diff     string
}

type ChangeLog struct {
	Change []Change
	Type   string
	Time   time.Time
}
