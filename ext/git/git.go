package git

import "context"

type Repository interface {
	CompareDiff(ctx context.Context, projectID any, fromRef, toRef string) ([]*Diff, error)
}

type RepositoryFile interface {
	GetRaw(ctx context.Context, projectID any, ref, fileName string) ([]byte, error)
}
