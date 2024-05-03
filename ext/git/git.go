package git

import "context"

type Repository interface {
	CompareDiff(ctx context.Context, projectID any, fromRef, toRef string) ([]*Diff, error)
	ListTree(ctx context.Context, projectID any, ref, path string) ([]*Tree, error)
}

type RepositoryFiles interface {
	GetRaw(ctx context.Context, projectID any, ref, fileName string) ([]byte, error)
}
