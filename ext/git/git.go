package git

import "context"

type Diff struct {
	OldPath, NewPath                 string
	NewFile, RenamedFile, DeleteFile bool
}

type Tree struct {
	Name string
	Type string
	Path string
}

type Repository interface {
	CompareDiff(ctx context.Context, projectId any, fromRef, toRef string) ([]*Diff, error)
	ListTree(ctx context.Context, projectId any, ref string, path string) ([]*Tree, error)
}
