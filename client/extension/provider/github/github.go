package github

import (
	"context"

	"github.com/google/go-github/v59/github"
)

//go:generate mockery --name Repository --outpkg=mock_github --output=../../mock/provider/github
type Repository interface {
	CompareCommits(ctx context.Context, owner, repo, base, head string, opts *github.ListOptions) (*github.CommitsComparison, *github.Response, error)
	GetContents(ctx context.Context, owner, repo, path string, opts *github.RepositoryContentGetOptions) (fileContent *github.RepositoryContent, directoryContent []*github.RepositoryContent, resp *github.Response, err error)
	ListCommits(ctx context.Context, owner, repo string, opts *github.CommitsListOptions) ([]*github.RepositoryCommit, *github.Response, error)
}
