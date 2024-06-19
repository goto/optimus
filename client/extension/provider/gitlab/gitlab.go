package gitlab

import "github.com/xanzy/go-gitlab"

//go:generate mockery --name Repository --outpkg=mock_gitlab --output=../../mock/provider/gitlab
type Repository interface {
	Compare(pid interface{}, opt *gitlab.CompareOptions, options ...gitlab.RequestOptionFunc) (*gitlab.Compare, *gitlab.Response, error)
}

//go:generate mockery --name RepositoryFile --outpkg=mock_gitlab --output=../../mock/provider/gitlab
type RepositoryFile interface {
	GetRawFile(pid interface{}, fileName string, opt *gitlab.GetRawFileOptions, options ...gitlab.RequestOptionFunc) ([]byte, *gitlab.Response, error)
}
