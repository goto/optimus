package gitlab

import (
	"context"
	"net/http"
	"os"

	"github.com/xanzy/go-gitlab"

	"github.com/goto/optimus/ext/git"
)

const (
	defaultPerPage = 100
)

type API struct {
	client *gitlab.Client
}

func (g *API) CompareDiff(ctx context.Context, projectID any, fromRef, toRef string) ([]*git.Diff, error) {
	var (
		compareOption = &gitlab.CompareOptions{
			From:     gitlab.Ptr(fromRef),
			To:       gitlab.Ptr(toRef),
			Straight: gitlab.Ptr(true),
			Unidiff:  gitlab.Ptr(true),
		}
		compareResp *gitlab.Compare
		err         error
	)

	compareResp, _, err = g.client.Repositories.Compare(projectID, compareOption, gitlab.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	resp := make([]*git.Diff, 0, len(compareResp.Diffs))
	for _, diff := range compareResp.Diffs {
		if diff == nil {
			continue
		}
		resp = append(resp, &git.Diff{
			OldPath:     diff.OldPath,
			NewPath:     diff.NewPath,
			NewFile:     diff.NewFile,
			RenamedFile: diff.RenamedFile,
			DeleteFile:  diff.DeletedFile,
		})
	}

	return resp, nil
}

func (g *API) ListTree(ctx context.Context, projectID any, ref, path string) ([]*git.Tree, error) {
	var (
		listTreeOption = &gitlab.ListTreeOptions{
			ListOptions: gitlab.ListOptions{Page: 1, PerPage: defaultPerPage, OrderBy: "id", Pagination: "keyset", Sort: "asc"},
			Path:        gitlab.Ptr(path),
			Ref:         gitlab.Ptr(ref),
			Recursive:   gitlab.Ptr(true),
		}
		resp         = make([]*git.Tree, 0)
		listTreeResp []*gitlab.TreeNode
		err          error
	)

	for {
		listTreeResp, _, err = g.client.Repositories.ListTree(projectID, listTreeOption, gitlab.WithContext(ctx))
		if err != nil {
			return nil, err
		}

		for _, tree := range listTreeResp {
			if tree == nil {
				continue
			}
			resp = append(resp, &git.Tree{
				Name: tree.Name,
				Type: tree.Type,
				Path: tree.Path,
			})
		}

		if len(listTreeResp) < listTreeOption.PerPage {
			break
		}

		// next page
		listTreeOption.Page++
	}

	return resp, nil
}

func (g *API) GetRaw(ctx context.Context, projectID any, ref, fileName string) ([]byte, error) {
	var (
		option *gitlab.GetRawFileOptions
		resp   *gitlab.Response
		buff   []byte
		err    error
	)

	if ref != "" {
		option = &gitlab.GetRawFileOptions{Ref: gitlab.Ptr(ref)}
	}

	buff, resp, err = g.client.RepositoryFiles.GetRawFile(projectID, fileName, option, gitlab.WithContext(ctx))
	if err != nil {
		if resp.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}
	return buff, nil
}

func NewGitlab(baseURL, token string) (*API, error) {
	var (
		opts []gitlab.ClientOptionFunc
		api  = &API{}
		err  error
	)

	if baseURL != "" {
		opts = append(opts, gitlab.WithBaseURL(baseURL))
	}

	if os.Getenv("GIT_PERSONAL") == "true" {
		api.client, err = gitlab.NewClient(token, opts...)
	} else {
		api.client, err = gitlab.NewJobClient(token, opts...)
	}
	return api, err
}
