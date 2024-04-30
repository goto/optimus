package gitlab

import (
	"context"

	"github.com/goto/optimus/ext/git"
	"github.com/xanzy/go-gitlab"
)

type Gitlab struct {
	client *gitlab.Client
}

func (g *Gitlab) CompareDiff(ctx context.Context, projectId any, fromRef, toRef string) ([]*git.Diff, error) {
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

	compareResp, _, err = g.client.Repositories.Compare(projectId, compareOption, gitlab.WithContext(ctx))
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

func (g *Gitlab) ListTree(ctx context.Context, projectId any, ref string, path string) ([]*git.Tree, error) {
	var (
		listTreeOption = &gitlab.ListTreeOptions{
			ListOptions: gitlab.ListOptions{Page: 1, PerPage: 100, OrderBy: "id", Pagination: "keyset", Sort: "asc"},
			Path:        gitlab.Ptr(path),
			Ref:         gitlab.Ptr(ref),
			Recursive:   gitlab.Ptr(true),
		}
		resp         = make([]*git.Tree, 0)
		listTreeResp []*gitlab.TreeNode
		err          error
	)

	for {
		listTreeResp, _, err = g.client.Repositories.ListTree(projectId, listTreeOption, gitlab.WithContext(ctx))
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

func NewGitlab(baseUrl, token string) (*Gitlab, error) {
	client, err := gitlab.NewClient(token, gitlab.WithBaseURL(baseUrl))
	if err != nil {
		return nil, err
	}

	return &Gitlab{client: client}, nil
}
