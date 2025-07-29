package model

import (
	"context"
)

const (
	ProviderGitHub = "github"
	ProviderGitLab = "gitlab"
)

// Parser is contract that will be defined by each provider
// to parse remote metadata from path
type Parser func(remotePath string) (*Metadata, error)

// Client is a contract that will be defined by each provider
// to execute client-related operation
type Client interface {
	// DownloadRelease downloads a release specified by the parameter.
	// This string parameter is not necessarily the URL path.
	// Each provider can defines what this parameter is.
	DownloadRelease(context.Context, string) (*RepositoryRelease, error)
	// DownloadAsset downloads asset based on the parameter.
	// This string parameter is not necessarily the URL path.
	// Each provider can defines what this parameter is.
	DownloadAsset(context.Context, string) ([]byte, error)
}

// RepositoryAPI is contract defined by each provider to control repository via provider API
type RepositoryAPI interface {
	// CompareDiff get diff between two references.
	CompareDiff(ctx context.Context, projectID any, target, source string) ([]*Diff, error)
	// GetFileContent fetch file content from specific file path
	GetFileContent(ctx context.Context, projectID any, ref, filePath string) ([]byte, error)
}

type CommitAPI interface {
	GetLatestCommitByPath(ctx context.Context, projectID any, path string) (*Commit, error)
	GetCommitDiff(ctx context.Context, projectID any, sha string) ([]*Diff, error)
}
