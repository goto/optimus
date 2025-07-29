package model

type Diff struct {
	OldPath string
	NewPath string
}

type Diffs []*Diff

type Commit struct {
	SHA     string
	Message string
	URL     string
}
