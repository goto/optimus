package model

type Diff struct {
	OldPath string
	NewPath string
}

type Diffs []*Diff
