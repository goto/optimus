package model

type Diff struct {
	OldPath string
	NewPath string
}

type Diffs []*Diff

func (diffs Diffs) GetAllDirectories(appendDirectoryWithCriteria func(directory string, directoryExists map[string]bool, fileDirectories []string) []string) []string {
	directories := make([]string, 0)
	directoryExists := make(map[string]bool)

	for i := range diffs {
		directories = appendDirectoryWithCriteria(diffs[i].OldPath, directoryExists, directories)
		directories = appendDirectoryWithCriteria(diffs[i].NewPath, directoryExists, directories)
	}

	return directories
}

type Tree struct {
	Name string
	Type string
	Path string
}
