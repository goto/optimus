package plan

import (
	"fmt"
	"strings"

	"github.com/goto/optimus/client/local/model"
)

const ParentFileName = "this.yaml"

type SpecReader interface {
	ReadAll(directory string) ([]*model.JobSpec, error)
}

type JobGetter struct {
	specReader SpecReader
}

func (j *JobGetter) GetPathsByParentPath(parentPath string) ([]string, error) {
	if !strings.HasSuffix(parentPath, ParentFileName) {
		return nil, fmt.Errorf("parent path must end with '%s'", ParentFileName)
	}

	parentDirPath := strings.TrimSuffix(parentPath, ParentFileName)
	jobSpecs, err := j.specReader.ReadAll(parentDirPath)
	if err != nil {
		return nil, fmt.Errorf("error reading job specs from parent path '%s': %w", parentDirPath, err)
	}

	jobPaths := make([]string, 0, len(jobSpecs))
	for _, jobSpec := range jobSpecs {
		jobPaths = append(jobPaths, jobSpec.Path)
	}

	return jobPaths, nil
}

func NewJobGetter(specReader SpecReader) *JobGetter {
	return &JobGetter{
		specReader: specReader,
	}
}
