package plan

type DirectoryParser interface {
	ValidDirectory(directory string) bool
	ParseDirectory(directory string) string
}

type JobPathGetter interface {
	GetPathsByParentPath(parentDirPath string) ([]string, error)
}

func GetValidJobDirectory(reader JobPathGetter, directories []string) []string {
	var (
		jobDirectories []string
		jobParser      = new(JobPlan)
	)

	for _, directory := range directories {
		jobPaths, err := reader.GetPathsByParentPath(directory)
		if err != nil || len(jobPaths) == 0 {
			jobPaths = []string{directory}
		}

		for _, jobPath := range jobPaths {
			if jobParser.ValidDirectory(jobPath) {
				jobDirectory := jobParser.ParseDirectory(jobPath)
				if jobDirectory == "" {
					continue
				}
				jobDirectories = append(jobDirectories, jobDirectory)
			}
		}
	}

	return jobDirectories
}

func GetValidResourceDirectory(directories []string) []string {
	var (
		resourcesDirectories []string
		resourceParser       = new(ResourcePlan)
	)

	for _, directory := range directories {
		if resourceParser.ValidDirectory(directory) {
			resourcesDirectories = append(resourcesDirectories, resourceParser.ParseDirectory(directory))
		}
	}

	return resourcesDirectories
}

func DistinctDirectory(directories []string) []string {
	directoryDictionary := make(map[string]bool)
	res := make([]string, 0)
	for _, directory := range directories {
		if _, exists := directoryDictionary[directory]; exists {
			continue
		}
		directoryDictionary[directory] = true
		res = append(res, directory)
	}
	return res
}
