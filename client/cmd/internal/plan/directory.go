package plan

type DirectoryParser interface {
	ValidDirectory(directory string) bool
	ParseDirectory(directory string) string
}

func GetValidJobDirectory(directories []string) []string {
	var (
		jobDirectories []string
		jobParser      = new(JobPlan)
	)

	for i := range directories {
		if jobParser.ValidDirectory(directories[i]) {
			jobDirectories = append(jobDirectories, jobParser.ParseDirectory(directories[i]))
		}
	}

	return jobDirectories
}

func GetValidResourceDirectory(directories []string) []string {
	var (
		resourcesDirectories []string
		resourceParser       = new(ResourcePlan)
	)

	for i := range directories {
		if resourceParser.ValidDirectory(directories[i]) {
			resourcesDirectories = append(resourcesDirectories, resourceParser.ParseDirectory(directories[i]))
		}
	}

	return resourcesDirectories
}

func DistinctDirectory(directories []string) []string {
	directoryDictionary := make(map[string]bool)
	res := make([]string, 0)
	for i := range directories {
		if directoryDictionary[directories[i]] {
			continue
		}
		directoryDictionary[directories[i]] = true
		res = append(res, directories[i])
	}
	return res
}
