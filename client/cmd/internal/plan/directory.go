package plan

import (
	"strings"
)

type DirectoryParser interface {
	ValidDirectory(directory string) bool
	ParseDirectory(directory string) string
}

func GetValidJobDirectory(directories []string) []string {
	var (
		jobDirectories []string
		jobParser      = new(JobPlan)
	)

	for _, directory := range directories {
		if jobParser.ValidDirectory(directory) {
			jobDirectory := jobParser.ParseDirectory(directory)
			if jobDirectory == "" {
				continue
			}
			jobDirectories = append(jobDirectories, jobDirectory)
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

func FilterArchiveDirectories(directories []string) []string {
	var filteredDirectories []string
	for _, directory := range directories {
		if strings.HasPrefix(directory, "./archive") ||
			strings.HasPrefix(directory, "/archive") ||
			strings.HasPrefix(directory, "archive") ||
			strings.HasPrefix(directory, ".archive") {
			continue
		}
		filteredDirectories = append(filteredDirectories, directory)
	}
	return filteredDirectories
}
