package upstream

import (
	"regexp"
	"strings"
)

type QueryParser func(query string) []*Resource

var (
	topLevelUpstreamsPattern = regexp.MustCompile("" +
		"(?i)(?:FROM)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-\\*?]+)`?" +
		"|" +
		"(?i)(?:JOIN)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" +
		"|" +
		"(?i)(?:WITH)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?\\s+(?:AS)" +
		"|" +
		"(?i)(?:VIEW)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" +
		"|" +
		"(?i)(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`\\s*(?:AS)?")

	singleLineCommentsPattern = regexp.MustCompile(`(--.*)`)
	multiLineCommentsPattern  = regexp.MustCompile(`(((/\*)+?[\w\W]*?(\*/)+))`)
	specialCommentPattern     = regexp.MustCompile(`(\/\*\s*(@[a-zA-Z0-9_-]+)\s*\*\/)`)
)

func ParseTopLevelUpstreamsFromQuery(query string) []*Resource {
	cleanedQuery := cleanQueryFromComment(query)

	resourcesFound := make(map[string]bool)
	pseudoResources := make(map[string]bool)

	matches := topLevelUpstreamsPattern.FindAllStringSubmatch(cleanedQuery, -1)

	for _, match := range matches {
		var projectIdx, datasetIdx, nameIdx, ignoreUpstreamIdx int
		tokens := strings.Fields(match[0])
		clause := strings.ToLower(tokens[0])

		switch clause {
		case "from":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 1, 2, 3, 4
		case "join":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 5, 6, 7, 8
		case "with":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 9, 10, 11, 12
		case "view":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 13, 14, 15, 16
		default:
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 17, 18, 19, 20
		}

		project := match[projectIdx]
		dataset := match[datasetIdx]
		name := match[nameIdx]

		if project == "" || dataset == "" || name == "" {
			continue
		}

		if strings.TrimSpace(match[ignoreUpstreamIdx]) == "@ignoreupstream" {
			continue
		}

		if clause == "view" {
			continue
		}

		resource := Resource{
			Project: project,
			Dataset: dataset,
			Name:    name,
		}

		if clause == "with" {
			pseudoResources[resource.URN()] = true
		} else {
			resourcesFound[resource.URN()] = true
		}
	}

	var output []*Resource

	for resourceURN := range resourcesFound {
		if pseudoResources[resourceURN] {
			continue
		}
		r, _ := FromDestinationURN(resourceURN)
		output = append(output, r)
	}

	return output
}

func cleanQueryFromComment(query string) string {
	cleanedQuery := singleLineCommentsPattern.ReplaceAllString(query, "")

	matches := multiLineCommentsPattern.FindAllString(query, -1)
	for _, match := range matches {
		if specialCommentPattern.MatchString(match) {
			continue
		}
		cleanedQuery = strings.ReplaceAll(cleanedQuery, match, "")
	}

	return cleanedQuery
}

// TODO: check if ddl requires custom query, if not then we can remove this function
func ParseNestedUpsreamsFromDDL(ddl string) []*Resource {
	return ParseTopLevelUpstreamsFromQuery(ddl)
}
