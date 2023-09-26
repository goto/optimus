package parser

import (
	"regexp"
	"strings"

	"github.com/goto/optimus/ext/store/bigquery"
)

type (
	// ParserFunc parses rawResource to list of resource urn
	ParserFunc func(rawResource string) []bigquery.ResourceURN
)

var (
	topLevelUpstreamsPattern = regexp.MustCompile(
		"(?i)(?:FROM)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-\\*?]+)`?" + //nolint:gocritic
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

func ParseTopLevelUpstreamsFromQuery(query string) []bigquery.ResourceURN {
	cleanedQuery := cleanQueryFromComment(query)

	resourcesFound := make(map[bigquery.ResourceURN]bool)
	pseudoResources := make(map[bigquery.ResourceURN]bool)

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

		resourceURN, _ := bigquery.NewResourceURN(project, dataset, name)

		if clause == "with" {
			pseudoResources[resourceURN] = true
		} else {
			resourcesFound[resourceURN] = true
		}
	}

	output := []bigquery.ResourceURN{}

	for resourceURN := range resourcesFound {
		if pseudoResources[resourceURN] {
			continue
		}
		output = append(output, resourceURN)
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
