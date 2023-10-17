package parser

import (
	"regexp"
	"strings"

	"github.com/goto/optimus/ext/store/bigquery"
)

var (
	topLevelUpstreamsPattern = regexp.MustCompile(
		"(?i)(?:FROM)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-\\*?]+)`?" + //nolint:gocritic
			"|" +
			"(?i)(?:JOIN)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" +
			"|" +
			"(?i)(?:WITH)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?\\s+(?:AS)" +
			"|" +
			// ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
			"(?i)(?:MERGE)\\s*(?:INTO)?\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" + // to ignore
			"|" +
			// ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
			"(?i)(?:INSERT)\\s*(?:INTO)?\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" + // to ignore
			"|" +
			// ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_statement
			"(?i)(?:DELETE)\\s*(?:FROM)?\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" + // to ignore
			"|" +
			// ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
			"(?i)(?:CREATE)\\s*(?:OR\\s+REPLACE)?\\s*(?:VIEW|(?:TEMP\\s+)?TABLE)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" + // to ignore
			"|" +
			"(?i)(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`\\s*(?:AS)?")

	singleLineCommentsPattern = regexp.MustCompile(`(--.*)`)
	multiLineCommentsPattern  = regexp.MustCompile(`(((/\*)+?[\w\W]*?(\*/)+))`)
	specialCommentPattern     = regexp.MustCompile(`(\/\*\s*(@[a-zA-Z0-9_-]+)\s*\*\/)`)
)

func ParseTopLevelUpstreamsFromQuery(query string) []string {
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
		case "merge":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 13, 14, 15, 16
		case "insert":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 17, 18, 19, 20
		case "delete":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 21, 22, 23, 24
		case "create":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 25, 26, 27, 28
		default:
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 29, 30, 31, 32
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

		if clause == "create" || clause == "merge" || clause == "insert" || clause == "delete" {
			continue
		}

		resourceURN, _ := bigquery.NewResourceURN(project, dataset, name)

		if clause == "with" {
			pseudoResources[resourceURN] = true
		} else {
			resourcesFound[resourceURN] = true
		}
	}

	output := []string{}

	for resourceURN := range resourcesFound {
		if pseudoResources[resourceURN] {
			continue
		}
		output = append(output, resourceURN.URN())
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
