package parser

import (
	"regexp"
	"strings"
)

var (
	topLevelUpstreamsPattern = regexp.MustCompile(
		"(?i)(?:FROM)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-\\*?]+`?)`?" + //nolint:gocritic
			"|" +
			"(?i)(?:JOIN)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-]+`?)`?" +
			"|" +
			"(?i)(?:WITH)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-]+`?)`?\\s+(?:AS)" +
			"|" +
			// ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
			"(?i)(?:MERGE)\\s*(?:INTO)?\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-]+`?)`?" + // to ignore
			"|" +
			// ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
			"(?i)(?:INSERT)\\s*(?:INTO)?\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-]+`?)`?" + // to ignore
			"|" +
			// ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_statement
			"(?i)(?:DELETE)\\s*(?:FROM)?\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-]+`?)`?" + // to ignore
			"|" +
			// ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
			"(?i)(?:CREATE)\\s*(?:OR\\s+REPLACE)?\\s*(?:VIEW|(?:TEMP\\s+)?TABLE)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-]+`?)`?" + // to ignore
			"|" +
			"(?i)(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-]+`?)`?\\s*(?:AS)?")

	singleLineCommentsPattern = regexp.MustCompile(`(--.*)`)
	multiLineCommentsPattern  = regexp.MustCompile(`(((/\*)+?[\w\W]*?(\*/)+))`)
	specialCommentPattern     = regexp.MustCompile(`(\/\*\s*(@[a-zA-Z0-9_-]+)\s*\*\/)`)
)

func ParseTopLevelUpstreamsFromQuery(query string) []string {
	cleanedQuery := cleanQueryFromComment(query)

	tableFound := map[string]bool{}
	pseudoTable := map[string]bool{}

	matches := topLevelUpstreamsPattern.FindAllStringSubmatch(cleanedQuery, -1)

	for _, match := range matches {
		var tableIdx, ignoreUpstreamIdx int
		tokens := strings.Fields(match[0])
		clause := strings.ToLower(tokens[0])

		switch clause {
		case "from":
			ignoreUpstreamIdx, tableIdx = 1, 2
		case "join":
			ignoreUpstreamIdx, tableIdx = 3, 4
		case "with":
			ignoreUpstreamIdx, tableIdx = 5, 6
		case "merge":
			ignoreUpstreamIdx, tableIdx = 7, 8
		case "insert":
			ignoreUpstreamIdx, tableIdx = 9, 10
		case "delete":
			ignoreUpstreamIdx, tableIdx = 11, 12
		case "create":
			ignoreUpstreamIdx, tableIdx = 13, 14
		default:
			ignoreUpstreamIdx, tableIdx = 15, 16
		}

		if strings.TrimSpace(match[ignoreUpstreamIdx]) == "@ignoreupstream" {
			continue
		}

		if clause == "create" || clause == "merge" || clause == "insert" || clause == "delete" {
			continue
		}

		tableName := cleanTableFromTickQuote(match[tableIdx])
		if clause == "with" {
			pseudoTable[tableName] = true
		} else {
			tableFound[tableName] = true
		}
	}

	output := []string{}

	for table := range tableFound {
		if pseudoTable[table] {
			continue
		}
		output = append(output, table)
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

func cleanTableFromTickQuote(tableName string) string {
	return strings.ReplaceAll(tableName, "`", "")
}
