package processor

import (
	"regexp"
	"strings"
)

var (
	singleLineCommentsPattern = regexp.MustCompile(`(--.*)`)
	multiLineCommentsPattern  = regexp.MustCompile(`(((/\*)+?[\w\W]*?(\*/)+))`)
	specialCommentPattern     = regexp.MustCompile(`(\/\*\s*(@[a-zA-Z0-9_-]+)\s*\*\/)`) // special comment pattern eg. @ignoreupstream
)

func CleanQuery(query string) string {
	cleanedQuery := CleanQueryFromComment(query)
	matches := specialCommentPattern.FindAllString(query, -1)
	for _, match := range matches {
		cleanedQuery = strings.ReplaceAll(cleanedQuery, match, "")
	}
	return cleanedQuery
}

func CleanQueryFromComment(query string) string {
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
