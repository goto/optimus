package parser

import (
	"regexp"
	"sort"
	"strings"
)

type fromListEventKind int

const (
	eventKindKeyword    fromListEventKind = iota
	eventKindOpenParen                    // entering a subquery
	eventKindCloseParen                   // leaving a subquery
)

type fromListEvent struct {
	pos     int
	kind    fromListEventKind
	turnsOn bool // only meaningful for eventKindKeyword: true for FROM/JOIN, false otherwise
}

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
			"(?i)(?:SET)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-\\*?]+`?)`?" + // to ignore
			"|" +
			"(?i)(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-]+`?)`?\\s*(?:AS)?")

	singleLineCommentsPattern = regexp.MustCompile(`(--.*)`)
	multiLineCommentsPattern  = regexp.MustCompile(`(((/\*)+?[\w\W]*?(\*/)+))`)
	specialCommentPattern     = regexp.MustCompile(`(\/\*\s*(@[a-zA-Z0-9_-]+)\s*\*\/)`)

	fromListStateKeywordPattern = regexp.MustCompile(`(?i)\b(FROM|JOIN|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|ON|WINDOW|QUALIFY|UNION|LIMIT)\b`)
)

func ParseTopLevelUpstreamsFromQuery(query string) []string {
	cleanedQuery := cleanQueryFromComment(query)

	return extractUpstreamTables(cleanedQuery)
}

func extractUpstreamTables(query string) []string {
	referencedTables := map[string]bool{}
	cteAliases := map[string]bool{}

	matchIndexes := topLevelUpstreamsPattern.FindAllStringSubmatchIndex(query, -1)
	insideFromOrJoinClause := buildFromListChecker(query, matchIndexes)

	for _, idx := range matchIndexes {
		tableName, isCTEAlias := extractValidTable(query, idx, insideFromOrJoinClause)
		if tableName == "" {
			continue
		}

		if isCTEAlias {
			cteAliases[tableName] = true
		} else {
			referencedTables[tableName] = true
		}
	}

	return filterAliases(referencedTables, cteAliases)
}

func clauseGroupIndices(clause string) (ignoreUpstreamIdx, tableIdx int, isKeyword bool) {
	switch clause {
	case "from":
		return 1, 2, true
	case "join":
		return 3, 4, true
	case "with":
		return 5, 6, true
	case "merge":
		return 7, 8, true
	case "insert":
		return 9, 10, true
	case "delete":
		return 11, 12, true
	case "create":
		return 13, 14, true
	case "set":
		return 15, 16, true
	default:
		return 17, 18, false
	}
}

func isWriteOnlyClause(clause string) bool {
	switch clause {
	case "create", "merge", "insert", "delete", "set":
		return true
	}
	return false
}

func filterAliases(referencedTables, cteAliases map[string]bool) []string {
	output := make([]string, 0, len(referencedTables))
	for table := range referencedTables {
		if !cteAliases[table] {
			output = append(output, table)
		}
	}
	return output
}

// groupText returns the text captured by submatch group groupIndex within idx (as produced by
// FindAllStringSubmatchIndex), or "" if that group did not participate in the match.
func groupText(query string, idx []int, groupIndex int) string {
	start, end := idx[2*groupIndex], idx[2*groupIndex+1]
	if start < 0 {
		return ""
	}
	return query[start:end]
}

// buildFromListChecker returns a closure that answers, for any position index in query, whether that
// position sits inside an active FROM/JOIN table list. This is used to differentiate dotted
// identifiers (like the comma-joined `a.b.d` in `FROM a.b.c, a.b.d`) from false positives like
// nested struct field access
func buildFromListChecker(query string, upstreamMatches [][]int) func(pos int) bool {
	writeOnlySpans := collectWriteOnlySpans(query, upstreamMatches)

	events := buildFromListEvents(query, writeOnlySpans)

	positions, states := buildFromListStateIndex(events)

	return func(pos int) bool {
		i := sort.SearchInts(positions, pos)
		if i == 0 {
			return false
		}
		return states[i-1]
	}
}

// extractValidTable validates a single regex match and returns the cleaned table name and whether
// it is a CTE alias. Returns ("", false) if the match should be skipped.
func extractValidTable(query string, idx []int, insideFromOrJoinClause func(int) bool) (string, bool) {
	clause := getClauseFromMatch(query, idx)
	ignoreUpstreamIdx, tableIdx, isKeywordMatch := clauseGroupIndices(clause)

	if strings.TrimSpace(groupText(query, idx, ignoreUpstreamIdx)) == "@ignoreupstream" {
		return "", false
	}
	if isWriteOnlyClause(clause) {
		return "", false
	}
	if !isKeywordMatch && !insideFromOrJoinClause(idx[0]) {
		return "", false
	}

	return cleanTableFromTickQuote(groupText(query, idx, tableIdx)), clause == "with"
}

func getClauseFromMatch(query string, upstreamMatchIdx []int) string {
	tokens := strings.Fields(groupText(query, upstreamMatchIdx, 0))
	return strings.ToLower(tokens[0])
}

func collectWriteOnlySpans(query string, upstreamMatches [][]int) [][2]int {
	var spans [][2]int
	for _, idx := range upstreamMatches {
		clause := getClauseFromMatch(query, idx)
		if isWriteOnlyClause(clause) {
			spans = append(spans, [2]int{idx[0], idx[1]})
		}
	}
	return spans
}

// buildFromListEvents scans query for clause keywords and parentheses, skipping any keyword
// that falls inside a write-only span, and returns all events sorted by position.
func buildFromListEvents(query string, writeOnlySpans [][2]int) []fromListEvent {
	events := buildKeywordEvents(query, writeOnlySpans)
	parenthesesEvents := buildParenthesesEvents(query)

	events = append(events, parenthesesEvents...)
	sort.Slice(events, func(i, j int) bool { return events[i].pos < events[j].pos })

	return events
}

func buildKeywordEvents(query string, writeOnlySpans [][2]int) []fromListEvent {
	var events []fromListEvent

	for _, keywordIdx := range fromListStateKeywordPattern.FindAllStringIndex(query, -1) {
		if withinAnySpan(keywordIdx[0], writeOnlySpans) {
			continue
		}
		kw := strings.ToLower(strings.Join(strings.Fields(query[keywordIdx[0]:keywordIdx[1]]), " "))
		events = append(events, fromListEvent{
			pos:     keywordIdx[0],
			kind:    eventKindKeyword,
			turnsOn: kw == "from" || kw == "join",
		})
	}

	return events
}

func buildParenthesesEvents(query string) []fromListEvent {
	var events []fromListEvent

	for i, c := range query {
		switch c {
		case '(':
			events = append(events, fromListEvent{pos: i, kind: eventKindOpenParen})
		case ')':
			events = append(events, fromListEvent{pos: i, kind: eventKindCloseParen})
		}
	}

	return events
}

// buildFromListStateIndex walks the sorted events and tracks whether a FROM/JOIN clause is
// active at each position. Parentheses create an isolated scope: opening a paren saves the
// current active state and resets it (so a subquery's FROM doesn't leak into the enclosing
// clause), and closing a paren restores the enclosing scope's state.
func buildFromListStateIndex(events []fromListEvent) (positions []int, states []bool) {
	positions = make([]int, len(events))
	states = make([]bool, len(events))

	active := false
	var scopeStack []bool

	for i, e := range events {
		switch e.kind {
		case eventKindOpenParen:
			scopeStack = append(scopeStack, active)
			active = false
		case eventKindCloseParen:
			if len(scopeStack) > 0 {
				active = scopeStack[len(scopeStack)-1]
				scopeStack = scopeStack[:len(scopeStack)-1]
			} else {
				active = false
			}
		case eventKindKeyword:
			active = e.turnsOn
		}
		positions[i] = e.pos
		states[i] = active
	}

	return
}

func withinAnySpan(pos int, spans [][2]int) bool {
	for _, s := range spans {
		if pos >= s[0] && pos < s[1] {
			return true
		}
	}
	return false
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
