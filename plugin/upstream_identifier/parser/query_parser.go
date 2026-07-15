package parser

import (
	"regexp"
	"sort"
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
			"(?i)(?:SET)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-\\*?]+`?)`?" + // to ignore
			"|" +
			"(?i)(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?(`?[\\w-]+`?\\.`?[\\w-]+`?\\.`?[\\w-]+`?)`?\\s*(?:AS)?")

	singleLineCommentsPattern = regexp.MustCompile(`(--.*)`)
	multiLineCommentsPattern  = regexp.MustCompile(`(((/\*)+?[\w\W]*?(\*/)+))`)
	specialCommentPattern     = regexp.MustCompile(`(\/\*\s*(@[a-zA-Z0-9_-]+)\s*\*\/)`)

	// fromListStateKeywordPattern recognizes the keywords that toggle whether a given position in
	// the query is inside an active FROM/JOIN table list. It is scanned independently of
	// topLevelUpstreamsPattern (see newFromListChecker) precisely because a bare dotted identifier
	// with no clause keyword directly in front of it (topLevelUpstreamsPattern's keyword-less final
	// alternative, dispatched to the "default" case below) is structurally ambiguous on its own:
	// it might be the next item in a comma-joined FROM list, or it might be a struct/record field
	// access, a SELECT-list expression, or part of a WHERE/ON condition. This independent scan is
	// what disambiguates the two by tracking which clause is actually in effect at that position.
	fromListStateKeywordPattern = regexp.MustCompile(`(?i)\b(FROM|JOIN|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|ON|WINDOW|QUALIFY|UNION|LIMIT)\b`)
)

func ParseTopLevelUpstreamsFromQuery(query string) []string {
	cleanedQuery := cleanQueryFromComment(query)

	tableFound := map[string]bool{}
	pseudoTable := map[string]bool{}

	matches := topLevelUpstreamsPattern.FindAllStringSubmatchIndex(cleanedQuery, -1)
	inFromList := newFromListChecker(cleanedQuery, matches)

	for _, idx := range matches {
		var tableIdx, ignoreUpstreamIdx int
		tokens := strings.Fields(groupText(cleanedQuery, idx, 0))
		clause := strings.ToLower(tokens[0])

		isKeywordMatch := true
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
		case "set":
			ignoreUpstreamIdx, tableIdx = 15, 16
		default:
			ignoreUpstreamIdx, tableIdx = 17, 18
			isKeywordMatch = false
		}

		if strings.TrimSpace(groupText(cleanedQuery, idx, ignoreUpstreamIdx)) == "@ignoreupstream" {
			continue
		}

		if clause == "create" || clause == "merge" || clause == "insert" || clause == "delete" || clause == "set" {
			continue
		}

		// A keyword-less match is only a genuine table reference if it's actually reachable from
		// a FROM/JOIN clause (e.g. SELECT * FROM a.b.c, a.b.f which makes a.b.c & a.b.f actual upstreams) -
		// otherwise it's a false positive, such as struct/record field access, a SELECT-list
		// expression, or a WHERE/ON condition, and must not be treated as an upstream.
		if !isKeywordMatch && !inFromList(idx[0]) {
			continue
		}

		tableName := cleanTableFromTickQuote(groupText(cleanedQuery, idx, tableIdx))
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

// groupText returns the text captured by submatch group groupIndex within idx (as produced by
// FindAllStringSubmatchIndex), or "" if that group did not participate in the match.
func groupText(query string, idx []int, groupIndex int) string {
	start, end := idx[2*groupIndex], idx[2*groupIndex+1]
	if start < 0 {
		return ""
	}
	return query[start:end]
}

// newFromListChecker returns a function reporting, for a given byte offset into query, whether
// that position is reachable from a preceding FROM/JOIN keyword without having crossed into a
// different clause (WHERE, GROUP BY, ORDER BY, HAVING, ON, WINDOW, QUALIFY, UNION, LIMIT) or out
// of the parenthesis depth it was set at (so a subquery's own FROM doesn't leak its "in a table
// list" state into the clause that encloses it, e.g. `WHERE x IN (SELECT y FROM a.b.c) AND d.e.f`
// must not treat d.e.f as a table just because the subquery had an active FROM).
// upstreamMatches masks out MERGE/INSERT/DELETE/CREATE/SET spans (already found by the caller via
// topLevelUpstreamsPattern) so that e.g. the literal "FROM" inside "DELETE FROM x.y.z" is never
// treated as a real FROM clause.
func newFromListChecker(query string, upstreamMatches [][]int) func(pos int) bool {
	var ignoredSpans [][2]int
	for _, idx := range upstreamMatches {
		clause := strings.ToLower(strings.Fields(groupText(query, idx, 0))[0])
		switch clause {
		case "merge", "insert", "delete", "create", "set":
			ignoredSpans = append(ignoredSpans, [2]int{idx[0], idx[1]})
		}
	}

	type event struct {
		pos     int
		turnsOn bool
		paren   byte // '(', ')', or 0 for a keyword event
	}

	var events []event
	for _, kwIdx := range fromListStateKeywordPattern.FindAllStringIndex(query, -1) {
		if withinAnySpan(kwIdx[0], ignoredSpans) {
			continue
		}
		kw := strings.ToLower(strings.Join(strings.Fields(query[kwIdx[0]:kwIdx[1]]), " "))
		events = append(events, event{pos: kwIdx[0], turnsOn: kw == "from" || kw == "join"})
	}
	for i, c := range query {
		switch c {
		case '(':
			events = append(events, event{pos: i, paren: '('})
		case ')':
			events = append(events, event{pos: i, paren: ')'})
		}
	}
	sort.Slice(events, func(i, j int) bool { return events[i].pos < events[j].pos })

	positions := make([]int, len(events))
	states := make([]bool, len(events))

	state := false
	var stack []bool
	for i, e := range events {
		switch e.paren {
		case '(':
			stack = append(stack, state)
			state = false
		case ')':
			if len(stack) > 0 {
				state = stack[len(stack)-1]
				stack = stack[:len(stack)-1]
			} else {
				state = false
			}
		default:
			state = e.turnsOn
		}
		positions[i] = e.pos
		states[i] = state
	}

	return func(pos int) bool {
		// binary search for the last event strictly before pos
		i := sort.SearchInts(positions, pos)
		if i == 0 {
			return false
		}
		return states[i-1]
	}
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
