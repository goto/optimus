package utils

import "strings"

// EscapeLikePattern escapes SQL LIKE wildcard characters (% and _) and the
// escape character (\), so that the given value matches literally when used
// inside a LIKE clause. Use it for user supplied prefixes, e.g.:
//
//	query += " AND full_name LIKE $1"
//	args = append(args, utils.EscapeLikePattern(prefix)+"%")
func EscapeLikePattern(pattern string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(pattern)
}
