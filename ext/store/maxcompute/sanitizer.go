package maxcompute

import "github.com/aliyun/aliyun-odps-go-sdk/odps/common"

var (
	// reserved keywords https://www.alibabacloud.com/help/en/maxcompute/user-guide/reserved-words-and-keywords
	reservedKeywords = []string{
		"add", "after", "all", "alter", "analyze", "and", "archive", "array", "as", "asc",
		"before", "between", "bigint", "binary", "blob", "boolean", "both", "decimal",
		"bucket", "buckets", "by", "cascade", "case", "cast", "cfile", "change", "cluster",
		"clustered", "clusterstatus", "collection", "column", "columns", "comment", "compute",
		"concatenate", "continue", "create", "cross", "current", "cursor", "data", "database",
		"databases", "date", "datetime", "dbproperties", "deferred", "delete", "delimited",
		"desc", "describe", "directory", "disable", "distinct", "distribute", "double", "drop",
		"else", "enable", "end", "except", "escaped", "exclusive", "exists", "explain", "export",
		"extended", "external", "false", "fetch", "fields", "fileformat", "first", "float",
		"following", "format", "formatted", "from", "full", "function", "functions", "grant",
		"group", "having", "hold_ddltime", "idxproperties", "if", "import", "in", "index",
		"indexes", "inpath", "inputdriver", "inputformat", "insert", "int", "intersect", "into",
		"is", "items", "join", "keys", "lateral", "left", "lifecycle", "like", "limit", "lines",
		"load", "local", "location", "lock", "locks", "long", "map", "mapjoin", "materialized",
		"minus", "msck", "not", "no_drop", "null", "of", "offline", "offset", "on", "option",
		"or", "order", "out", "outer", "outputdriver", "outputformat", "over", "overwrite",
		"partition", "partitioned", "partitionproperties", "partitions", "percent", "plus",
		"preceding", "preserve", "procedure", "purge", "range", "rcfile", "read", "readonly",
		"reads", "rebuild", "recordreader", "recordwriter", "reduce", "regexp", "rename",
		"repair", "replace", "restrict", "revoke", "right", "rlike", "row", "rows", "schema",
		"schemas", "select", "semi", "sequencefile", "serde", "serdeproperties", "set", "shared",
		"show", "show_database", "smallint", "sort", "sorted", "ssl", "statistics", "status",
		"stored", "streamtable", "string", "struct", "table", "tables", "tablesample",
		"tblproperties", "temporary", "terminated", "textfile", "then", "timestamp", "tinyint",
		"to", "touch", "transform", "trigger", "true", "type", "unarchive", "unbounded", "undo",
		"union", "uniontype", "uniquejoin", "unlock", "unsigned", "update", "use", "using",
		"utc", "utc_timestamp", "view", "when", "where", "while", "div",
	}

	reservedKeywordsMap map[string]struct{}
)

func init() {
	reservedKeywordsMap = make(map[string]struct{})
	for _, keyword := range reservedKeywords {
		reservedKeywordsMap[keyword] = struct{}{}
	}
}

func SafeKeyword(keyword string) string {
	if _, exists := reservedKeywordsMap[keyword]; exists {
		return common.QuoteRef(keyword)
	}

	return keyword
}
