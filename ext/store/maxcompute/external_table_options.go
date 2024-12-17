package maxcompute

import "strings"

const (
	CSV         string = "CSV"
	TSV         string = "TSV"
	JSON        string = "JSON"
	TxtFile     string = "TEXTFILE"
	RcFile      string = "RCFILE"
	ORC         string = "ORC"
	OrcFile     string = "ORCFILE"
	SeqFile     string = "SEQUENCEFILE"
	Parquet     string = "PARQUET"
	Avro        string = "AVRO"
	GoogleSheet string = "GOOGLE_SHEETS"
	LarkSheets  string = "LARK_SHEETS"
)

func handlerForFormat(format string) string {
	switch strings.ToUpper(format) {
	// the built-in text extractor for CSV and TSV
	case GoogleSheet:
		return "com.aliyun.odps.CsvStorageHandler"
	case CSV:
		return "com.aliyun.odps.CsvStorageHandler"
	case TSV:
		return "com.aliyun.odps.TsvStorageHandler"

	// Extractors for inbuilt Open Source Data Formats
	case JSON:
		return "org.apache.hive.hcatalog.data.JsonSerDe"
	// case "CUSTOM_CSV":
	//	return "org.apache.hadoop.hive.serde2.OpenCSVSerde"
	case SeqFile:
		return "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
	case TxtFile:
		return "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
	case RcFile:
		return "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"
	case ORC:
		return "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
	case OrcFile:
		return "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
	case Parquet:
		return "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
	case Avro:
		return "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
	}
	return ""
}
