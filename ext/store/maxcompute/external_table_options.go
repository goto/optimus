package maxcompute

import "strings"

const (
	CSVHandler = "com.aliyun.odps.CsvStorageHandler"
	TSVHandler = "com.aliyun.odps.TsvStorageHandler"
)

const (
	CSV         string                  = "CSV"
	TSV         string                  = "TSV"
	JSON        string                  = "JSON"
	TxtFile     string                  = "TEXTFILE"
	RcFile      string                  = "RCFILE"
	ORC         string                  = "ORC"
	OrcFile     string                  = "ORCFILE"
	SeqFile     string                  = "SEQUENCEFILE"
	Parquet     string                  = "PARQUET"
	Avro        string                  = "AVRO"
	GoogleSheet ExternalTableSourceType = "GOOGLE_SHEETS"
	GoogleDrive ExternalTableSourceType = "GOOGLE_DRIVE"
	LarkSheet   ExternalTableSourceType = "LARK_SHEET"
	OSS         ExternalTableSourceType = "OSS"
)

func handlerForFormat(format string) string {
	// todo: review carefully this part
	switch strings.ToUpper(format) {
	case CSV:
		return CSVHandler
	case TSV:
		return TSVHandler

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
	default:
		return CSVHandler
	}
}
