package maxcompute

func handlerForFormat(format string) string {
	switch format {
	// the built-in text extractor for CSV and TSV
	case "CSV":
		return "com.aliyun.odps.CsvStorageHandler"
	case "TSV":
		return "com.aliyun.odps.TsvStorageHandler"

	// Extractors for inbuilt Open Source Data Formats
	case "JSON":
		return "org.apache.hive.hcatalog.data.JsonSerDe"
	//case "CUSTOM_CSV":
	//	return "org.apache.hadoop.hive.serde2.OpenCSVSerde"
	case "SEQUENCEFILE":
		return "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
	case "TEXTFILE":
		return "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
	case "RCFILE":
		return "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"
	case "ORC":
		return "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
	case "ORCFILE":
		return "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
	case "PARQUET":
		return "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
	case "AVRO":
		return "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
	}
	return ""
}
