package io.lkmx.turmix.clients.csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface CsvLoader {
    Dataset<Row> loadCsv(CsvSource csv);
}
