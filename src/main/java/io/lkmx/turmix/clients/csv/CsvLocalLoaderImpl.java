package io.lkmx.turmix.clients.csv;

import io.lkmx.turmix.clients.spark.SessionFactory;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvLocalLoaderImpl implements CsvLoader {

    @Override
    public Dataset<Row> loadCsv(CsvSource csv) {

        // Opens the session
        SparkSession spark = SessionFactory.getSession();

        // Imports the data
        DataFrameReader dfr = spark.read()
            .format("csv")
            .option("header", "true")
            .option("encoding", csv.getEncoding());

        // If the source is multiline we enable the option for Spark
        if (csv.getLineMode() == LineMode.MULTI_LINE) {
            dfr = dfr.option("multiLine", "true");
        }

        // Loads the dataframe
        Dataset<Row> df = dfr.load(csv.getUrl());

        // If the count of both the columns defined for the csv and the
        // columns in the dataframe doesn't match, throw an exception
        if (df.columns().length != csv.getColumns().length) {
            throw new RuntimeException("Columns defined for csv " + csv.getUrl() + " doesn't match with the columns in the dataframe");
        }

        // Rename each column
        for (int index = 0; index < csv.getColumns().length; index++) {
            df = df.withColumnRenamed(df.columns()[index], csv.getColumns()[index]);
        }

        return df;
    }
}
