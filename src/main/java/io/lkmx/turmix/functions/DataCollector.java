package io.lkmx.turmix.functions;

import io.lkmx.turmix.domain.Dataframe;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataCollector {

    Dataframe getDataframe();

    Dataset<Row> collect();

}
