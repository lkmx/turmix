package io.lkmx.turmix.domain;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

public class DataRepository {

    private final Map<String, Dataset<Row>> data;

    public DataRepository() {
        this.data = new HashMap<>();
    }

    public void store(Dataframe key, Dataset<Row> dataframe) {
        data.put(key.name(), dataframe);
    }

    public Dataset<Row> fetch(Dataframe key) {
        Dataset<Row> dataset = data.get(key.name());
        if (dataset == null) throw new RuntimeException("Dataset with key " + key + " couldn't be fetch");
        return dataset;
    }

    public DataRepository clone() {
        final DataRepository repo = new DataRepository();
        repo.data.putAll(this.data);
        return repo;
    }
}
