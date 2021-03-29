package io.lkmx.turmix.collectors;

import io.lkmx.turmix.clients.csv.CsvLoader;
import io.lkmx.turmix.clients.csv.CsvLocalLoaderImpl;
import io.lkmx.turmix.clients.csv.CsvSource;
import io.lkmx.turmix.clients.csv.LineMode;
import io.lkmx.turmix.domain.Dataframe;
import io.lkmx.turmix.functions.DataCollector;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@Builder
@RequiredArgsConstructor
public final class CsvCollector implements DataCollector {

    private final CsvLoader csvLoader = new CsvLocalLoaderImpl();

    @Getter
    private final Dataframe dataframe;

    private final String url;

    private final LineMode lineMode;

    private final String encoding;

    public Dataset<Row> collect() {
        final CsvSource csvSource = CsvSource.builder()
            .url(url)
            .encoding(encoding)
            .lineMode(lineMode)
            .columns(dataframe.descriptor().getColumns())
            .build();
        return csvLoader.loadCsv(csvSource);
    }
}
