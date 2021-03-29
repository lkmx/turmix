package io.lkmx.turmix.clients.csv;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class CsvSource {

    private final String url;

    private final String[] columns;

    private final LineMode lineMode;

    private final String encoding;

}
