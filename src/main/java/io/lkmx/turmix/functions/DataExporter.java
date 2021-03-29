package io.lkmx.turmix.functions;

import io.lkmx.turmix.domain.DataRepository;

public interface DataExporter {
    void export(DataRepository data);
}
