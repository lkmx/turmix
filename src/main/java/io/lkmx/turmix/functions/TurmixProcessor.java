package io.lkmx.turmix.functions;

import io.lkmx.turmix.domain.DataRepository;
import io.lkmx.turmix.functions.error.DataValidationException;
import io.lkmx.turmix.functions.error.ValidationException;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@Builder
public class TurmixProcessor {

    private final List<DataCollector> collectors;

    private final List<DataPreImprover> preImprovers;

    private final List<DataImprover> improvers;

    private final List<DataValidator> validators;

    private final List<DataStore> stores;

    private final List<DataExporter> exporters;

    public void processAll() {
        final DataRepository collected = collect();
        final DataRepository improved = improve(collected);
        final DataRepository validated = validate(improved);
        final DataRepository stored = store(validated);
        export(stored);
    }

    private DataRepository collect() {
        final DataRepository data = new DataRepository();
        // For each collector, we add it to the data repository using the proper dataframe.
        collectors.forEach(collector ->
            data.store(collector.getDataframe(), collector.collect()));
        return data;
    }

    private DataRepository improve(DataRepository data) {
        DataRepository improved = data.clone();

        for (DataImprover improver : improvers) {

            // Run all pre improvers before each improver
            for (DataPreImprover preImprover : preImprovers) {
                preImprover.improve(improved);
            }

            // Improve the data
            improver.improve(improved);

            // Clone the resulted data to preserve immutability
            improved = improved.clone();
        }

        return improved;
    }

    private DataRepository validate(DataRepository data) {
        final DataRepository validated = data.clone();

        boolean hasErrors = false;
        for (DataValidator validator : validators) {
            try {
                validator.validate(validated);
            } catch (DataValidationException e) {
                hasErrors = true;
            }
        }

        if (hasErrors) throw new ValidationException();

        return validated;
    }

    private DataRepository store(DataRepository data) {
        final DataRepository stored = data.clone();
        stores.forEach(store -> store.store(stored));
        return stored;
    }

    private void export(DataRepository data) {
        final DataRepository exported = data.clone();
        exporters.forEach(exporter -> exporter.export(exported));
    }
}
