package io.lkmx.turmix.functions;

import io.lkmx.turmix.domain.DataRepository;
import io.lkmx.turmix.functions.error.DataValidationException;

/**
 * Represents a unit of work to validate data.
 */
public interface DataValidator {

    void validate(DataRepository data) throws DataValidationException;

}
