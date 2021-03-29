package io.lkmx.turmix.functions.error;

import lombok.Getter;

import java.util.Collections;
import java.util.List;

public class DataValidationException extends RuntimeException {

    @Getter
    private List<ValidationErrorEntry> errors;

    public DataValidationException(String message) {
        super(message);
        this.errors = Collections.emptyList();
    }

    public DataValidationException(String message, List<ValidationErrorEntry> errors) {
        super(message);
        this.errors = errors;
    }
}
