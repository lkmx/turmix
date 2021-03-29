package io.lkmx.turmix.functions.error;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class ValidationErrorEntry {

    @Getter
    private long lineNumber;

    @Getter
    private String description;
}


