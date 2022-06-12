package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.List;

public class RequestValidation {

    List<ValidationError> errors = new ArrayList<>();

    public boolean hasErrors () {
        return errors.size()>0;
    }

    public boolean passed() {
        return ! hasErrors();
    }

    public void registerError(String message) {
        errors.add(new ValidationError(message));
    }

    public String toString () {
        StringBuilder errorString = new StringBuilder();
        errors.stream().forEach(error -> errorString.append(System.lineSeparator() + error.message));
        return errorString.toString();
    }

    public static class ValidationError {
        public ValidationError(String message) {
            this.message = message;
        }
        public String message;
    }

    public void addValidation (RequestValidation validation) {
        errors.addAll(validation.errors);
    }

}
