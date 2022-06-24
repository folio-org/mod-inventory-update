package org.folio.inventoryupdate;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

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

    public void registerError(JsonObject jsonMessage) {
        errors.add(new ValidationError(jsonMessage));
    }

    public String toString () {
        StringBuilder errorString = new StringBuilder();
        errors.stream().forEach(error -> errorString.append(System.lineSeparator() + error.messageJson.encode()));
        return errorString.toString();
    }

    public String asJsonString () {
        return asJson().encode();
    }

    public JsonObject asJson () {
        JsonObject errorJson = new JsonObject();
        JsonArray errorArray = new JsonArray();
        errorJson.put("errors", errorArray);
        errors.stream().forEach(error -> errorArray.add(error.messageJson));
        return errorJson;
    }

    public static class ValidationError {

        JsonObject messageJson = new JsonObject();

        public ValidationError(String message) {
            messageJson.put("message", message);
        }
        public ValidationError(JsonObject message) {
            messageJson.put("message", message);
        }
    }



    public void addValidation (RequestValidation validation) {
        errors.addAll(validation.errors);
    }

}
