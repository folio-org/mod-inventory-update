package org.folio.inventoryupdate;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.InventoryRecord;

import java.util.ArrayList;
import java.util.List;

public class RequestValidation {

    List<ErrorResponse> errors = new ArrayList<>();

    public boolean hasErrors () {
        return errors.size()>0;
    }

    public boolean passed() {
        return ! hasErrors();
    }

    public void registerError(ErrorResponse error) {
        errors.add(error);
    }

    public String toString () {
        StringBuilder errorString = new StringBuilder();
        errors.stream().forEach(error -> errorString.append(System.lineSeparator() + error.asJsonString()));
        return errorString.toString();
    }

    public JsonObject asJson () {
        JsonObject errorJson = new JsonObject();
        JsonArray errorArray = new JsonArray();
        errorJson.put("errors", errorArray);
        errors.stream().forEach(error -> errorArray.add(error.asJson()));
        return errorJson;
    }

    public void addValidation (RequestValidation validation) {
        errors.addAll(validation.errors);
    }

    public InventoryRecord.Entity firstEntityType () {
        return (hasErrors() ? errors.get(0).getEntityType() : null);
    }

    public String firstMessage() {
        return (hasErrors() ? errors.get(0).getMessageAsString() : "");
    }

    public String firstShortMessage() {
        return (hasErrors() ? errors.get(0).getShortMessage() : "");
    }

    public JsonObject firstEntity () {
        return (hasErrors() ? errors.get(0).getEntity() : new JsonObject());
    }


}
