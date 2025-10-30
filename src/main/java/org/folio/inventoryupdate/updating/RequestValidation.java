package org.folio.inventoryupdate.updating;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.updating.entities.InventoryRecord;

import java.util.ArrayList;
import java.util.List;

public class RequestValidation {

    List<ErrorReport> errors = new ArrayList<>();

    public boolean hasErrors () {
        return !errors.isEmpty();
    }

    public boolean passed() {
        return ! hasErrors();
    }

    public void registerError(ErrorReport error) {
        errors.add(error);
    }

    public void addValidation (RequestValidation validation) {
        errors.addAll(validation.errors);
    }

    public InventoryRecord.Entity firstEntityType () {
        return (hasErrors() ? errors.getFirst().getEntityType() : null);
    }

    public String firstMessage() {
        return (hasErrors() ? errors.getFirst().getMessageAsString() : "");
    }

    public String firstShortMessage() {
        return (hasErrors() ? errors.getFirst().getShortMessage() : "");
    }

    public JsonObject firstEntity () {
        return (hasErrors() ? errors.getFirst().getEntity() : new JsonObject());
    }

    public JsonObject getFirstRequestJson () {
        return hasErrors() ? errors.getFirst().getRequestJson() : new JsonObject();

    }

}
