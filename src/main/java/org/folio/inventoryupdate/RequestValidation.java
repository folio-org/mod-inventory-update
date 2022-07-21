package org.folio.inventoryupdate;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.InventoryRecord;

import java.util.ArrayList;
import java.util.List;

public class RequestValidation {

    List<ErrorReport> errors = new ArrayList<>();

    public boolean hasErrors () {
        return errors.size()>0;
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

    public JsonObject getFirstRequestJson () {
        return hasErrors() ? errors.get(0).getRequestJson() : new JsonObject();

    }

}
