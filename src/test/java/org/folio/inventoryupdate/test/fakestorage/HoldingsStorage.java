package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestHoldingsRecord;

public class HoldingsStorage extends RecordStorage {

    @Override
    protected void createRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        int code = insert(new TestHoldingsRecord(recordJson));
        respond(routingContext, recordJson, code);
    }

    @Override
    protected void updateRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        String id = routingContext.pathParam("id");
        int code = update(id, new TestHoldingsRecord(recordJson));
        respond(routingContext, code);
    }

    @Override
    public String getResultSetName() {
        return "holdingsRecords";
    }

}
