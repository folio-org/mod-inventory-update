package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputItem;

public class ItemStorage extends RecordStorage {

    @Override
    protected String getResultSetName() {
        return ITEMS;
    }

    @Override
    protected void declareDependencies() {
        fakeStorage.holdingsStorage.acceptDependant(this, InputItem.HOLDINGS_RECORD_ID);
    }

    @Override
    protected void createRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        int code = insert(new InputItem(recordJson));
        respond(routingContext, recordJson, code);
    }

    @Override
    protected void updateRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        String id = routingContext.pathParam("id");
        int code = update(id, new InputItem(recordJson));
        respond(routingContext, code);
    }

}
