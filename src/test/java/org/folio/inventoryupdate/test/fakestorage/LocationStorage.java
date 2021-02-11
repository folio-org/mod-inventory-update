package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputLocation;

public class LocationStorage extends RecordStorage {


    @Override
    protected String getResultSetName() {
        return LOCATIONS;
    }

    @Override
    protected void declareDependencies() {
        // Locations has none in fake storage
    }

    @Override
    protected void createRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        int code = insert(new InputLocation(recordJson));
        respond(routingContext, recordJson, code);
    }

    @Override
    protected void updateRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        String id = routingContext.pathParam("id");
        int code = update(id, new InputLocation(recordJson));
        respond(routingContext, code);
    }

}
