package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestInstanceRelationship;

public class InstanceRelationshipStorage extends RecordStorage {


    @Override
    public String getResultSetName() {
        return "instanceRelationships";
    }

    public void createRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        int code = insert(new TestInstanceRelationship(recordJson));
        respond(routingContext, recordJson, code);
    }

    public void updateRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        String id = routingContext.pathParam("id");
        int code = update(id, new TestInstanceRelationship(recordJson));
        respond(routingContext, code);
    }

}
