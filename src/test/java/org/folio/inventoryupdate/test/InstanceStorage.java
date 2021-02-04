package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class InstanceStorage extends RecordStorage {

    public String getResultSetName() {
        return INSTANCES;
    }

    public void createInstance(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        int code = insert(new Instance(recordJson));
        respond(routingContext, recordJson, code);
    }

    public void updateInstance(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        String id = routingContext.pathParam("id");
        int code = update(id, new Instance(recordJson));
        respond(routingContext, code);
    }


}
