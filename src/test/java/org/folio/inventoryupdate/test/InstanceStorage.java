package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InstanceStorage extends RecordStorage {
    public final static String INSTANCES = "instances";

    private List<Instance> getInstances () {
        return super.getRecordStream().map( record -> (Instance) record).collect(Collectors.toCollection(ArrayList::new));
    }

    private JsonObject getInstancesAsJsonResultSet (String query) {
        JsonObject response = new JsonObject();
        JsonArray instancesJson = new JsonArray();
        getInstances().forEach( instance -> {
            if (instance.match(query)) {
                instancesJson.add(instance.getJson());
            }});
        response.put(INSTANCES, instancesJson);
        response.put(TOTAL_RECORDS, instancesJson.size());
        return response;
    }

    /**
     * Inventory Storage handler
     * @param routingContext
     */
    public void getInstancesByQuery(RoutingContext routingContext) {
        final String query = decode(routingContext.request().getParam("query"));
        routingContext.request().endHandler(res -> {
            respond(routingContext, getInstancesAsJsonResultSet(query), 200);
        });
        routingContext.request().exceptionHandler(res -> {
            respondWithMessage(routingContext, res);
        });
    }

    public void getInstanceById(RoutingContext routingContext) {
        final String id = routingContext.pathParam("id");
        Instance instance = (Instance) getRecord(id);

        routingContext.request().endHandler(res -> {
            respond(routingContext, instance.getJson(), 200);
        });
        routingContext.request().exceptionHandler(res -> {
            respondWithMessage(routingContext, res);
        });
    }

    public void createInstance(RoutingContext routingContext) {
        JsonObject instanceJson = new JsonObject(routingContext.getBodyAsString());
        int code = insert(new Instance(instanceJson));
        respond(routingContext, instanceJson, code);
    }

    public void updateInstance(RoutingContext routingContext) {
        JsonObject instanceJson = new JsonObject(routingContext.getBodyAsString());
        String id = routingContext.pathParam("id");
        int code = update(id, new Instance(instanceJson));
        respond(routingContext, code);
    }


}
