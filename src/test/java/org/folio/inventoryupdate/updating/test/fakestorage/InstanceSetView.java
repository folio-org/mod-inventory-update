package org.folio.inventoryupdate.updating.test.fakestorage;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.util.HashSet;

public class InstanceSetView extends RecordStorage {

    public String getResultSetName() {
        return INSTANCE_SETS;
    }

    @Override
    protected void declareDependencies() {
        // read only
    }

    @Override
    protected void declareMandatoryProperties() {
        // read only
    }

    @Override
    public void getRecords(RoutingContext routingContext) {
      try {
        var limit = Integer.valueOf(routingContext.request().getParam("limit"));
        if (limit > 10) {
          respondWithMessage(routingContext, "InstanceSetView limit must be <= 10", 400);
          return;
        }
        var query = decode(routingContext.request().getParam("query"));
        var instancesObject = fakeStorage.instanceStorage.buildJsonRecordsResponse(query);
        var instances = instancesObject.getJsonArray(INSTANCES);
        var instanceSets = new JsonArray();
        for (var instance : instances) {
            instanceSets.add(getInstanceSet((JsonObject) instance));
        }
        var responseJson = new JsonObject().put("instanceSets", instanceSets);
        respond(routingContext, responseJson, 200);
      } catch (Exception e) {
        respondWithMessage(routingContext, "Error on getting InstanceSetView records: " + e.getMessage(), 500);
      }
    }

    private JsonObject getInstanceSet(JsonObject instance) {
        if (fakeStorage.precedingSucceedingStorage.failOnGetRecords) {
            throw new RuntimeException("fakeStorage.precedingSucceedingStorage.failOnGetRecords");
        }

        var instanceId = instance.getString("id");
        var holdings = new JsonArray();
        var holdingsIds = new HashSet<String>();
        fakeStorage.holdingsStorage.records.forEach((holdingId, holdingRecord) -> {
            if (holdingRecord.getJson().getString("instanceId").equals(instanceId)) {
                holdings.add(holdingRecord.getJson());
                holdingsIds.add(holdingId);
            }
        });
        var items = new JsonArray();
        fakeStorage.itemStorage.records.forEach((itemId, itemRecord) -> {
            var holdingId = itemRecord.getJson().getString("holdingsRecordId");
            if (holdingsIds.contains(holdingId)) {
                items.add(itemRecord.getJson());
            }
        });
        var precedingTitles = new JsonArray();
        var succeedingTitles = new JsonArray();
        fakeStorage.precedingSucceedingStorage.records.forEach((id, inventoryRecord) -> {
            var json = inventoryRecord.getJson();
            if (instanceId.equals(json.getString("precedingInstanceId"))) {
                succeedingTitles.add(json);
            }
            if (instanceId.equals(json.getString("succeedingInstanceId"))) {
                precedingTitles.add(json);
            }
        });
        var superInstanceRelationships = new JsonArray();
        var subInstanceRelationships = new JsonArray();
        fakeStorage.instanceRelationshipStorage.records.forEach((id, inventoryRecord) -> {
            var json = inventoryRecord.getJson();
            if (instanceId.equals(json.getString("superInstanceId"))) {
                subInstanceRelationships.add(json);
            }
            if (instanceId.equals(json.getString("subInstanceId"))) {
                superInstanceRelationships.add(json);
            }
        });
        return new JsonObject()
                        .put("id", instanceId)
                        .put("instance", instance)
                        .put("holdingsRecords", holdings)
                        .put("items", items)
                        .put("precedingTitles", precedingTitles)
                        .put("succeedingTitles", succeedingTitles)
                        .put("superInstanceRelationships", superInstanceRelationships)
                        .put("subInstanceRelationships", subInstanceRelationships);
    }

}
