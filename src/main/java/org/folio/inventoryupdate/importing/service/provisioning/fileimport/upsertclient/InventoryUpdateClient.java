package org.folio.inventoryupdate.importing.service.provisioning.fileimport.upsertclient;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public abstract class InventoryUpdateClient {

  public abstract Future<UpdateResponse> inventoryDeletion(JsonObject theRecord);

  public abstract Future<UpdateResponse> inventoryUpsert(JsonObject recordSets);

  public record UpdateResponse(int statusCode, JsonObject json) {
    public JsonObject getMetrics() {
      if (json != null) {
        return json().getJsonObject("metrics");
      } else {
        return new JsonObject();
      }
    }

    public JsonArray getErrors() {
      if (json != null && json.containsKey("errors")) {
        return json.getJsonArray("errors");
      } else {
        return new JsonArray();
      }
    }

    public boolean hasErrorObjects() {
      return !getErrors().isEmpty() && getErrors().getJsonObject(0).getValue("message") instanceof JsonObject;
    }
  }
}
