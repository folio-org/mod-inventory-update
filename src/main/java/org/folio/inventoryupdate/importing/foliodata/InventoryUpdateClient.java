package org.folio.inventoryupdate.importing.foliodata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public abstract class InventoryUpdateClient {

  public abstract Future<UpdateResponse> inventoryDeletion (JsonObject theRecord);
  public abstract Future<UpdateResponse> inventoryUpsert (JsonObject recordSets);

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
  }
}
