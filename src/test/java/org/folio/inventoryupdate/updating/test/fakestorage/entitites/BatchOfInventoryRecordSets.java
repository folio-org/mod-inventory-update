package org.folio.inventoryupdate.updating.test.fakestorage.entitites;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class BatchOfInventoryRecordSets {
  JsonObject batchJson;
  private static final String INVENTORY_RECORD_SETS = "inventoryRecordSets";

  public BatchOfInventoryRecordSets () {
    batchJson = new JsonObject();
    batchJson.put(INVENTORY_RECORD_SETS, new JsonArray());
  }

  public BatchOfInventoryRecordSets addRecordSet (InventoryRecordSet recordSet) {
    batchJson.getJsonArray(INVENTORY_RECORD_SETS).add(recordSet.getJson());
    return this;
  }

  public BatchOfInventoryRecordSets addRecordSet (JsonObject recordSet) {
    batchJson.getJsonArray(INVENTORY_RECORD_SETS).add(recordSet);
    return this;
  }

  public JsonObject getJson () {
    return batchJson;
  }
}
