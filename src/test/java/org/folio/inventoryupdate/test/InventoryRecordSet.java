package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;

public class InventoryRecordSet {

    private TestInstance instance;

    public InventoryRecordSet(TestInstance instance) {
        this.instance = instance;
    }

    public JsonObject getJson() {
        JsonObject irsJson = new JsonObject();
        irsJson.put("instance", instance.getJson());
        return irsJson;
    }
}
