package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;

public class InventoryRecordSet {

    private Instance instance;

    public InventoryRecordSet(Instance instance) {
        this.instance = instance;
    }

    public JsonObject getJson() {
        JsonObject irsJson = new JsonObject();
        irsJson.put("instance", instance.getJson());
        return irsJson;
    }
}
