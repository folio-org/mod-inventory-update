package org.folio.inventoryupdate.unittests.fakestorage.entities;

import io.vertx.core.json.JsonObject;

public class InventoryRecordSet {

    private final InputInstance instance;

    public InventoryRecordSet(InputInstance instance) {
        this.instance = instance;
    }

    public JsonObject getJson() {
        JsonObject irsJson = new JsonObject();
        irsJson.put("instance", instance.getJson());
        return irsJson;
    }
}
