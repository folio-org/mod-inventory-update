package org.folio.inventory.updating.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;

public class InventoryRecordSet {

    private InputInstance instance;

    public InventoryRecordSet(InputInstance instance) {
        this.instance = instance;
    }

    public JsonObject getJson() {
        JsonObject irsJson = new JsonObject();
        irsJson.put("instance", instance.getJson());
        return irsJson;
    }
}
