package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestInstance;

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
