package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;

import java.util.UUID;

public abstract class InventoryRecord {

    public static String ID = "id";
    protected JsonObject recordJson;

    public InventoryRecord() {
        recordJson = new JsonObject();
    }

    public InventoryRecord(JsonObject record) {
        recordJson = record;
    }

    public JsonObject getJson() {
        return recordJson;
    }

    public InventoryRecord setId (String id) {
        recordJson.put(ID, id);
        return this;
    }

    public boolean hasId() {
        return recordJson.getString(ID) != null;
    }

    public InventoryRecord generateId () {
        recordJson.put(ID,UUID.randomUUID().toString());
        return this;
    }

    public String getId () {
        return recordJson.getString(ID);
    }

}
