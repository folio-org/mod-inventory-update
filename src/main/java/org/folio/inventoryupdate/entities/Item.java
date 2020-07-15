package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

public class Item extends InventoryRecord {

    public Item (JsonObject item) {
        this.jsonRecord = item;
        type = Entity.ITEM;
    }

    public String getHoldingsRecordId () {
        return jsonRecord.getString("holdingsRecordId");
    }

    public boolean hasHoldingsRecordId () {
        return (getHoldingsRecordId() != null && !getHoldingsRecordId().isEmpty());
    }

    public void setHoldingsRecordId (String uuid) {
        jsonRecord.put("holdingsRecordId", uuid);
    }

    public void skipDependants() {
        // has none
    }

}