package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

public class Item extends InventoryRecord {

    public static final String HOLDINGS_RECORD_ID = "holdingsRecordId";

    public Item (JsonObject item) {
        this.jsonRecord = item;
        entityType = Entity.ITEM;
    }

    public String getHoldingsRecordId () {
        return jsonRecord.getString(HOLDINGS_RECORD_ID);
    }

    public boolean hasHoldingsRecordId () {
        return (getHoldingsRecordId() != null && !getHoldingsRecordId().isEmpty());
    }

    public void setHoldingsRecordId (String uuid) {
        jsonRecord.put(HOLDINGS_RECORD_ID, uuid);
    }

    public void skipDependants() {
        // has none
    }

}