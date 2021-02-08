package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;

public class TestHoldingsRecord extends InventoryRecord {
    public TestHoldingsRecord(JsonObject record) {
        super(record);
    }

    public TestHoldingsRecord() {
        super();
    }

}
