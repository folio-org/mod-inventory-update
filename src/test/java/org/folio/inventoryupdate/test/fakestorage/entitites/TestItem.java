package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;

public class TestItem extends InventoryRecord {

    public static String BARCODE = "barcode";
    public static String HOLDINGS_RECORD_ID = "holdingsRecordId";

    public TestItem(JsonObject record) {
        super(record);
    }

    public TestItem() {
        super();
    }

    public TestItem setBarcode (String barcode) {
        recordJson.put(BARCODE, barcode);
        return this;
    }

    public TestItem setHoldingsRecordId (String holdingsRecordId) {
        recordJson.put(HOLDINGS_RECORD_ID, holdingsRecordId);
        return this;
    }

}
