package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;

public class InputItem extends InventoryRecord {

    public static String HRID = "hrid";
    public static String BARCODE = "barcode";
    public static String HOLDINGS_RECORD_ID = "holdingsRecordId";

    public InputItem(JsonObject record) {
        super(record);
    }

    public InputItem() {
        super();
        setStatus("Unknown");
    }

    public InputItem setHrid (String hrid) {
        recordJson.put(HRID,hrid);
        return this;
    }
    public InputItem setBarcode (String barcode) {
        recordJson.put(BARCODE, barcode);
        return this;
    }

    public InputItem setHoldingsRecordId (String holdingsRecordId) {
        recordJson.put(HOLDINGS_RECORD_ID, holdingsRecordId);
        return this;
    }

    public InputItem setStatus (String statusName) {
        JsonObject status = new JsonObject();
        status.put("name", statusName);
        recordJson.put("status", status);
        return this;
    }

}
