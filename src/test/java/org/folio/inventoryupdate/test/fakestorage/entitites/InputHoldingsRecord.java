package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;

public class InputHoldingsRecord extends InventoryRecord {

    public static String HRID = "hrid";
    public static String CALL_NUMBER = "callNumber";
    public static String INSTANCE_ID = "instanceId";


    public InputHoldingsRecord(JsonObject record) {
        super(record);
    }

    public InputHoldingsRecord() {
        super();
    }

    public InputHoldingsRecord setInstanceId (String instanceId) {
        recordJson.put(INSTANCE_ID,instanceId);
        return this;
    }

    public InputHoldingsRecord setHrid (String hrid) {
        recordJson.put(HRID,hrid);
        return this;
    }

    public InputHoldingsRecord setCallNumber (String callNumber) {
        recordJson.put(CALL_NUMBER, callNumber);
        return this;
    }

}
