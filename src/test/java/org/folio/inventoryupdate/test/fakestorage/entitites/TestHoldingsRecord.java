package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;
import org.junit.Test;

public class TestHoldingsRecord extends InventoryRecord {

    public static String HRID = "hrid";
    public static String CALL_NUMBER = "callNumber";
    public static String INSTANCE_ID = "instanceId";


    public TestHoldingsRecord(JsonObject record) {
        super(record);
    }

    public TestHoldingsRecord() {
        super();
    }

    public TestHoldingsRecord setInstanceId (String instanceId) {
        recordJson.put(INSTANCE_ID,instanceId);
        return this;
    }

    public TestHoldingsRecord setHrid (String hrid) {
        recordJson.put(HRID,hrid);
        return this;
    }

    public TestHoldingsRecord setCallNumber (String callNumber) {
        recordJson.put(CALL_NUMBER, callNumber);
        return this;
    }

}
