package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;

public class TestInstanceTitleSuccession extends InventoryRecord {

    public static final String PRECEDING_INSTANCE_ID = "precedingInstanceId";
    public static final String SUCCEEDING_INSTANCE_ID = "succeedingInstanceId";

    public TestInstanceTitleSuccession(JsonObject record) {
        super(record);
    }

    public TestInstanceTitleSuccession() {
        super();
    }

    public String getPrecedingInstanceId () {
        return recordJson.getString(PRECEDING_INSTANCE_ID);
    }

    public String getSucceedingInstanceId () {
        return recordJson.getString(SUCCEEDING_INSTANCE_ID);
    }

    public TestInstanceTitleSuccession setPrecedingInstanceId (String id) {
        recordJson.put(PRECEDING_INSTANCE_ID, id);
        return this;
    }

    public TestInstanceTitleSuccession setSucceedingInstanceId (String id) {
        recordJson.put(SUCCEEDING_INSTANCE_ID, id);
        return this;
    }
}
