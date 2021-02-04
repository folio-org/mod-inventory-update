package org.folio.inventoryupdate.test;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class InstanceTitleSuccession extends InventoryRecord {

    public static final String PRECEDING_INSTANCE_ID = "precedingInstanceId";
    public static final String SUCCEEDING_INSTANCE_ID = "succeedingInstanceId";
    private String instanceId;

    public InstanceTitleSuccession (JsonObject record) {
        super(record);
    }

    public String getPrecedingInstanceId () {
        return recordJson.getString(PRECEDING_INSTANCE_ID);
    }

    public String getSucceedingInstanceId () {
        return recordJson.getString(SUCCEEDING_INSTANCE_ID);
    }

    public InstanceTitleSuccession setPrecedingInstanceId (String id) {
        recordJson.put(PRECEDING_INSTANCE_ID, id);
        return this;
    }

    public InstanceTitleSuccession setSucceedingInstanceId (String id) {
        recordJson.put(SUCCEEDING_INSTANCE_ID, id);
        return this;
    }
}
