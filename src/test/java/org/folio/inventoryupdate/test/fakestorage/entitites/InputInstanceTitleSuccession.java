package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;

public class InputInstanceTitleSuccession extends InventoryRecord {

    public static final String PRECEDING_INSTANCE_ID = "precedingInstanceId";
    public static final String SUCCEEDING_INSTANCE_ID = "succeedingInstanceId";
    public static final String INSTANCE_IDENTIFIER = "instanceIdentifier";

    public InputInstanceTitleSuccession(JsonObject record) {
        super(record);
    }

    public InputInstanceTitleSuccession() {
        super();
    }

    public String getPrecedingInstanceId () {
        return recordJson.getString(PRECEDING_INSTANCE_ID);
    }

    public String getSucceedingInstanceId () {
        return recordJson.getString(SUCCEEDING_INSTANCE_ID);
    }

    public InputInstanceTitleSuccession setPrecedingInstanceId (String id) {
        recordJson.put(PRECEDING_INSTANCE_ID, id);
        return this;
    }

    public InputInstanceTitleSuccession setSucceedingInstanceId (String id) {
        recordJson.put(SUCCEEDING_INSTANCE_ID, id);
        return this;
    }

    public InputInstanceTitleSuccession setInstanceIdentifierHrid(String hrid) {
        recordJson.put(INSTANCE_IDENTIFIER, new JsonObject().put("hrid", hrid));
        return this;
    }

}
