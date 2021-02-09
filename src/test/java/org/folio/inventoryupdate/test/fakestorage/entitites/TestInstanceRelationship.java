package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;

public class TestInstanceRelationship extends InventoryRecord {

    public static final String SUB_INSTANCE_ID = "subInstanceId";
    public static final String SUPER_INSTANCE_ID = "superInstanceId";

    public static final String INSTANCE_IDENTIFIER = "instanceIdentifier";

    public TestInstanceRelationship(JsonObject record) {
        super(record);
    }

    public TestInstanceRelationship() {
        super();
    }

    public TestInstanceRelationship setSubInstanceId (String id) {
        recordJson.put(SUB_INSTANCE_ID, id);
        return this;
    }

    public TestInstanceRelationship setSuperInstanceId (String id) {
        recordJson.put(SUPER_INSTANCE_ID, id);
        return this;
    }

    public TestInstanceRelationship setInstanceIdentifierHrid(String hrid) {
        recordJson.put(INSTANCE_IDENTIFIER, new JsonObject().put("hrid", hrid));
        return this;
    }

}
