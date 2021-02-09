package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;

public class InputInstanceRelationship extends InventoryRecord {

    public static final String SUB_INSTANCE_ID = "subInstanceId";
    public static final String SUPER_INSTANCE_ID = "superInstanceId";

    public static final String INSTANCE_IDENTIFIER = "instanceIdentifier";

    public InputInstanceRelationship(JsonObject record) {
        super(record);
    }

    public InputInstanceRelationship() {
        super();
    }

    public InputInstanceRelationship setSubInstanceId (String id) {
        recordJson.put(SUB_INSTANCE_ID, id);
        return this;
    }

    public InputInstanceRelationship setSuperInstanceId (String id) {
        recordJson.put(SUPER_INSTANCE_ID, id);
        return this;
    }

    public InputInstanceRelationship setInstanceIdentifierHrid(String hrid) {
        recordJson.put(INSTANCE_IDENTIFIER, new JsonObject().put("hrid", hrid));
        return this;
    }

}