package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;

public class InputInstanceRelationship extends InventoryRecord {

    public static final String SUB_INSTANCE_ID = "subInstanceId";
    public static final String SUPER_INSTANCE_ID = "superInstanceId";
    public static final String INSTANCE_RELATIONSHIP_TYPE_ID = "instanceRelationshipTypeId";

    public static final String INSTANCE_IDENTIFIER = "instanceIdentifier";
    public static final String PROVISIONAL_INSTANCE = "provisionalInstance";

    public InputInstanceRelationship setSubInstanceId (String id) {
        recordJson.put(SUB_INSTANCE_ID, id);
        return this;
    }

    public InputInstanceRelationship setSuperInstanceId (String id) {
        recordJson.put(SUPER_INSTANCE_ID, id);
        return this;
    }

    public InputInstanceRelationship setInstanceRelationshipTypeId (String id) {
        recordJson.put(INSTANCE_RELATIONSHIP_TYPE_ID, id);
        return this;
    }

    public InputInstanceRelationship setInstanceIdentifierHrid(String hrid) {
        if (hrid != null) {
            recordJson.put(INSTANCE_IDENTIFIER, new JsonObject().put("hrid", hrid));
        } else {
            recordJson.put(INSTANCE_IDENTIFIER, new JsonObject());
        }

        return this;
    }

    public InputInstanceRelationship setInstanceIdentifierUuid (String uuid) {
        recordJson.put(INSTANCE_IDENTIFIER, new JsonObject().put("uuid", uuid));
        return this;

    }

    public InputInstanceRelationship setProvisionalInstance (JsonObject instanceJson) {
        recordJson.put(PROVISIONAL_INSTANCE, instanceJson);
        return this;
    }

}
