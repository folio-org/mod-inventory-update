package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

public class InstanceRelationship extends InventoryRecord {

    private static final String SUB_INSTANCE_ID = "subInstanceId";
    private static final String SUPER_INSTANCE_ID = "superInstanceId";
    private static final String INSTANCE_RELATIONSHIP_TYPE_ID = "instanceRelationshipId";

    public InstanceRelationship (JsonObject instanceRelationJson) {
        jsonRecord = instanceRelationJson;
        type = Entity.INSTANCE_RELATIONSHIP;
    }

    public InstanceRelationship(String relationshipId, String subInstanceId, String superInstanceId, String instanceRelationshipTypeId) {
        jsonRecord = new JsonObject();
        jsonRecord.put("id", relationshipId);
        jsonRecord.put(SUB_INSTANCE_ID, subInstanceId);
        jsonRecord.put(SUPER_INSTANCE_ID, superInstanceId);
        jsonRecord.put(INSTANCE_RELATIONSHIP_TYPE_ID, instanceRelationshipTypeId);
        type = Entity.INSTANCE_RELATIONSHIP;
    }

    public void skipDependants() {
        // has none
    }

}
