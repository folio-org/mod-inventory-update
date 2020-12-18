package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

public class InstanceRelationship extends InventoryRecord {

    private String instanceId;
    public static final String SUB_INSTANCE_ID = "subInstanceId";
    public static final String SUPER_INSTANCE_ID = "superInstanceId";
    public static final String INSTANCE_RELATIONSHIP_TYPE_ID = "instanceRelationshipTypeId";

    private String subInstanceHrid = null;
    private String superInstanceHrid = null;

    public static InstanceRelationship makeRelationshipFromExisting(String instanceId, JsonObject instanceRelationJson) {
        InstanceRelationship relation = new InstanceRelationship();
        relation.instanceId = instanceId;
        relation.jsonRecord = instanceRelationJson;
        relation.type = Entity.INSTANCE_RELATIONSHIP;
        return relation;
    }

    public static InstanceRelationship makeParentRelationshipWithHRID(String instanceId, String parentHrid, String instanceRelationshipTypeId) {
        InstanceRelationship relation = new InstanceRelationship();
        relation.jsonRecord = new JsonObject();
        relation.instanceId = instanceId;
        relation.jsonRecord.put(SUB_INSTANCE_ID, instanceId);
        relation.jsonRecord.put(INSTANCE_RELATIONSHIP_TYPE_ID, instanceRelationshipTypeId);
        relation.superInstanceHrid = parentHrid;
        return relation;
    }

    public static InstanceRelationship makeChildRelationshipWithHRID(String instanceId, String childHrid, String instanceRelationshipTypeId) {
        InstanceRelationship relation = new InstanceRelationship();
        relation.jsonRecord = new JsonObject();
        relation.instanceId = instanceId;
        relation.jsonRecord.put(SUPER_INSTANCE_ID, instanceId);
        relation.jsonRecord.put(INSTANCE_RELATIONSHIP_TYPE_ID, instanceRelationshipTypeId);
        relation.subInstanceHrid = childHrid;
        return relation;
    }


    public InstanceRelationship() {}

    public boolean isRelationToParent () {
        return instanceId.equals(getSubInstanceId());
    }

    public boolean isRelationToChild () {
        return instanceId.equals(getSuperInstanceId());
    }

    public String getSubInstanceId () {
        return jsonRecord.getString(SUB_INSTANCE_ID);
    }

    public String getSuperInstanceId() {
        return jsonRecord.getString(SUPER_INSTANCE_ID);
    }

    public String getInstanceRelationshipTypeId () {
        return jsonRecord.getString(INSTANCE_RELATIONSHIP_TYPE_ID);
    }

    public void skipDependants() {
        // has none
    }

}
