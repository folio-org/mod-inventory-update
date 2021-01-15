package org.folio.inventoryupdate.entities;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.HridQuery;
import org.folio.inventoryupdate.InventoryQuery;
import org.folio.inventoryupdate.InventoryStorage;
import org.folio.okapi.common.OkapiClient;

public class InstanceRelationship extends InstanceToInstanceRelation {

    private String instanceId;
    public static final String SUB_INSTANCE_ID = "subInstanceId";
    public static final String SUPER_INSTANCE_ID = "superInstanceId";
    public static final String INSTANCE_RELATIONSHIP_TYPE_ID = "instanceRelationshipTypeId";

    public static InstanceRelationship makeRelationshipFromJsonRecord(String instanceId, JsonObject instanceRelationJson) {
        InstanceRelationship relation = new InstanceRelationship();
        relation.instanceId = instanceId;
        relation.jsonRecord = instanceRelationJson;
        relation.type = Entity.INSTANCE_RELATIONSHIP;
        return relation;
    }

    public static InstanceRelationship makeRelationship(String instanceId, String subInstanceId, String superInstanceId, String instanceRelationshipTypeId) {
        InstanceRelationship relation = new InstanceRelationship();
        relation.instanceId = instanceId;
        relation.jsonRecord = new JsonObject();
        relation.jsonRecord.put(SUB_INSTANCE_ID, subInstanceId);
        relation.jsonRecord.put(SUPER_INSTANCE_ID, superInstanceId);
        relation.jsonRecord.put(INSTANCE_RELATIONSHIP_TYPE_ID, instanceRelationshipTypeId);
        relation.type = Entity.INSTANCE_RELATIONSHIP;
        return relation;
    }

    static Future<InstanceRelationship> makeParentRelationWithInstanceIdentifier(OkapiClient client, String instanceId, JsonObject parentJson, String identifierKey) {
        Promise<InstanceRelationship> promise = Promise.promise();
        JsonObject instanceIdentifier = parentJson.getJsonObject(InstanceToInstanceRelations.INSTANCE_IDENTIFIER);
        String hrid = instanceIdentifier.getString(identifierKey);
        InventoryQuery hridQuery = new HridQuery(hrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete(instance -> {
            if (instance.succeeded()) {
                if (instance.result() != null) {
                    JsonObject instanceJson = instance.result();
                    InstanceRelationship relationship = makeRelationship(
                            instanceId,
                            instanceId,
                            instanceJson.getString(InstanceToInstanceRelations.ID),
                            parentJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                    promise.complete(relationship);
                } else {
                    JsonObject provisionalInstanceJson = parentJson.getJsonObject(InstanceToInstanceRelation.PROVISIONAL_INSTANCE);
                    if (provisionalInstanceJson == null) {
                        promise.fail(" Referenced parent Instance not found and no provisional Instance info provided; cannot create relationship to non-existing Instance [" + hrid + "], got:" + InstanceToInstanceRelations.LF + parentJson.encodePrettily());
                    } else {
                        String title = provisionalInstanceJson.getString(InstanceToInstanceRelations.TITLE);
                        String source = provisionalInstanceJson.getString(InstanceToInstanceRelations.SOURCE);
                        String instanceTypeId = provisionalInstanceJson.getString(InstanceToInstanceRelations.INSTANCE_TYPE_ID);
                        if (title == null || source == null || instanceTypeId == null) {
                            promise.fail(" Cannot create relationship to non-existing Instance [" + hrid + "] unless both title, source and resource type is provided for creating a provisional Instance, got:" + InstanceToInstanceRelations.LF + parentJson.encodePrettily());
                        } else {
                            Instance provisionalInstance = prepareProvisionalInstance(hrid, provisionalInstanceJson);
                            InstanceRelationship relationship = makeRelationship(
                                    instanceId,
                                    instanceId,
                                    provisionalInstance.getUUID(),
                                    parentJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                            relationship.setProvisionalInstance(provisionalInstance);
                            promise.complete(relationship);
                        }
                    }
                }
            }
        });
        return promise.future();
    }

    static Future<InstanceRelationship> makeChildRelationWithInstanceIdentifier(OkapiClient client, String instanceId, JsonObject childJson, String identifierKey) {
        Promise<InstanceRelationship> promise = Promise.promise();
        String hrid = childJson.getJsonObject(InstanceToInstanceRelations.INSTANCE_IDENTIFIER).getString(identifierKey);
        InventoryQuery hridQuery = new HridQuery(hrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete( instance -> {
            if (instance.succeeded()) {
                if (instance.result() != null) {
                    JsonObject instanceJson = instance.result();
                    InstanceRelationship relationship = makeRelationship(
                            instanceId,
                            instanceJson.getString(InstanceToInstanceRelations.ID),
                            instanceId,
                            childJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                    promise.complete(relationship);
                } else {
                    JsonObject provisionalInstanceJson = childJson.getJsonObject(InstanceToInstanceRelation.PROVISIONAL_INSTANCE);
                    if (provisionalInstanceJson == null) {
                        promise.fail(" Referenced parent Instance not found and no provisional Instance info provided; cannot create relationship to non-existing Instance [" + hrid + "], got:" + InstanceToInstanceRelations.LF + childJson.encodePrettily());
                    } else {
                        String title = provisionalInstanceJson.getString(InstanceToInstanceRelations.TITLE);
                        String source = provisionalInstanceJson.getString(InstanceToInstanceRelations.SOURCE);
                        String instanceTypeId = provisionalInstanceJson.getString(InstanceToInstanceRelations.INSTANCE_TYPE_ID);
                        if (title == null || source == null || instanceTypeId == null) {
                            promise.fail(" Cannot create relationship to non-existing Instance [" + hrid + "] unless both title, source and resource type is provided for creating a provisional Instance, got:" + InstanceToInstanceRelations.LF + childJson.encodePrettily());
                        } else {
                            Instance provisionalInstance = prepareProvisionalInstance(hrid, provisionalInstanceJson);
                            InstanceRelationship relationship = makeRelationship(
                                    instanceId,
                                    instanceId,
                                    provisionalInstance.getUUID(),
                                    childJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                            relationship.setProvisionalInstance(provisionalInstance);
                            promise.complete(relationship);
                        }
                    }
                }
            }
        });
        return promise.future();
    }

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

    @Override
    public boolean equals (Object o) {
        if (o instanceof InstanceRelationship) {
            InstanceRelationship other = (InstanceRelationship) o;
            return (other.getSubInstanceId().equals(this.getSubInstanceId()) &&
                    other.getSuperInstanceId().equals(this.getSuperInstanceId()) &&
                    other.getInstanceRelationshipTypeId().equals(this.getInstanceRelationshipTypeId()));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public String toString () {
        StringBuilder str = new StringBuilder();
        str.append("// Sub: ").append(jsonRecord.getString(SUB_INSTANCE_ID))
           .append(" Super: ").append(jsonRecord.getString(SUPER_INSTANCE_ID))
           .append(" TypeId: ").append(jsonRecord.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
        return str.toString();
    }

}
