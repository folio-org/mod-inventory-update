package org.folio.inventoryupdate.entities;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
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

    protected static final Logger logger = LoggerFactory.getLogger("inventory-update");

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

    //TODO: DRY makeParentRelationWithInstanceIdentifier and makeChildRelationWithInstanceIdentifier
    static Future<InstanceRelationship> makeParentRelationWithInstanceIdentifier(OkapiClient client, String instanceId, JsonObject parentJson, String identifierKey) {
        Promise<InstanceRelationship> promise = Promise.promise();
        JsonObject instanceIdentifier = parentJson.getJsonObject(InstanceRelationsController.INSTANCE_IDENTIFIER);
        String hrid = instanceIdentifier.getString(identifierKey);
        InventoryQuery hridQuery = new HridQuery(hrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete(instance -> {
            if (instance.succeeded()) {
                if (instance.result() != null) {
                    JsonObject instanceJson = instance.result();
                    InstanceRelationship relationship = makeRelationship(
                            instanceId,
                            instanceId,
                            instanceJson.getString(InstanceRelationsController.ID),
                            parentJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                    promise.complete(relationship);
                } else {
                    JsonObject provisionalInstanceJson = parentJson.getJsonObject(InstanceToInstanceRelation.PROVISIONAL_INSTANCE);
                    if (provisionalInstanceJson == null) {
                        InstanceRelationship relationship = makeRelationship(
                            instanceId,
                            instanceId,
                            null,
                            parentJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                        relationship.requiresProvisionalInstanceToBeCreated(true);
                        Instance failedCreation = new Instance(new JsonObject());
                        failedCreation.setTransition(Transaction.CREATE);
                        failedCreation.fail();
                        failedCreation.logError("No info provided for creating required provisional instance", 422);
                        relationship.setProvisionalInstance(failedCreation);
                        relationship.setTransition(Transaction.CREATE);
                        relationship.logError( "Referenced parent Instance not found and no provisional Instance info provided; cannot create relationship to non-existing Instance [" + hrid + "], got:" + InstanceRelationsController.LF + parentJson.encodePrettily(),422);
                        relationship.fail();
                        promise.complete(relationship);
                    } else {
                        String title = provisionalInstanceJson.getString(InstanceRelationsController.TITLE);
                        String source = provisionalInstanceJson.getString(InstanceRelationsController.SOURCE);
                        String instanceTypeId = provisionalInstanceJson.getString(InstanceRelationsController.INSTANCE_TYPE_ID);
                        if (title == null || source == null || instanceTypeId == null) {
                            InstanceRelationship relationship = makeRelationship(
                                    instanceId,
                                    instanceId,
                                    null,
                                    parentJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                            Instance failedCreation = new Instance(new JsonObject());
                            failedCreation.setTransition(Transaction.CREATE);
                            failedCreation.fail();
                            failedCreation.logError("Cannot create required provisional instance ["+ hrid + "] unless both title, source and resource type are provided", 422);
                            relationship.setProvisionalInstance(failedCreation);
                            relationship.requiresProvisionalInstanceToBeCreated(true);
                            relationship.setTransition(Transaction.CREATE);
                            relationship.logError( " Cannot create relationship to non-existing Instance [" + hrid + "] unless both title, source and resource type are provided for creating a provisional Instance, got:" + InstanceRelationsController.LF + parentJson.encodePrettily(),422);
                            relationship.fail();
                            promise.complete(relationship);
                        } else {
                            Instance provisionalInstance = prepareProvisionalInstance(hrid, provisionalInstanceJson);
                            InstanceRelationship relationship = makeRelationship(
                                    instanceId,
                                    instanceId,
                                    provisionalInstance.getUUID(),
                                    parentJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                            relationship.requiresProvisionalInstanceToBeCreated(true);
                            relationship.setProvisionalInstance(provisionalInstance);
                            promise.complete(relationship);
                        }
                    }
                }
            } else {
                promise.fail("Error looking up Instance for creating relationship with it: " + instance.cause().getMessage());
            }
        });
        return promise.future();
    }

    static Future<InstanceRelationship> makeChildRelationWithInstanceIdentifier(OkapiClient client, String instanceId, JsonObject childJson, String identifierKey) {
        Promise<InstanceRelationship> promise = Promise.promise();
        String hrid = childJson.getJsonObject(InstanceRelationsController.INSTANCE_IDENTIFIER).getString(identifierKey);
        InventoryQuery hridQuery = new HridQuery(hrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete( instance -> {
            if (instance.succeeded()) {
                if (instance.result() != null) {
                    JsonObject instanceJson = instance.result();
                    InstanceRelationship relationship = makeRelationship(
                            instanceId,
                            instanceJson.getString(InstanceRelationsController.ID),
                            instanceId,
                            childJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                    promise.complete(relationship);
                } else {
                    JsonObject provisionalInstanceJson = childJson.getJsonObject(InstanceToInstanceRelation.PROVISIONAL_INSTANCE);
                    if (provisionalInstanceJson == null) {
                        logger.info("No provisional instance provided");
                        InstanceRelationship relationship = makeRelationship(
                                instanceId,
                                null,
                                instanceId,
                                childJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                        Instance failedCreation = new Instance(new JsonObject());
                        failedCreation.setTransition(Transaction.CREATE);
                        failedCreation.fail();
                        failedCreation.logError("No info provided for creating required provisional instance", 422);
                        relationship.setProvisionalInstance(failedCreation);
                        relationship.requiresProvisionalInstanceToBeCreated(true);
                        relationship.setTransition(Transaction.CREATE);
                        relationship.logError( "Referenced child Instance not found and no provisional Instance info provided; cannot create relationship to non-existing Instance [" + hrid + "], got:" + InstanceRelationsController.LF + childJson.encodePrettily(),422);
                        relationship.fail();
                        promise.complete(relationship);
                    } else {
                        logger.debug("Creating provisional instance");
                        String title = provisionalInstanceJson.getString(InstanceRelationsController.TITLE);
                        String source = provisionalInstanceJson.getString(InstanceRelationsController.SOURCE);
                        String instanceTypeId = provisionalInstanceJson.getString(InstanceRelationsController.INSTANCE_TYPE_ID);
                        if (title == null || source == null || instanceTypeId == null) {
                            InstanceRelationship relationship = makeRelationship(
                                    instanceId,
                                    null,
                                    instanceId,
                                    childJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                            Instance failedCreation = new Instance(new JsonObject());
                            failedCreation.setTransition(Transaction.CREATE);
                            failedCreation.fail();
                            failedCreation.logError("Cannot create required provisional instance ["+ hrid + "] unless both title, source and resource type are provided", 422);
                            relationship.setProvisionalInstance(failedCreation);
                            relationship.requiresProvisionalInstanceToBeCreated(true);
                            relationship.setTransition(Transaction.CREATE);
                            relationship.logError( " Cannot create relationship to non-existing Instance [" + hrid + "] unless both title, source and resource type is provided for creating a provisional Instance, got:" + InstanceRelationsController.LF + childJson.encodePrettily(),422);
                            relationship.fail();
                            promise.complete(relationship);
                        } else {
                            Instance provisionalInstance = prepareProvisionalInstance(hrid, provisionalInstanceJson);
                            InstanceRelationship relationship = makeRelationship(
                                    instanceId,
                                    instanceId,
                                    provisionalInstance.getUUID(),
                                    childJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                            relationship.requiresProvisionalInstanceToBeCreated(true);
                            relationship.setProvisionalInstance(provisionalInstance);
                            logger.debug("Provisional Instance " + provisionalInstance.asJsonString());
                            logger.debug("Relationship: " + relationship.asJsonString());
                            promise.complete(relationship);
                        }
                    }
                }
            } else {
                promise.fail("Error looking up Instance for creating relationship with it: " + instance.cause().getMessage());
            }
        });
        return promise.future();
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
            return (other.getSubInstanceId() != null && other.getSubInstanceId().equals(this.getSubInstanceId()) &&
                    other.getSuperInstanceId() != null && other.getSuperInstanceId().equals(this.getSuperInstanceId()) &&
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
