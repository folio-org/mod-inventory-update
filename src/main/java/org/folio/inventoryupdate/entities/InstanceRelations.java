package org.folio.inventoryupdate.entities;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.HridQuery;
import org.folio.inventoryupdate.InventoryQuery;
import org.folio.inventoryupdate.InventoryStorage;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.folio.inventoryupdate.entities.InventoryRecordSet.HRID;

public class InstanceRelations extends JsonRepresentation {

    // JSON property keys
    public static final String INSTANCE_RELATIONS = "instanceRelations";
    public static final String EXISTING_RELATIONS = "existingRelations";
    public static final String PARENT_INSTANCES = "parentInstances";
    public static final String CHILD_INSTANCES = "childInstances";
    public static final String INSTANCE_IDENTIFIER = "instanceIdentifier";
    public static final String ID = "id";
    public static final String INSTANCE_TYPE_ID = "instanceTypeId";
    public static final String TITLE = "title";
    public static final String SOURCE = "source";
    public static final String PROVISIONAL_INSTANCE = "provisionalInstance";
    // EOF JSON property keys

    private static String LF = System.lineSeparator();
    private List<InstanceRelationship> parentRelations = new ArrayList<>();
    private List<InstanceRelationship> childRelations = new ArrayList<>();
    private JsonObject relationshipJson = new JsonObject();
    protected final Logger logger = LoggerFactory.getLogger("inventory-update");

    public InstanceRelations () {}

    public List<InstanceRelationship> getInstanceRelationsByTransactionType (InventoryRecord.Transaction transition) {
        List<InstanceRelationship> records = new ArrayList<>();
        for (InstanceRelationship record : getRelations())  {
            logger.debug("Looking at relationship with transaction type " + record.getTransaction());
            if (record.getTransaction() == transition && ! record.skipped()) {
                records.add(record);
            }
        }
        logger.debug("Found " + records.size() + " to " + transition);
        return records;
    }

    public void setRelationshipsJson (JsonObject json) {
        relationshipJson = json;
    }

    public void markAllRelationsForDeletion() {
        for (InstanceRelationship relation : getRelations()) {
            relation.setTransition(InventoryRecord.Transaction.DELETE);
        }
    }

    public boolean hasRelation (InstanceRelationship relation) {
        for (InstanceRelationship relationHere : getRelations()) {
            logger.info("Comparing " + relation.toString() + " with " + relationHere.toString());
            if (relation.equals(relationHere)) {
                logger.info("Same relation");
                return true;
            } else {
                logger.info("Different relations");
            }

        }
        return false;
    }

    public Future<Void> makeRelationshipRecordsFromIdentifiers(OkapiClient client, String instanceId) {
        Promise<Void> promise = Promise.promise();
        if (relationshipJson.containsKey(PARENT_INSTANCES) || relationshipJson.containsKey(CHILD_INSTANCES)) {

            makeParentRelationsWithInstanceIdentifiers(client, instanceId, relationshipJson.getJsonArray(PARENT_INSTANCES)).onComplete(parents -> {
                if (parents.succeeded()) {
                    if (parents.result() != null) {
                        this.parentRelations = parents.result();
                    }
                    makeChildRelationsWithInstanceIdentifiers(client, instanceId, relationshipJson.getJsonArray(CHILD_INSTANCES)).onComplete (children -> {
                        if (children.succeeded()) {
                            if (children.result() != null) {
                                this.childRelations = children.result();
                            }
                            promise.complete();
                        } else {
                            promise.fail("There was a problem looking up or creating Instance IDs to build child relationships:" + LF + "  " + children.cause().getMessage());
                        }

                    });
                } else {
                    promise.fail("There was a problem looking up or creating Instance IDs to build parent relationships:" + LF + "  " + parents.cause().getMessage());
                }
            });
        } else {
            promise.complete();
        }
        return promise.future();
    }

    private static Future<List<InstanceRelationship>> makeParentRelationsWithInstanceIdentifiers(OkapiClient client, String instanceId, JsonArray parentIdentifiers) {
        Promise<List<InstanceRelationship>> promise = Promise.promise();
        if (parentIdentifiers != null) {
            @SuppressWarnings("rawtypes")
            List<Future> relationsFutures = new ArrayList<>();
            for (Object o : parentIdentifiers) {
                JsonObject relationshipJson = (JsonObject) o;
                if (relationshipJson.containsKey(INSTANCE_IDENTIFIER) && relationshipJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey(HRID)) {
                    relationsFutures.add(makeParentRelationWithInstanceIdentifier(client, instanceId, relationshipJson, HRID ));
                }
            }
            CompositeFuture.all(relationsFutures).onComplete( parentInstances -> {
               if (parentInstances.succeeded()) {
                   if (parentInstances.result().list() != null) {
                       List<InstanceRelationship> relations = new ArrayList<>();
                       for (Object o : parentInstances.result().list()) {
                           InstanceRelationship relationship =  (InstanceRelationship) o;
                           relations.add(relationship);
                       }
                       promise.complete(relations);
                   }
               } else {
                   promise.fail("Failed to construct parent relationships with provided parent Instance identifiers:" + LF + "  " + parentInstances.cause().getMessage());
               }
            });
        } else {
            promise.complete(null);
        }
        return promise.future();
    }

    private static Future<List<InstanceRelationship>> makeChildRelationsWithInstanceIdentifiers(OkapiClient client, String instanceId, JsonArray identifiers) {
        Promise<List<InstanceRelationship>> promise = Promise.promise();
        if (identifiers != null) {
            @SuppressWarnings("rawtypes")
            List<Future> relationsFutures = new ArrayList<>();
            for (Object o : identifiers) {
                JsonObject relationshipJson = (JsonObject) o;
                if (relationshipJson.containsKey(INSTANCE_IDENTIFIER) && relationshipJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey(HRID)) {
                    relationsFutures.add(makeChildRelationWithInstanceIdentifier(client, instanceId, relationshipJson, HRID ));
                }
            }
            CompositeFuture.all(relationsFutures).onComplete( instances -> {
                if (instances.succeeded()) {
                    if (instances.result().list() != null) {
                        List<InstanceRelationship> relations = new ArrayList<>();
                        for (Object o : instances.result().list()) {
                            InstanceRelationship relationship =  (InstanceRelationship) o;
                            relations.add(relationship);
                        }
                        promise.complete(relations);
                    }
                } else  {
                    promise.fail("Failed to construct child relationships with provided child Instance identifiers:" + LF + "  " + instances.cause().getMessage());
                }
            });
        } else {
            promise.complete(null);
        }
        return promise.future();
    }

    private static Future<InstanceRelationship> makeParentRelationWithInstanceIdentifier(OkapiClient client, String instanceId, JsonObject parentJson, String identifierKey) {
        Promise<InstanceRelationship> promise = Promise.promise();
        JsonObject instanceIdentifier = parentJson.getJsonObject(INSTANCE_IDENTIFIER);
        String hrid = instanceIdentifier.getString(identifierKey);
        InventoryQuery hridQuery = new HridQuery(hrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete( instance -> {
            if (instance.succeeded()) {
                if (instance.result() != null) {
                    JsonObject instanceJson = instance.result();
                    InstanceRelationship relationship = InstanceRelationship.makeRelationshipWithInstanceIdentifier(
                            instanceId,
                            instanceId,
                            instanceJson.getString(ID),
                            parentJson.getString(InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID));
                    promise.complete(relationship);
                } else {
                    JsonObject provisionalInstanceJson = parentJson.getJsonObject(PROVISIONAL_INSTANCE);
                    if (provisionalInstanceJson == null) {
                        promise.fail(" Referenced parent Instance not found and no provisional Instance info provided; cannot create relationship to non-existing Instance [" + hrid + "], got:" + LF + parentJson.encodePrettily());
                    } else {
                        String title = provisionalInstanceJson.getString(TITLE);
                        String source = provisionalInstanceJson.getString(SOURCE);
                        String instanceTypeId = provisionalInstanceJson.getString(INSTANCE_TYPE_ID);
                        if (title == null || source == null || instanceTypeId == null) {
                            promise.fail(" Cannot create relationship to non-existing Instance [" + hrid + "] unless both title, source and resource type is provided for creating a provisional Instance, got:" + LF + parentJson.encodePrettily());
                        } else {
                            Instance provisionalInstance = prepareProvisionalInstance(hrid, provisionalInstanceJson);
                            InstanceRelationship relationship = InstanceRelationship.makeRelationshipWithInstanceIdentifier(
                                    instanceId,
                                    instanceId,
                                    provisionalInstance.getUUID(),
                                    parentJson.getString(InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID));
                            relationship.setProvisionalInstance(provisionalInstance);
                            promise.complete(relationship);
                        }
                    }
                }
            }
        });
        return promise.future();
    }

    private static Future<InstanceRelationship> makeChildRelationWithInstanceIdentifier(OkapiClient client, String instanceId, JsonObject childJson, String identifierKey) {
        Promise<InstanceRelationship> promise = Promise.promise();
        String hrid = childJson.getJsonObject(INSTANCE_IDENTIFIER).getString(identifierKey);
        InventoryQuery hridQuery = new HridQuery(hrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete( instance -> {
            if (instance.succeeded()) {
                if (instance.result() != null) {
                    JsonObject instanceJson = instance.result();
                    InstanceRelationship relationship = InstanceRelationship.makeRelationshipWithInstanceIdentifier(
                            instanceId,
                            instanceJson.getString(ID),
                            instanceId,
                            childJson.getString(InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID));
                    promise.complete(relationship);
                } else {
                    JsonObject provisionalInstanceJson = childJson.getJsonObject(PROVISIONAL_INSTANCE);
                    if (provisionalInstanceJson == null) {
                        promise.fail(" Referenced parent Instance not found and no provisional Instance info provided; cannot create relationship to non-existing Instance [" + hrid + "], got:" + LF + childJson.encodePrettily());
                    } else {
                        String title = provisionalInstanceJson.getString(TITLE);
                        String source = provisionalInstanceJson.getString(SOURCE);
                        String instanceTypeId = provisionalInstanceJson.getString(INSTANCE_TYPE_ID);
                        if (title == null || source == null || instanceTypeId == null) {
                            promise.fail(" Cannot create relationship to non-existing Instance [" + hrid + "] unless both title, source and resource type is provided for creating a provisional Instance, got:" + LF + childJson.encodePrettily());
                        } else {
                            Instance provisionalInstance = prepareProvisionalInstance(hrid, provisionalInstanceJson);
                            InstanceRelationship relationship = InstanceRelationship.makeRelationshipWithInstanceIdentifier(
                                    instanceId,
                                    instanceId,
                                    provisionalInstance.getUUID(),
                                    childJson.getString(InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID));
                            relationship.setProvisionalInstance(provisionalInstance);
                            promise.complete(relationship);
                        }
                    }
                }
            }
        });
        return promise.future();
    }

    /**
     * Create a temporary Instance to create a relationship to.
     * @param hrid
     * @param provisionalInstanceJson
     * @return
     */
    private static Instance prepareProvisionalInstance (String hrid, JsonObject provisionalInstanceJson) {
        JsonObject json = new JsonObject(provisionalInstanceJson.toString());
        if (! json.containsKey(HRID)) {
            json.put(HRID, hrid);
        }
        if (! json.containsKey(ID)) {
            json.put(ID, UUID.randomUUID().toString());
        }
        return new Instance(json);
    }

    public void registerRelationshipJsonRecords(String instanceId, JsonObject instanceRelations) {
        if (instanceRelations.containsKey(EXISTING_RELATIONS)) {
            JsonArray existingRelations = instanceRelations.getJsonArray(EXISTING_RELATIONS);
            for (Object o : existingRelations) {
                InstanceRelationship relationship = InstanceRelationship.makeRelationshipFromJson(instanceId, (JsonObject) o);
                if (relationship.isRelationToChild()) {
                    childRelations.add(relationship);
                } else {
                    parentRelations.add(relationship);
                }
            }
        }
    }

    public Future<JsonObject> handleInstanceRelationCreatesIfAny (OkapiClient okapiClient) {

        Promise<JsonObject> promise = Promise.promise();

        @SuppressWarnings("rawtypes")
        List<Future> provisionalInstancesFutures = new ArrayList<>();
        for (InstanceRelationship relation : relationshipsToCreate()) {
            if (relation.requiresProvisionalInstanceToBeCreated()) {
                provisionalInstancesFutures.add(InventoryStorage.postInventoryRecord(okapiClient, relation.getProvisionalInstance()));
            }
        }
        CompositeFuture.join(provisionalInstancesFutures).onComplete( allProvisionalInstancesCreated -> {
            if (allProvisionalInstancesCreated.succeeded()) {
                @SuppressWarnings("rawtypes")
                List<Future> createFutures = new ArrayList<>();
                for (InstanceRelationship relation : relationshipsToCreate()) {
                    createFutures.add(InventoryStorage.postInventoryRecord(okapiClient, relation));
                }
                CompositeFuture.join(createFutures).onComplete( allRelationsCreated -> {
                    if (allRelationsCreated.succeeded()) {
                        promise.complete(new JsonObject());
                    } else {
                        promise.fail("There was an error creating instance relations:" + LF + "  " + allRelationsCreated.cause().getMessage());
                    }
                });
            } else {
                promise.fail("There was an error creating provisional Instances:" + LF + "  " + allProvisionalInstancesCreated.cause().getMessage());
            }
        });
        return promise.future();
    }

    // moved over from UpdatePlan
    public List<InstanceRelationship> relationshipsToCreate () {
        return getInstanceRelationsByTransactionType(InventoryRecord.Transaction.CREATE);
    }



    @Override
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        JsonArray parents = new JsonArray();
        for (InstanceRelationship relation : makeParentRelationsWithInstanceIdentifiers()) {
            parents.add(relation.asJson());
        }
        json.put(PARENT_INSTANCES, parents);
        JsonArray children = new JsonArray();
        for (InstanceRelationship relation : makeChildRelationsWithInstanceIdentifiers()) {
            children.add(relation.asJson());
        }
        json.put(CHILD_INSTANCES, children);
        return json;
    }

    @Override
    public boolean hasErrors() {
        return false;
    }

    @Override
    public JsonArray getErrors() {
        return null;
    }

    public List<InstanceRelationship> makeParentRelationsWithInstanceIdentifiers() {
        return parentRelations;
    }

    public List<InstanceRelationship> makeChildRelationsWithInstanceIdentifiers() {
        return childRelations;
    }

    public List<InstanceRelationship> getRelations () {
        List<InstanceRelationship> all = new ArrayList<>();
        if (parentRelations != null) all.addAll(parentRelations);
        if (childRelations != null) all.addAll(childRelations);
        return all;
    }

    @Override
    public String toString () {
        StringBuilder str = new StringBuilder();
        for (InstanceRelationship rel : getRelations()) {
            str.append(rel.toString());
        }
        return str.toString();
    }

}
