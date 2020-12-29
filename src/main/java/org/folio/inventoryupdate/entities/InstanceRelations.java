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

    public static final String INSTANCE_RELATIONS = "instanceRelations";
    public static final String EXISTING_RELATIONS = "existingRelations";
    public static final String PARENT_INSTANCES = "parentInstances";
    public static final String CHILD_INSTANCES = "childInstances";
    public static final String INSTANCE_IDENTIFIER = "instanceIdentifier";

    private List<InstanceRelationship> parentRelations = new ArrayList<>();
    private List<InstanceRelationship> childRelations = new ArrayList<>();
    private JsonObject relationshipJson = new JsonObject();
    protected final Logger logger = LoggerFactory.getLogger("inventory-update");
    private List<Instance> interimInstance = new ArrayList<>();

    public InstanceRelations () {}

    public List<InstanceRelationship> getInstanceRelationsByTransactionType (InventoryRecord.Transaction transition) {
        List<InstanceRelationship> records = new ArrayList();
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

    public void markRelationsForDeletion () {
        for (InstanceRelationship relation : getRelations()) {
            relation.setTransition(InventoryRecord.Transaction.DELETE);
        }
    }

    public Future<Void> makeRelationshipRecordsFromIdentifiers(OkapiClient client, String instanceId) {
        Promise promise = Promise.promise();
        if (relationshipJson.containsKey(PARENT_INSTANCES) || relationshipJson.containsKey(CHILD_INSTANCES)) {

            getParentRelations(client, instanceId, relationshipJson.getJsonArray(PARENT_INSTANCES)).onComplete(parents -> {
                if (parents.succeeded()) {
                    this.parentRelations = parents.result();
                } else {
                    promise.fail("There was a problem looking up Instance IDs to build parent relationships: " + parents.cause().getMessage());
                }
                getChildRelations(client, instanceId, relationshipJson.getJsonArray(CHILD_INSTANCES)).onComplete (children -> {
                   if (children.succeeded()) {
                       this.childRelations = children.result();
                       promise.complete();
                   } else {
                       promise.fail("There was a problem looking up Instance IDs to build child relationships: " + children.cause().getMessage());
                   }

                });
            });
        } else {
            promise.complete();
        }
        return promise.future();
    }

    private static Future<List<InstanceRelationship>> getParentRelations (OkapiClient client, String instanceId, JsonArray parentIdentifiers) {
        Promise<List<InstanceRelationship>> promise = Promise.promise();
        if (parentIdentifiers != null) {
            List<Future> relationsFutures = new ArrayList<Future>();
            for (Object o : parentIdentifiers) {
                JsonObject relationshipJson = (JsonObject) o;
                if (relationshipJson.containsKey(INSTANCE_IDENTIFIER) && relationshipJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey("hrid")) {
                    relationsFutures.add(getParentRelation(client, instanceId, relationshipJson, HRID ));
                }
            }
            CompositeFuture.all(relationsFutures).onComplete( parentInstances -> {
               if (parentInstances.succeeded()) {
                   if (parentInstances.result().list() != null) {
                       List<InstanceRelationship> relations = new ArrayList();
                       for (Object o : parentInstances.result().list()) {
                           InstanceRelationship relationship =  (InstanceRelationship) o;
                           relations.add(relationship);
                       }
                       promise.complete(relations);
                   }
               } else {
                   promise.fail("Failed to look up parent Instances in storage: " + parentInstances.cause().getMessage());
               }
            });
        } else {
            promise.complete(null);
        }
        return promise.future();
    }

    private static Future<List<InstanceRelationship>> getChildRelations (OkapiClient client, String instanceId, JsonArray identifiers) {
        Promise<List<InstanceRelationship>> promise = Promise.promise();
        if (identifiers != null) {
            List<Future> relationsFutures = new ArrayList<Future>();
            for (Object o : identifiers) {
                JsonObject relationshipJson = (JsonObject) o;
                if (relationshipJson.containsKey(INSTANCE_IDENTIFIER) && relationshipJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey("hrid")) {
                    relationsFutures.add(getChildRelation(client, instanceId, relationshipJson, HRID ));
                }
            }
            CompositeFuture.all(relationsFutures).onComplete( instances -> {
                if (instances.succeeded()) {
                    if (instances.result().list() != null) {
                        List<InstanceRelationship> relations = new ArrayList();
                        for (Object o : instances.result().list()) {
                            InstanceRelationship relationship =  (InstanceRelationship) o;
                            relations.add(relationship);
                        }
                        promise.complete(relations);
                    }
                }
            });
        } else {
            promise.complete(null);
        }
        return promise.future();
    }

    private static Future<InstanceRelationship> getParentRelation (OkapiClient client, String instanceId, JsonObject parentIdentifier, String identifierKey) {
        Promise<InstanceRelationship> promise = Promise.promise();
        String hrid = parentIdentifier.getJsonObject(INSTANCE_IDENTIFIER).getString(identifierKey);
        InventoryQuery hridQuery = new HridQuery(hrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete( instance -> {
            if (instance.succeeded()) {
                if (instance.result() != null) {
                    JsonObject instanceJson = instance.result();
                    InstanceRelationship relationship = InstanceRelationship.makeRelationship(
                            instanceId,
                            instanceId,
                            instanceJson.getString("id"),
                            parentIdentifier.getString(InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID));
                    promise.complete(relationship);
                } else {
                    // todo: create temporary instance and relation to it
                    Instance interimInstance = prepareInterimInstance(hrid);
                    promise.complete(null);
                }
            }
        });
        return promise.future();
    }

    private static Instance prepareInterimInstance (String hrid) {
        JsonObject json = new JsonObject();
        json.put("hrid", hrid);
        json.put("id", UUID.randomUUID().toString());
        // todo: set material type
        // todo: tag it 'interim'
        return new Instance(json);
    }

    private static Future<InstanceRelationship> getChildRelation (OkapiClient client, String instanceId, JsonObject parentIdentifier, String identifierKey) {
        Promise<InstanceRelationship> promise = Promise.promise();
        String hrid = parentIdentifier.getJsonObject(INSTANCE_IDENTIFIER).getString(identifierKey);
        InventoryQuery hridQuery = new HridQuery(hrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete( instance -> {
            if (instance.succeeded()) {
                if (instance.result() != null) {
                    JsonObject instanceJson = instance.result();
                    InstanceRelationship relationship = InstanceRelationship.makeRelationship(
                            instanceId,
                            instanceJson.getString("id"),
                            instanceId,
                            parentIdentifier.getString(InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID));
                    promise.complete(relationship);
                } else {
                    // todo: create temporary instance and relation to it
                    promise.complete(null);
                }
            }
        });
        return promise.future();
    }


    public void registerRelationshipJsonRecords(String instanceId, JsonObject instanceRelations) {
        if (instanceRelations.containsKey(EXISTING_RELATIONS)) {
            JsonArray existingRelations = instanceRelations.getJsonArray(EXISTING_RELATIONS);
            for (Object o : existingRelations) {
                InstanceRelationship relationship = InstanceRelationship.makeRelationship(instanceId, (JsonObject) o);
                if (relationship.isRelationToChild()) {
                    childRelations.add(relationship);
                } else {
                    parentRelations.add(relationship);
                }
            }
        }
    }

    @Override
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        JsonArray parents = new JsonArray();
        for (InstanceRelationship relation : getParentRelations()) {
            parents.add(relation.asJson());
        }
        json.put("parentInstances", parents);
        JsonArray children = new JsonArray();
        for (InstanceRelationship relation : getChildRelations()) {
            children.add(relation.asJson());
        }
        json.put("childInstances", children);
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

    public List<InstanceRelationship> getParentRelations() {
        return parentRelations;
    }

    public List<InstanceRelationship> getChildRelations() {
        return childRelations;
    }

    private boolean hasRelations () {
        return  (parentRelations.size() + childRelations.size() > 0);
    }

    public List<InstanceRelationship> getRelations () {
        List<InstanceRelationship> all = new ArrayList<>();
        if (parentRelations != null) all.addAll(parentRelations);
        if (childRelations != null) all.addAll(childRelations);
        return all;
    }
}
