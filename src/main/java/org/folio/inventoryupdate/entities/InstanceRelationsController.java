package org.folio.inventoryupdate.entities;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.InventoryStorage;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.folio.inventoryupdate.entities.InventoryRecordSet.HRID;

public class InstanceRelationsController extends JsonRepresentation {

    // JSON property keys
    public static final String INSTANCE_RELATIONS = "instanceRelations";
    public static final String EXISTING_PARENT_CHILD_RELATIONS = "existingParentChildRelations";
    public static final String PARENT_INSTANCES = "parentInstances";
    public static final String CHILD_INSTANCES = "childInstances";
    public static final String SUCCEEDING_TITLES = "succeedingTitles";
    public static final String PRECEDING_TITLES = "precedingTitles";
    public static final String EXISTING_PRECEDING_SUCCEEDING_TITLES = "existingPrecedingSucceedingTitles";
    public static final String INSTANCE_IDENTIFIER = "instanceIdentifier";
    public static final String ID = "id";
    public static final String INSTANCE_TYPE_ID = "instanceTypeId";
    public static final String TITLE = "title";
    public static final String SOURCE = "source";
    // EOF JSON property keys

    public static final String LF = System.lineSeparator();
    private List<InstanceRelationship> parentRelations = new ArrayList<>();
    private List<InstanceRelationship> childRelations = new ArrayList<>();
    private List<InstanceTitleSuccession> succeedingTitles = new ArrayList<>();
    private List<InstanceTitleSuccession> precedingTitles = new ArrayList<>();

    private JsonObject instanceRelationsJson = new JsonObject();
    protected final Logger logger = LoggerFactory.getLogger("inventory-update");

    public InstanceRelationsController() {}

    public List<InstanceToInstanceRelation> getInstanceRelationsByTransactionType (InventoryRecord.Transaction transition) {
        List<InstanceToInstanceRelation> records = new ArrayList<>();
        for (InstanceToInstanceRelation record : getInstanceToInstanceRelations())  {
            // logger.debug("Looking at relation (" + record.entityType() + ", " + record.asJsonString() + ") with transaction type " + record.getTransaction());
            if (record.getTransaction() == transition && ! record.skipped()) {
                records.add(record);
            }
        }
        // logger.debug("Found " + records.size() + " to " + transition);
        return records;
    }

    public void setInstanceRelationsJson(JsonObject json) {
        instanceRelationsJson = json;
    }

    public void markAllRelationsForDeletion() {
        for (InstanceToInstanceRelation relation : getInstanceToInstanceRelations()) {
            relation.setTransition(InventoryRecord.Transaction.DELETE);
        }
    }

    public boolean hasThisRelation(InstanceToInstanceRelation relation) {
        for (InstanceToInstanceRelation relationHere : getInstanceToInstanceRelations()) {
            if (relation.equals(relationHere)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasInstanceRelationships () {
        return getRelationships().size()>0;
    }

    public boolean hasTitleSuccessions () {
        return getTitleSuccessions().size()>0;
    }

    public Future<Void> makeInstanceRelationRecordsFromIdentifiers(OkapiClient client, String instanceId) {
        Promise<Void> promise = Promise.promise();
        if (instanceRelationsJson.containsKey(PARENT_INSTANCES)
                || instanceRelationsJson.containsKey(CHILD_INSTANCES)
                || instanceRelationsJson.containsKey(SUCCEEDING_TITLES)
                || instanceRelationsJson.containsKey(PRECEDING_TITLES)) {
            makeParentRelationsFromIdentifiers(client, instanceId, instanceRelationsJson.getJsonArray(PARENT_INSTANCES)).onComplete(parents -> {
                    makeChildRelationsFromIdentifiers(client, instanceId, instanceRelationsJson.getJsonArray(CHILD_INSTANCES)).onComplete (children -> {
                        makeSucceedingTitlesFromIdentifiers(client, instanceId, instanceRelationsJson.getJsonArray(SUCCEEDING_TITLES)).onComplete(succeedingTitles -> {
                            makePrecedingTitlesFromIdentifiers(client, instanceId, instanceRelationsJson.getJsonArray(PRECEDING_TITLES)).onComplete(precedingTitles -> {
                                StringBuilder errorMessages = new StringBuilder();
                                if (parents.succeeded()) {
                                    if (parents.result() != null) {
                                        this.parentRelations = parents.result();
                                    }
                                } else {
                                    errorMessages.append(LF + "There was a problem looking up or creating Instance IDs to build parent relationships:" + LF + "  " + parents.cause().getMessage());
                                }
                                if (children.succeeded()) {
                                    if (children.result() != null) {
                                        this.childRelations = children.result();
                                    }
                                } else {
                                    errorMessages.append(LF + "There was a problem looking up or creating Instance IDs to build child relationships:" + LF + "  " + children.cause().getMessage());
                                }
                                if (succeedingTitles.succeeded()) {
                                    if (succeedingTitles.result() != null) {
                                        this.succeedingTitles = succeedingTitles.result();
                                    }
                                } else {
                                    errorMessages.append(LF + "There was a problem looking up or creating Instance IDs to build succeeding titles links:" + LF + "  " + succeedingTitles.cause().getMessage());
                                }
                                if (precedingTitles.succeeded()) {
                                    if (precedingTitles.result() != null) {
                                        this.precedingTitles = precedingTitles.result();
                                    }
                                } else {
                                    errorMessages.append(LF + "There was a problem looking up or creating Instance IDs to build preceding titles links:" + LF + "  " + precedingTitles.cause().getMessage());
                                }
                                if (parents.succeeded() && children.succeeded() && succeedingTitles.succeeded() && precedingTitles.succeeded()) {
                                    promise.complete();
                                } else {
                                    promise.fail(errorMessages.toString());
                                }
                            });
                        });
                    });
            });
        } else {
            promise.complete();
        }
        return promise.future();
    }

    private static Future<List<InstanceRelationship>> makeParentRelationsFromIdentifiers(OkapiClient client, String instanceId, JsonArray parentIdentifiers) {
        Promise<List<InstanceRelationship>> promise = Promise.promise();
        if (parentIdentifiers != null) {
            @SuppressWarnings("rawtypes")
            List<Future> relationsFutures = new ArrayList<>();
            for (Object o : parentIdentifiers) {
                JsonObject relationshipJson = (JsonObject) o;
                if (relationshipJson.containsKey(INSTANCE_IDENTIFIER) && relationshipJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey(HRID)) {
                    relationsFutures.add(InstanceRelationship.makeParentRelationWithInstanceIdentifier(client, instanceId, relationshipJson, HRID ));
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

    private static Future<List<InstanceRelationship>> makeChildRelationsFromIdentifiers(OkapiClient client, String instanceId, JsonArray identifiers) {
        Promise<List<InstanceRelationship>> promise = Promise.promise();
        if (identifiers != null) {
            @SuppressWarnings("rawtypes")
            List<Future> relationsFutures = new ArrayList<>();
            for (Object o : identifiers) {
                JsonObject relationshipJson = (JsonObject) o;
                if (relationshipJson.containsKey(INSTANCE_IDENTIFIER) && relationshipJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey(HRID)) {
                    relationsFutures.add(InstanceRelationship.makeChildRelationWithInstanceIdentifier(client, instanceId, relationshipJson, HRID ));
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

    private static Future<List<InstanceTitleSuccession>> makeSucceedingTitlesFromIdentifiers (OkapiClient client, String instanceId, JsonArray identifiers) {
        Promise<List<InstanceTitleSuccession>> promise = Promise.promise();
        if (identifiers != null) {
            @SuppressWarnings("rawtypes")
            List<Future> titlesFutures = new ArrayList<>();
            for (Object o : identifiers) {
                JsonObject successionJson = (JsonObject) o;
                if (successionJson.containsKey(INSTANCE_IDENTIFIER) && successionJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey(HRID)) {
                    titlesFutures.add(InstanceTitleSuccession.makeSucceedingTitleWithInstanceIdentifier(client, instanceId, successionJson, HRID ));
                }
            }
            CompositeFuture.all(titlesFutures).onComplete( instances -> {
                if (instances.succeeded()) {
                    if (instances.result().list() != null) {
                        List<InstanceTitleSuccession> relations = new ArrayList<>();
                        for (Object o : instances.result().list()) {
                            InstanceTitleSuccession relation =  (InstanceTitleSuccession) o;
                            relations.add(relation);
                        }
                        promise.complete(relations);
                    }
                } else  {
                    promise.fail("Failed to construct links to succeeding titles with provided Instance identifiers:" + LF + "  " + instances.cause().getMessage());
                }
            });
        } else {
            promise.complete(null);
        }
        return promise.future();
    }

    private static Future<List<InstanceTitleSuccession>> makePrecedingTitlesFromIdentifiers (OkapiClient client, String instanceId, JsonArray identifiers) {
        Promise<List<InstanceTitleSuccession>> promise = Promise.promise();
        if (identifiers != null) {
            @SuppressWarnings("rawtypes")
            List<Future> titlesFutures = new ArrayList<>();
            for (Object o : identifiers) {
                JsonObject successionJson = (JsonObject) o;
                if (successionJson.containsKey(INSTANCE_IDENTIFIER) && successionJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey(HRID)) {
                    titlesFutures.add(InstanceTitleSuccession.makePrecedingTitleWithInstanceIdentifier(client, instanceId, successionJson, HRID ));
                }
            }
            CompositeFuture.all(titlesFutures).onComplete( instances -> {
                if (instances.succeeded()) {
                    if (instances.result().list() != null) {
                        List<InstanceTitleSuccession> relations = new ArrayList<>();
                        for (Object o : instances.result().list()) {
                            InstanceTitleSuccession relation =  (InstanceTitleSuccession) o;
                            relations.add(relation);
                        }
                        promise.complete(relations);
                    }
                } else  {
                    promise.fail("Failed to construct links for preceding titles with provided Instance identifiers:" + LF + "  " + instances.cause().getMessage());
                }
            });
        } else {
            promise.complete(null);
        }
        return promise.future();
    }


    public void registerRelationshipJsonRecords(String instanceId, JsonObject instanceRelations) {
        if (instanceRelations.containsKey(EXISTING_PARENT_CHILD_RELATIONS)) {
            JsonArray existingRelations = instanceRelations.getJsonArray(EXISTING_PARENT_CHILD_RELATIONS);
            for (Object o : existingRelations) {
                InstanceRelationship relationship = InstanceRelationship.makeRelationshipFromJsonRecord(instanceId, (JsonObject) o);
                if (relationship.isRelationToChild()) {
                    childRelations.add(relationship);
                } else {
                    parentRelations.add(relationship);
                }
            }
        }
        if (instanceRelations.containsKey(EXISTING_PRECEDING_SUCCEEDING_TITLES)) {
            JsonArray existingTitles = instanceRelations.getJsonArray(EXISTING_PRECEDING_SUCCEEDING_TITLES);
            for (Object o : existingTitles) {
                InstanceTitleSuccession relation = InstanceTitleSuccession.makeInstanceTitleSuccessionFromJsonRecord(instanceId, (JsonObject) o);
                if (relation.isSucceedingTitle()) {
                    succeedingTitles.add(relation);
                } else {
                    precedingTitles.add(relation);
                }
            }
        }
    }

    public Future<JsonObject> handleInstanceRelationCreatesIfAny (OkapiClient okapiClient) {

        Promise<JsonObject> promise = Promise.promise();

        @SuppressWarnings("rawtypes")
        List<Future> provisionalInstancesFutures = new ArrayList<>();
        for (InstanceToInstanceRelation relation : instanceRelationsToCreate()) {
            if (relation.requiresProvisionalInstanceToBeCreated() && relation.hasPreparedProvisionalInstance()) {
                provisionalInstancesFutures.add(InventoryStorage.postInventoryRecord(okapiClient, relation.getProvisionalInstance()));
            }
        }
        CompositeFuture.join(provisionalInstancesFutures).onComplete( allProvisionalInstancesCreated -> {
            if (allProvisionalInstancesCreated.succeeded()) {
                @SuppressWarnings("rawtypes")
                List<Future> createFutures = new ArrayList<>();
                for (InstanceToInstanceRelation relation : instanceRelationsToCreate()) {
                    if (!relation.requiresProvisionalInstanceToBeCreated() || relation.hasPreparedProvisionalInstance()) {
                        createFutures.add(InventoryStorage.postInventoryRecord(okapiClient, relation));
                    } else {
                        createFutures.add(failRelationCreation(relation));
                    }
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

    private Future<Void> failRelationCreation(InstanceToInstanceRelation relation) {
        Promise promise = Promise.promise();
        promise.fail(relation.getError().encodePrettily());
        return promise.future();
    }

    public List<InstanceToInstanceRelation> instanceRelationsToCreate() {
        return getInstanceRelationsByTransactionType(InventoryRecord.Transaction.CREATE);
    }

    public List<Instance> provisionalInstancesToCreate() {
        ArrayList<Instance> provisionalInstances = new ArrayList<>();
        for (InstanceToInstanceRelation relation: getInstanceRelationsByTransactionType(InventoryRecord.Transaction.CREATE)) {
            if (relation.requiresProvisionalInstanceToBeCreated()) {
                provisionalInstances.add(relation.provisionalInstance);
            }
        }
        return provisionalInstances;
    }

    @Override
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        JsonArray parents = new JsonArray();
        for (InstanceRelationship relation : getParentRelations()) {
            parents.add(relation.asJson());
        }
        json.put(PARENT_INSTANCES, parents);
        JsonArray children = new JsonArray();
        for (InstanceRelationship relation : getChildRelations()) {
            children.add(relation.asJson());
        }
        json.put(CHILD_INSTANCES, children);
        JsonArray nextTitles = new JsonArray();
        for (InstanceTitleSuccession succeeding : succeedingTitles) {
            nextTitles.add(succeeding.asJson());
        }
        //todo: fix this:
        json.put(SUCCEEDING_TITLES,nextTitles);
        JsonArray previousTitles = new JsonArray();
        for (InstanceTitleSuccession preceding : precedingTitles) {
            previousTitles.add(preceding.asJson());
        }
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

    public List<InstanceRelationship> getRelationships() {
        List<InstanceRelationship> all = new ArrayList<>();
        if (parentRelations != null) all.addAll(parentRelations);
        if (childRelations != null) all.addAll(childRelations);
        return all;
    }

    public List<InstanceTitleSuccession> getTitleSuccessions() {
        List<InstanceTitleSuccession> all = new ArrayList<>();
        if (precedingTitles != null) all.addAll(precedingTitles);
        if (succeedingTitles != null) all.addAll(succeedingTitles);
        return all;
    }

    public List<InstanceToInstanceRelation> getInstanceToInstanceRelations() {
        List<InstanceToInstanceRelation> all = new ArrayList<>();
        if (parentRelations != null) all.addAll(parentRelations);
        if (childRelations != null) all.addAll(childRelations);
        if (precedingTitles != null) all.addAll(precedingTitles);
        if (succeedingTitles != null) all.addAll(succeedingTitles);
        return all;
    }

    public void writeToStats(JsonObject stats) {
//todo: add deletes
        String outcomeStats = "{ \"" + InventoryRecord.Outcome.COMPLETED + "\": 0, \"" + InventoryRecord.Outcome.FAILED + "\": 0, \"" + InventoryRecord.Outcome.SKIPPED + "\": 0, \"" + InventoryRecord.Outcome.PENDING + "\": 0 }";

        String transactionsStats =
                "{ \""+ InventoryRecord.Transaction.CREATE + "\": " + outcomeStats + ", \""
                      + InventoryRecord.Transaction.DELETE + "\": " + outcomeStats + ", \""
                      + "PROVISIONAL_INSTANCE" + "\": " + outcomeStats + " }";

        if (hasInstanceRelationships()) {
            if (!stats.containsKey(InventoryRecord.Entity.INSTANCE_RELATIONSHIP.toString())) {
                stats.put(InventoryRecord.Entity.INSTANCE_RELATIONSHIP.toString(), new JsonObject(transactionsStats));
            }
        }
        if (hasTitleSuccessions()) {
            if (!stats.containsKey(InventoryRecord.Entity.INSTANCE_TITLE_SUCCESSION.toString())) {
                stats.put(InventoryRecord.Entity.INSTANCE_TITLE_SUCCESSION.toString(), new JsonObject(transactionsStats));
            }
        }

        List<InstanceToInstanceRelation> relationsRecords = Stream.of(
                getRelationships(),
                getTitleSuccessions()
        ).flatMap(Collection::stream).collect(Collectors.toList());

        //todo: POJO wrapper around stats JSON?
        for (InstanceToInstanceRelation record : relationsRecords) {
            JsonObject entityStats;
            entityStats = stats.getJsonObject(record.entityType().toString());
            if (!record.getTransaction().equals(InventoryRecord.Transaction.NONE)) {
                JsonObject outcomes = entityStats.getJsonObject(record.getTransaction().toString());
                outcomes.put(record.getOutcome().toString(), outcomes.getInteger(record.getOutcome().toString()) + 1);
                if (record.requiresProvisionalInstanceToBeCreated()) {
                    Instance instance = record.getProvisionalInstance();
                    JsonObject provisionalInstanceStats = entityStats.getJsonObject("PROVISIONAL_INSTANCE");
                    provisionalInstanceStats.put(instance.getOutcome().toString(), provisionalInstanceStats.getInteger(instance.getOutcome().toString()) +1);
                }
            }
        }
    }

    @Override
    public String toString () {
        StringBuilder str = new StringBuilder();
        for (InstanceRelationship rel : getRelationships()) {
            str.append(rel.toString());
        }
        for (InstanceTitleSuccession rel : getTitleSuccessions()) {
            str.append(rel.toString());
        }
        return str.toString();
    }

}
