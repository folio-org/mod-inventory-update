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
import org.folio.inventoryupdate.UpdateMetrics;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.folio.inventoryupdate.entities.InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.HRID;
import static org.folio.inventoryupdate.entities.InstanceToInstanceRelation.InstanceRelationsClass;

/**
 * Instance-to-Instance relations are held in the InventoryRecordSet class but the planning and update logic
 * is performed by this controller.
 */
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
    InventoryRecordSet irs;
    protected final Logger logger = LoggerFactory.getLogger("inventory-update");

    public InstanceRelationsController(InventoryRecordSet inventoryRecordSet) {
        this.irs = inventoryRecordSet;
        if (hasRelationshipRecords(irs.sourceJson)) {
            registerRelationshipJsonRecords(irs.getInstance().getUUID(),irs.sourceJson.getJsonObject(InstanceRelationsController.INSTANCE_RELATIONS));
            logger.debug("InventoryRecordSet initialized with existing instance relationships: " + this.toString());
        }
        if (hasRelationshipIdentifiers(irs.sourceJson)) {
            irs.instanceRelationsJson = irs.sourceJson.getJsonObject(InstanceRelationsController.INSTANCE_RELATIONS);
            logger.debug("InventoryRecordSet initialized with incoming instance relationships JSON (relations to be built.");
        }
    }

    /**
     * Checks if JSON contains relationship records from Inventory storage (got existing record set)
     * @param irsJson  Source JSON for InventoryRecordSet
     * @return
     */
    boolean hasRelationshipRecords(JsonObject irsJson) {
        return (irsJson != null
                && irsJson.containsKey(INSTANCE_RELATIONS)
                && irsJson.getJsonObject(INSTANCE_RELATIONS).containsKey(EXISTING_PARENT_CHILD_RELATIONS));
    }

    /**
     * Checks if JSON contains requests for creating relations (got updating/incoming record set)
     * @param irsJson Source JSON for InventoryRecordSet
     * @return
     */
    private boolean hasRelationshipIdentifiers (JsonObject irsJson) {
        if (irsJson != null) {
            JsonObject relationsJson = irsJson.getJsonObject(INSTANCE_RELATIONS);
            if (relationsJson != null) {
                return (relationsJson.containsKey(PARENT_INSTANCES) ||
                        relationsJson.containsKey(CHILD_INSTANCES) ||
                        relationsJson.containsKey(PRECEDING_TITLES) ||
                        relationsJson.containsKey(SUCCEEDING_TITLES));
            } else {
                return false;
            }
        }
        return false;
    }

    public List<InstanceToInstanceRelation> getInstanceRelationsByTransactionType (InventoryRecord.Transaction transition) {
        List<InstanceToInstanceRelation> records = new ArrayList<>();
        for (InstanceToInstanceRelation record : getInstanceToInstanceRelations())  {
            if (record.getTransaction() == transition && ! record.skipped()) {
                records.add(record);
            }
        }
        return records;
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

    /**
     * Planning
     * @param client
     * @param instanceId
     * @return
     */
    public Future<Void> makeInstanceRelationRecordsFromIdentifiers(OkapiClient client, String instanceId) {
        Promise<Void> promise = Promise.promise();
        if (hasRelationshipIdentifiers(irs.sourceJson)) {
            makeRelationsFromIdentifiers(client, instanceId, irs.instanceRelationsJson.getJsonArray(PARENT_INSTANCES), InstanceRelationsClass.TO_PARENT).onComplete(parents -> {
                    makeRelationsFromIdentifiers(client, instanceId, irs.instanceRelationsJson.getJsonArray(CHILD_INSTANCES), InstanceRelationsClass.TO_CHILD).onComplete (children -> {
                        makeRelationsFromIdentifiers(client, instanceId, irs.instanceRelationsJson.getJsonArray(SUCCEEDING_TITLES), InstanceRelationsClass.TO_PRECEDING).onComplete(succeedingTitles -> {
                            makeRelationsFromIdentifiers(client, instanceId, irs.instanceRelationsJson.getJsonArray(PRECEDING_TITLES), InstanceRelationsClass.TO_SUCCEEDING).onComplete(precedingTitles -> {
                                StringBuilder errorMessages = new StringBuilder();
                                if (parents.succeeded()) {
                                    if (parents.result() != null) {
                                        irs.parentRelations = parents.result();
                                    }
                                } else {
                                    errorMessages.append(LF + "There was a problem looking up or creating Instance IDs to build parent relationships:" + LF + "  " + parents.cause().getMessage());
                                }
                                if (children.succeeded()) {
                                    if (children.result() != null) {
                                        irs.childRelations = children.result();
                                    }
                                } else {
                                    errorMessages.append(LF + "There was a problem looking up or creating Instance IDs to build child relationships:" + LF + "  " + children.cause().getMessage());
                                }
                                if (succeedingTitles.succeeded()) {
                                    if (succeedingTitles.result() != null) {
                                        irs.succeedingTitles = succeedingTitles.result();
                                    }
                                } else {
                                    errorMessages.append(LF + "There was a problem looking up or creating Instance IDs to build succeeding titles links:" + LF + "  " + succeedingTitles.cause().getMessage());
                                }
                                if (precedingTitles.succeeded()) {
                                    if (precedingTitles.result() != null) {
                                        irs.precedingTitles = precedingTitles.result();
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

    /**
     * Planning
     * @param client
     * @param instanceId
     * @param identifiers
     * @param classOfRelations
     * @return
     */
    private static Future<List<InstanceToInstanceRelation>> makeRelationsFromIdentifiers(OkapiClient client, String instanceId, JsonArray identifiers, InstanceRelationsClass classOfRelations) {
        Promise<List<InstanceToInstanceRelation>> promise = Promise.promise();
        if (identifiers != null) {
            @SuppressWarnings("rawtypes")
            List<Future> relationsFutures = new ArrayList<>();
            for (Object o : identifiers) {
                JsonObject relationJson = (JsonObject) o;
                if (relationJson.containsKey(INSTANCE_IDENTIFIER) && relationJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey(HRID)) {
                    relationsFutures.add(makeInstanceRelationWithInstanceIdentifier(client, instanceId, relationJson, HRID, classOfRelations ));
                }
            }
            CompositeFuture.all(relationsFutures).onComplete( relatedInstances -> {
                if (relatedInstances.succeeded()) {
                    if (relatedInstances.result().list() != null) {
                        List<InstanceToInstanceRelation> relations = new ArrayList<>();
                        for (Object o : relatedInstances.result().list()) {
                            InstanceToInstanceRelation relation =  (InstanceToInstanceRelation) o;
                            relations.add(relation);
                        }
                        promise.complete(relations);
                    }
                } else {
                    promise.fail("Failed to construct parent relationships with provided parent Instance identifiers:" + LF + "  " + relatedInstances.cause().getMessage());
                }
            });
        } else {
            promise.complete(null);
        }
        return promise.future();
    }

    /**
     * Planning: Looks up the related Instance from storage and builds an Instance relation object of a given type,
     * @param client
     * @param instanceId
     * @param relatedObject
     * @param identifierKey
     * @param classOfRelations
     * @return
     */
    private static Future<InstanceToInstanceRelation> makeInstanceRelationWithInstanceIdentifier(OkapiClient client,
                                                                                                 String instanceId,
                                                                                                 JsonObject relatedObject,
                                                                                                 String identifierKey,
                                                                                                 InstanceRelationsClass classOfRelations) {
        Promise<InstanceToInstanceRelation> promise = Promise.promise();
        JsonObject instanceIdentifier = relatedObject.getJsonObject(InstanceRelationsController.INSTANCE_IDENTIFIER);
        String hrid = instanceIdentifier.getString(identifierKey);
        InventoryQuery hridQuery = new HridQuery(hrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete(existingInstance -> {
            if (existingInstance.succeeded()) {
                Instance provisionalInstance = null;
                String relateToThisId = null;
                if (existingInstance.result() != null) {
                    JsonObject relatedInstanceJson = existingInstance.result();
                    relateToThisId = relatedInstanceJson.getString(InstanceRelationsController.ID);
                } else {
                    JsonObject provisionalInstanceJson = relatedObject.getJsonObject(InstanceToInstanceRelation.PROVISIONAL_INSTANCE);
                    if (validateProvisionalInstanceProperties(provisionalInstanceJson)) {
                        provisionalInstance = prepareProvisionalInstance(hrid, provisionalInstanceJson);
                        relateToThisId = provisionalInstance.getUUID();
                    }
                }
                InstanceToInstanceRelation relation = null;
                switch (classOfRelations) {
                    case TO_PARENT:
                        relation = InstanceRelationship.makeRelationship(
                                instanceId, instanceId, relateToThisId,
                                relatedObject.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                        break;
                    case TO_CHILD:
                        relation = InstanceRelationship.makeRelationship(
                                instanceId, relateToThisId, instanceId,
                                relatedObject.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
                        break;
                    case TO_PRECEDING:
                        relation = InstanceTitleSuccession.makeInstanceTitleSuccession(
                                relateToThisId, instanceId);
                        break;
                    case TO_SUCCEEDING:
                        relation = InstanceTitleSuccession.makeInstanceTitleSuccession(
                                instanceId, relateToThisId);
                        break;
                }
                relation.setTransition(InventoryRecord.Transaction.CREATE);

                // If the related Instance does not already exist and it cannot be created, register the
                // problem but don't fail it here, in the planning phase, since that would abort the entire update.
                // Rather give the update plan a chance to continue to successful completion and then let the
                // record(s) in question fail eventually during execution of the plan.
                // @see: handleInstanceRelationCreatesIfAny and failRelationCreation
                if (existingInstance.result() == null) {
                    relation.requiresProvisionalInstanceToBeCreated(true);
                    if (provisionalInstance == null) {
                        Instance failedProvisionalInstance = new Instance(new JsonObject());
                        failedProvisionalInstance.fail();
                        failedProvisionalInstance.logError("Missing required properties for creating required provisional instance", 422);
                        relation.setProvisionalInstance(failedProvisionalInstance);
                        relation.logError("Referenced parent Instance not found and required provisional Instance info is missing; cannot create relation to non-existing Instance [" + hrid + "], got:" + InstanceRelationsController.LF + relatedObject.encodePrettily(), 422);
                        relation.fail(); // mark relation failed but don't fail the promise.
                    } else {
                        relation.setProvisionalInstance(provisionalInstance);
                    }
                }
                promise.complete(relation);
            } else {
                promise.fail("Error looking up Instance for creating Instance to Instance relation to it: " + existingInstance.cause().getMessage());
            }
        });
        return promise.future();
    }

    /**
     * Planning: Checks that the required information for creating a provisional Instance is available.
     * @param provisionalInstanceProperties
     * @return
     */
    private static boolean validateProvisionalInstanceProperties (JsonObject provisionalInstanceProperties) {
        if (provisionalInstanceProperties == null) {
            return false;
        } else {
            if (provisionalInstanceProperties.getString(InstanceRelationsController.TITLE) != null
                && provisionalInstanceProperties.getString(InstanceRelationsController.SOURCE) != null
                && provisionalInstanceProperties.getString(InstanceRelationsController.INSTANCE_TYPE_ID) != null) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Planning Create a temporary Instance to create a relationship to.
     * @param hrid human readable ID of the temporary Instance to create
     * @param provisionalInstanceJson other properties of the Instance to create
     * @return Instance POJO
     */
    protected static Instance prepareProvisionalInstance (String hrid, JsonObject provisionalInstanceJson) {
        JsonObject json = new JsonObject(provisionalInstanceJson.toString());
        if (! json.containsKey(HRID)) {
            json.put(HRID, hrid);
        }
        if (! json.containsKey(InstanceRelationsController.ID)) {
            json.put(InstanceRelationsController.ID, UUID.randomUUID().toString());
        }
        return new Instance(json);
    }

    /**
     * Planning: Takes Instance relation records from storage and creates Instance relations objects
     * @param instanceId
     * @param instanceRelations
     */
    public void registerRelationshipJsonRecords(String instanceId, JsonObject instanceRelations) {
        if (instanceRelations.containsKey(EXISTING_PARENT_CHILD_RELATIONS)) {
            JsonArray existingRelations = instanceRelations.getJsonArray(EXISTING_PARENT_CHILD_RELATIONS);
            for (Object o : existingRelations) {
                InstanceRelationship relationship = InstanceRelationship.makeRelationshipFromJsonRecord(instanceId, (JsonObject) o);
                if (relationship.isRelationToChild()) {
                    irs.childRelations.add(relationship);
                } else {
                    irs.parentRelations.add(relationship);
                }
            }
        }
        if (instanceRelations.containsKey(EXISTING_PRECEDING_SUCCEEDING_TITLES)) {
            JsonArray existingTitles = instanceRelations.getJsonArray(EXISTING_PRECEDING_SUCCEEDING_TITLES);
            for (Object o : existingTitles) {
                InstanceTitleSuccession relation = InstanceTitleSuccession.makeInstanceTitleSuccessionFromJsonRecord(instanceId, (JsonObject) o);
                if (relation.isSucceedingTitle()) {
                    irs.succeedingTitles.add(relation);
                } else {
                    irs.precedingTitles.add(relation);
                }
            }
        }
    }

    /**
     * Checks if requested/incoming Instance relations already exists, marks them for creation if they don't
     * and checks if existing Instance relations came in with the request, marks them for deletion if they did not.
     * @param updatingRecordSet
     * @param existingRecordSet
     */
    public static void prepareIncomingInstanceRelations(InventoryRecordSet updatingRecordSet, InventoryRecordSet existingRecordSet) {
        if (updatingRecordSet != null) {
            for (InstanceToInstanceRelation incomingRelation : updatingRecordSet.getInstanceRelationsController().getInstanceToInstanceRelations()) {
                if (existingRecordSet != null) {
                    if (existingRecordSet.getInstanceRelationsController().hasThisRelation(incomingRelation)) {
                        incomingRelation.skip();
                    }
                }
            }
        }
        if (existingRecordSet != null) {
            for (InstanceToInstanceRelation existingRelation : existingRecordSet.getInstanceRelationsController().getInstanceToInstanceRelations()) {
                if (!updatingRecordSet.getInstanceRelationsController().hasThisRelation(existingRelation)) {
                    existingRelation.setTransition(InventoryRecord.Transaction.DELETE);
                } else {
                    existingRelation.setTransition(InventoryRecord.Transaction.NONE);
                }
            }
        }
    }

    /**
     * Executing plan
     * @param okapiClient
     * @return
     */
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

    /**
     * Executing plan: Force Instance relation creation to fail; used when the planning logic detected a problem creating a
     * provisional record to relate to.
     * @param relation  the problematic Instance relation
     * @return
     */
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
        json.put(PARENT_INSTANCES, new JsonArray());
        json.put(CHILD_INSTANCES, new JsonArray());
        json.put(SUCCEEDING_TITLES, new JsonArray());
        json.put(PRECEDING_TITLES, new JsonArray());

        for (InstanceToInstanceRelation relation : getInstanceToInstanceRelations()) {
            logger.info("Relation: " + relation);
            logger.info("type of relation: " + relation.instanceRelationClass);
            switch (relation.instanceRelationClass) {
                case TO_PARENT:
                    json.getJsonArray(PARENT_INSTANCES).add(relation.asJson());
                    break;
                case TO_CHILD:
                    json.getJsonArray(CHILD_INSTANCES).add(relation.asJson());
                    break;
                case TO_PRECEDING:
                    json.getJsonArray(PRECEDING_TITLES).add(relation.asJson());
                    break;
                case TO_SUCCEEDING:
                    json.getJsonArray(SUCCEEDING_TITLES).add(relation.asJson());
                    break;
            }
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

    public List<InstanceToInstanceRelation> getRelationships() {
        return Stream.of(
                irs.parentRelations,
                irs.childRelations
        ).flatMap(Collection::stream).collect(Collectors.toList());
    }

    public List<InstanceToInstanceRelation> getTitleSuccessions() {
        return Stream.of(
                irs.precedingTitles,
                irs.succeedingTitles
        ).flatMap(Collection::stream).collect(Collectors.toList());
    }

    public List<InstanceToInstanceRelation> getInstanceToInstanceRelations() {
        return Stream.of(
                irs.parentRelations,
                irs.childRelations,
                irs.precedingTitles,
                irs.succeedingTitles
        ).flatMap(Collection::stream).collect(Collectors.toList());
    }

    public void writeToStats(UpdateMetrics metrics) {
        for (InstanceToInstanceRelation record : getInstanceToInstanceRelations()) {
            if (!record.getTransaction().equals(InventoryRecord.Transaction.NONE)) {
                metrics.entity(record.entityType).transaction(record.transaction).outcomes.increment(record.getOutcome());
                if (record.requiresProvisionalInstanceToBeCreated()) {
                    Instance provisionalInstance = record.getProvisionalInstance();
                    ((UpdateMetrics.InstanceRelationsMetrics) metrics.entity(record.entityType)).provisionalInstanceMetrics.increment(provisionalInstance.getOutcome());
                }
            }
        }

    }

    @Override
    public String toString () {
        StringBuilder str = new StringBuilder();
        for (InstanceToInstanceRelation rel : getInstanceToInstanceRelations()) {
            str.append(rel.toString());
        }
        return str.toString();
    }

}
