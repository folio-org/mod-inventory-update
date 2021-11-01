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
import org.folio.inventoryupdate.QueryByUUID;
import org.folio.inventoryupdate.UpdateMetrics;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.folio.inventoryupdate.entities.InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.HRID_IDENTIFIER_KEY;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.UUID_IDENTIFIER_KEY;
import static org.folio.inventoryupdate.entities.InstanceToInstanceRelation.InstanceRelationsClass;

/**
 * Instance-to-Instance relations are held in the InventoryRecordSet class but the planning and update logic
 * is performed by this controller.
 */
public class InstanceRelationsManager extends JsonRepresentation {

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

    boolean cameInWithEmptyParentIdentifierList = false;
    boolean cameInWithEmptyChildIdentifierList = false;
    boolean cameInWithEmptySucceedingTitlesList = false;
    boolean cameInWithEmptyPrecedingTitlesList = false;

    public static final String LF = System.lineSeparator();
    InventoryRecordSet irs;
    protected static final Logger logger = LoggerFactory.getLogger("inventory-update");

    public InstanceRelationsManager( InventoryRecordSet inventoryRecordSet) {
        this.irs = inventoryRecordSet;
        if (hasRelationshipRecords(irs.sourceJson)) {
            registerRelationshipJsonRecords(irs.getInstance().getUUID(),irs.sourceJson.getJsonObject(
                    InstanceRelationsManager.INSTANCE_RELATIONS));
            logger.debug("InventoryRecordSet initialized with existing instance relationships: " + this );
        }
        if (hasRelationshipIdentifiers(irs.sourceJson)) {
            irs.instanceRelationsJson = irs.sourceJson.getJsonObject( InstanceRelationsManager.INSTANCE_RELATIONS);
            cameInWithEmptyParentIdentifierList = irs.instanceRelationsJson.containsKey(PARENT_INSTANCES) && irs.instanceRelationsJson.getJsonArray(PARENT_INSTANCES).isEmpty();
            cameInWithEmptyChildIdentifierList = irs.instanceRelationsJson.containsKey(CHILD_INSTANCES) && irs.instanceRelationsJson.getJsonArray(CHILD_INSTANCES).isEmpty();
            cameInWithEmptyPrecedingTitlesList = irs.instanceRelationsJson.containsKey(PRECEDING_TITLES) && irs.instanceRelationsJson.getJsonArray(PRECEDING_TITLES).isEmpty();
            cameInWithEmptySucceedingTitlesList = irs.instanceRelationsJson.containsKey(SUCCEEDING_TITLES) && irs.instanceRelationsJson.getJsonArray(SUCCEEDING_TITLES).isEmpty();
            logger.debug("InventoryRecordSet initialized with incoming instance relationships JSON (relations to be built.");
        }
    }

    /**
     * Checks if JSON contains relationship records from Inventory storage (got existing record set)
     * @param irsJson  Source JSON for InventoryRecordSet
     * @return true if there are stored relations for this Instance
     */
    boolean hasRelationshipRecords(JsonObject irsJson) {
        return (irsJson.containsKey(INSTANCE_RELATIONS)
                && irsJson.getJsonObject(INSTANCE_RELATIONS).containsKey(EXISTING_PARENT_CHILD_RELATIONS));
    }

    /**
     * Checks if JSON contains requests for creating relations (got updating/incoming record set)
     * @param irsJson Source JSON for InventoryRecordSet
     * @return true if identifiers for building relationship objects are provided
     */
    private boolean hasRelationshipIdentifiers (JsonObject irsJson) {
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

    /**
     * A relation is considered omitted if the list it would be in exists but the relation is not in the list.
     *
     * Can be used to signal that relations of a given type should be deleted from the Instance by providing an
     * empty list of that type. When no such list is provided, on the other hand, then existing relations of that type
     * should be retained.
     *
     * @param relation Relationship to check
     * @return true if the relation is not in a provides list of relations, false if its present or if no list was provided
     */
    public boolean isThisRelationOmitted(InstanceToInstanceRelation relation) {
        switch (relation.instanceRelationClass) {
            case TO_PARENT:
                return (cameInWithEmptyParentIdentifierList || isThisRelationOmitted(irs.parentRelations, relation));
            case TO_CHILD:
                return (cameInWithEmptyChildIdentifierList || isThisRelationOmitted(irs.childRelations, relation));
            case TO_PRECEDING:
                return (cameInWithEmptyPrecedingTitlesList || isThisRelationOmitted(irs.precedingTitles, relation));
            case TO_SUCCEEDING:
                return (cameInWithEmptySucceedingTitlesList || isThisRelationOmitted(irs.succeedingTitles, relation));
        }
        return false;
    }

    /**
     * A relation is considered omitted from the list if the list exists (is not null) but the relation is not in it.
     * @param list list of relations to check the relation against
     * @param relation the relation to check
     * @return true if a list was provided and the relation is not in the list
     */
    private boolean isThisRelationOmitted(List<InstanceToInstanceRelation> list, InstanceToInstanceRelation relation) {
        return ( list != null && !list.contains( relation ) );
    }

    /**
     * Planning
     * @param client The Okapi client to use for look-ups
     * @param instanceId The instance to prepare relationship objects for
     */
    public Future<Void> makeInstanceRelationRecordsFromIdentifiers(OkapiClient client, String instanceId) {
        Promise<Void> promise = Promise.promise();
        if (hasRelationshipIdentifiers(irs.sourceJson)) {
            makeRelationsFromIdentifiers(client, instanceId, irs.instanceRelationsJson.getJsonArray(PARENT_INSTANCES), InstanceRelationsClass.TO_PARENT).onComplete(parents -> {
                    makeRelationsFromIdentifiers(client, instanceId, irs.instanceRelationsJson.getJsonArray(CHILD_INSTANCES), InstanceRelationsClass.TO_CHILD).onComplete (children -> {
                        makeRelationsFromIdentifiers(client, instanceId, irs.instanceRelationsJson.getJsonArray(SUCCEEDING_TITLES), InstanceRelationsClass.TO_SUCCEEDING).onComplete(succeedingTitles -> {
                            makeRelationsFromIdentifiers(client, instanceId, irs.instanceRelationsJson.getJsonArray(PRECEDING_TITLES), InstanceRelationsClass.TO_PRECEDING).onComplete(precedingTitles -> {
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

    private static InventoryQuery makeInstanceQueryUsingIdentifierObject( JsonObject instanceIdentifierJson) {
        InventoryQuery queryByUniqueId = null;
        if (instanceIdentifierJson.containsKey( HRID_IDENTIFIER_KEY )) {
            queryByUniqueId = new HridQuery( instanceIdentifierJson.getString( HRID_IDENTIFIER_KEY ) );
        } else if ( instanceIdentifierJson.containsKey( UUID_IDENTIFIER_KEY )) {
            try {
                UUID uuid = UUID.fromString( instanceIdentifierJson.getString(UUID_IDENTIFIER_KEY) );
                queryByUniqueId = new QueryByUUID(uuid);
            } catch (IllegalArgumentException iae) {
                logger.error("Could not parse provided 'uuid' property as a UUID for an Instance query, query will probably not find an Instance record:  " + instanceIdentifierJson.getString("uuid"));
                queryByUniqueId = new QueryByUUID(instanceIdentifierJson.getString( UUID_IDENTIFIER_KEY ));
            }
        }
        return queryByUniqueId;
    }
    /**
     * Planning. Use incoming identifiers (HRIDs or UUIDs) for creating relationship objects.
     * @param client Okapi client used for look-ups
     * @param instanceId The Instance to prepare relationship objects for
     * @param identifiers Provided unique Instance identifiers (HRID or UUID) to prepare relationship objects from
     * @param classOfRelations Type of relation (parent, child, succeeding, preceding)
     * @return A future list of prepared Instance relationship objects.
     */
    private static Future<List<InstanceToInstanceRelation>> makeRelationsFromIdentifiers(OkapiClient client, String instanceId, JsonArray identifiers, InstanceRelationsClass classOfRelations) {
        Promise<List<InstanceToInstanceRelation>> promise = Promise.promise();
        if (identifiers != null) {
            @SuppressWarnings("rawtypes")
            List<Future> relationsFutures = new ArrayList<>();
            for (Object o : identifiers) {
                JsonObject relationJson = (JsonObject) o;
                JsonObject instanceIdentifier = relationJson.getJsonObject( INSTANCE_IDENTIFIER );
                if (instanceIdentifier != null) {
                    InventoryQuery queryByUniqueId = makeInstanceQueryUsingIdentifierObject(instanceIdentifier);
                    relationsFutures.add(makeInstanceRelationWithInstanceIdentifier(
                                client, instanceId, relationJson, queryByUniqueId, classOfRelations ) );
                }
            }
            if (relationsFutures.isEmpty()) {
                promise.complete(null);
            } else
            {
                CompositeFuture.all( relationsFutures ).onComplete( relatedInstances -> {
                    if ( relatedInstances.succeeded() )
                    {
                        if ( relatedInstances.result().list() != null )
                        {
                            List<InstanceToInstanceRelation> relations = new ArrayList<>();
                            for ( Object o : relatedInstances.result().list() )
                            {
                                InstanceToInstanceRelation relation = (InstanceToInstanceRelation) o;
                                relations.add( relation );
                            }
                            promise.complete( relations );
                        }
                    }
                    else
                    {
                        promise.fail(
                                "Failed to construct parent relationships with provided parent Instance identifiers:" + LF + "  " + relatedInstances.cause().getMessage() );
                    }
                } );
            }
        } else {
            promise.complete(null);
        }
        return promise.future();
    }

    /**
     * Planning: Looks up the related Instance from storage to prepares an Instance relation object of a given type.
     * If no existing Instance was found and if the provided identifier was an HRID and if the request also contains the
     * minimum required information for creating an Instance, a 'provisional' Instance is prepared to fulfill the relationship.
     * @param client  The Okapi client for the look-up
     * @param instanceId The current Instance to build a relation from
     * @param relatedObject The provided information about the Instance to build a relation to.
     * @param classOfRelations Type of relation (to parent, child, preceding or succeeding)
     * @return The prepared Instance to Instance relationship object
     */
    private static Future<InstanceToInstanceRelation> makeInstanceRelationWithInstanceIdentifier(OkapiClient client,
                                                                                                 String instanceId,
                                                                                                 JsonObject relatedObject,
                                                                                                 InventoryQuery queryById,
                                                                                                 InstanceRelationsClass classOfRelations) {
        Promise<InstanceToInstanceRelation> promise = Promise.promise();
        boolean gotHrid = queryById instanceof HridQuery;
        InventoryStorage.lookupInstance(client, queryById).onComplete(existingInstance -> {
            if (existingInstance.succeeded()) {
                Instance provisionalInstance = null;
                String relateToThisId = null;
                if (existingInstance.result() != null) {
                    JsonObject relatedInstanceJson = existingInstance.result();
                    relateToThisId = relatedInstanceJson.getString( InstanceRelationsManager.ID);
                } else {
                    // The instance to relate to, does not exist. if an HRID was provided we might be able to create a provisional Instance
                    if (gotHrid) {
                        JsonObject provisionalInstanceJson = relatedObject.getJsonObject(
                                InstanceToInstanceRelation.PROVISIONAL_INSTANCE );
                        if ( validateProvisionalInstanceProperties( provisionalInstanceJson ) )
                        {
                            provisionalInstance = prepareProvisionalInstance( ((HridQuery) queryById).hrid, provisionalInstanceJson );
                            relateToThisId = provisionalInstance.getUUID();
                        }
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
                        logger.debug("Creating preceding");
                        relation = InstanceTitleSuccession.makeInstanceTitleSuccession(
                                instanceId, relateToThisId, instanceId);
                        logger.debug("Relation class: " + relation.instanceRelationClass);
                        break;
                    case TO_SUCCEEDING:
                        logger.debug("Creating succeeding");
                        relation = InstanceTitleSuccession.makeInstanceTitleSuccession(
                                instanceId, instanceId, relateToThisId);
                        logger.debug("Relation class: " + relation.instanceRelationClass);
                        break;
                }
                relation.setTransition(InventoryRecord.Transaction.CREATE);

                // If the related Instance does not already exist, and it cannot be created, register the
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
                        relation.logError("Referenced parent Instance not found and required provisional Instance " +
                                "info for potentially creating one is missing; cannot create relation to non-existing Instance, HRID: [" +
                                (gotHrid ? ((HridQuery) queryById).hrid : "no HRID provided") +"], got:" + InstanceRelationsManager.LF + relatedObject.encodePrettily(), 422);
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
     * @param provisionalInstanceProperties Set of properties for creating a new Instance
     * @return true if the minimum requirements are met
     */
    private static boolean validateProvisionalInstanceProperties (JsonObject provisionalInstanceProperties) {
        if (provisionalInstanceProperties == null) {
            return false;
        } else {
            if (provisionalInstanceProperties.getString( InstanceRelationsManager.TITLE) != null
                && provisionalInstanceProperties.getString( InstanceRelationsManager.SOURCE) != null
                && provisionalInstanceProperties.getString( InstanceRelationsManager.INSTANCE_TYPE_ID) != null) {
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
        if (! json.containsKey( HRID_IDENTIFIER_KEY )) {
            json.put( HRID_IDENTIFIER_KEY, hrid);
        }
        if (! json.containsKey( InstanceRelationsManager.ID)) {
            json.put( InstanceRelationsManager.ID, UUID.randomUUID().toString());
        }
        return new Instance(json);
    }

    /**
     * Planning: Takes Instance relation records from storage and creates Instance relations objects
     * @param instanceId The ID of the Instance to create relationship objects for
     * @param instanceRelations a set of relations from storage
     */
    public void registerRelationshipJsonRecords(String instanceId, JsonObject instanceRelations) {
        if (instanceRelations.containsKey(EXISTING_PARENT_CHILD_RELATIONS)) {
            JsonArray existingRelations = instanceRelations.getJsonArray(EXISTING_PARENT_CHILD_RELATIONS);
            for (Object o : existingRelations) {
                InstanceRelationship relationship = InstanceRelationship.makeRelationshipFromJsonRecord(instanceId, (JsonObject) o);
                if (relationship.isRelationToChild()) {
                    if (irs.childRelations == null) irs.childRelations = new ArrayList<>();
                    irs.childRelations.add(relationship);
                } else {
                    if (irs.parentRelations == null) irs.parentRelations = new ArrayList<>();
                    irs.parentRelations.add(relationship);
                }
            }
        }
        if (instanceRelations.containsKey(EXISTING_PRECEDING_SUCCEEDING_TITLES)) {
            JsonArray existingTitles = instanceRelations.getJsonArray(EXISTING_PRECEDING_SUCCEEDING_TITLES);
            for (Object o : existingTitles) {
                InstanceTitleSuccession relation = InstanceTitleSuccession.makeInstanceTitleSuccessionFromJsonRecord(instanceId, (JsonObject) o);
                if (relation.isSucceedingTitle()) {
                    if (irs.succeedingTitles == null) irs.succeedingTitles = new ArrayList<>();
                    irs.succeedingTitles.add(relation);
                } else {
                    if (irs.precedingTitles == null) irs.precedingTitles = new ArrayList<>();
                    irs.precedingTitles.add(relation);
                }
            }
        }
    }

    /**
     * Checks if requested/incoming Instance relations already exists, marks them for creation if they don't
     * and checks if existing Instance relations came in with the request, marks them for deletion if they did not.
     * @param updatingRecordSet The Inventory record set that is being prepared for updating Inventory
     * @param existingRecordSet The existing record set in Inventory
     */
    public void prepareIncomingInstanceRelations(InventoryRecordSet updatingRecordSet, InventoryRecordSet existingRecordSet) {
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
                if (updatingRecordSet.getInstanceRelationsController().isThisRelationOmitted(existingRelation)) {
                    existingRelation.setTransition(InventoryRecord.Transaction.DELETE);
                } else {
                    existingRelation.setTransition(InventoryRecord.Transaction.NONE);
                }
            }
        }
    }

    /**
     * Executing plan
     * @param okapiClient The client to use for updating Inventory storage
     */
    public Future<Void> handleInstanceRelationCreatesIfAny (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();

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
                        promise.complete();
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
     */
    private Future<Void> failRelationCreation(InstanceToInstanceRelation relation) {
        Promise<Void> promise = Promise.promise();
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
        for (InstanceToInstanceRelation relation : getInstanceToInstanceRelations()) {
            JsonObject relationJson = new JsonObject(relation.asJsonString());
            if (relation.hasPreparedProvisionalInstance()) {
                relationJson.put("CREATE_PROVISIONAL_INSTANCE", relation.getProvisionalInstance().asJson());
            }
            switch (relation.instanceRelationClass) {
                case TO_PARENT:
                    if (!json.containsKey(PARENT_INSTANCES)) json.put(PARENT_INSTANCES, new JsonArray());
                    json.getJsonArray(PARENT_INSTANCES).add(relationJson);
                    break;
                case TO_CHILD:
                    if (!json.containsKey(CHILD_INSTANCES)) json.put(CHILD_INSTANCES, new JsonArray());
                    json.getJsonArray(CHILD_INSTANCES).add(relationJson);
                    break;
                case TO_PRECEDING:
                    if (!json.containsKey(PRECEDING_TITLES)) json.put(PRECEDING_TITLES, new JsonArray());
                    json.getJsonArray(PRECEDING_TITLES).add(relationJson);
                    break;
                case TO_SUCCEEDING:
                    if (!json.containsKey(SUCCEEDING_TITLES)) json.put(SUCCEEDING_TITLES, new JsonArray());
                    json.getJsonArray(SUCCEEDING_TITLES).add(relationJson);
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

    public List<InstanceToInstanceRelation> getInstanceToInstanceRelations() {
        return Stream.of(
                irs.parentRelations == null ? new ArrayList<InstanceToInstanceRelation>() : irs.parentRelations,
                irs.childRelations == null ? new ArrayList<InstanceToInstanceRelation>() : irs.childRelations,
                irs.precedingTitles == null ? new ArrayList<InstanceToInstanceRelation>() : irs.precedingTitles,
                irs.succeedingTitles == null ? new ArrayList<InstanceToInstanceRelation>() : irs.succeedingTitles
        ).flatMap(Collection::stream).collect(Collectors.toList());
    }

    public void writeToStats(UpdateMetrics metrics) {
        if (! getInstanceToInstanceRelations().isEmpty()) {
            for ( InstanceToInstanceRelation record : getInstanceToInstanceRelations() ) {
                logger.info("Record: " + record.jsonRecord.encode());
                logger.info("Transaction: " + record.getTransaction());
                logger.info("Entity type: " + record.entityType);
                if ( !record.getTransaction().equals( InventoryRecord.Transaction.NONE ) ) {
                    metrics.entity( record.entityType ).transaction( record.transaction ).outcomes.increment(
                            record.getOutcome() );
                    if ( record.requiresProvisionalInstanceToBeCreated() ) {
                        Instance provisionalInstance = record.getProvisionalInstance();
                        ( (UpdateMetrics.InstanceRelationsMetrics) metrics.entity( record.entityType ) ).provisionalInstanceMetrics.increment(
                                provisionalInstance.getOutcome() );
                    }
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
