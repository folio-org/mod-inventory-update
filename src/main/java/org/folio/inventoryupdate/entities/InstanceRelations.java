package org.folio.inventoryupdate.entities;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.ErrorReport;
import org.folio.inventoryupdate.QueryByHrid;
import org.folio.inventoryupdate.InventoryQuery;
import org.folio.inventoryupdate.InventoryStorage;
import org.folio.inventoryupdate.QueryByUUID;
import org.folio.inventoryupdate.UpdateMetrics;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.List;

/**
 * Instance-to-Instance relations are held in the InventoryRecordSet class but the planning and update logic
 * is performed by this controller.
 */
public class InstanceRelations extends JsonRepresentation {

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
    protected static final Logger logger = LoggerFactory.getLogger("inventory-update");

    public InstanceRelations(InventoryRecordSet inventoryRecordSet) {
        this.irs = inventoryRecordSet;
    }


    /**
     * Planning
     * @param client The Okapi client to use for look-ups
     * @param instanceId The instance to prepare relationship objects for
     */
    public Future<Void> makeInstanceRelationRecordsFromIdentifiers(OkapiClient client, String instanceId) {
        Promise<Void> promise = Promise.promise();
        if (irs.instanceReferences != null && irs.instanceReferences.hasRelationsArrays()) {
            for (InstanceReference reference : irs.instanceReferences.references) {
                reference.setFromInstanceId(instanceId);
            }
        }
        if (irs.instanceReferences != null && irs.instanceReferences.hasRelationsArrays()) {
            makeRelationsFromIdentifiers(client, irs.instanceReferences.getReferencesToParentInstances()).onComplete(parents -> {
                    makeRelationsFromIdentifiers(client, irs.instanceReferences.getReferencesToChildInstances()).onComplete (children -> {
                        makeRelationsFromIdentifiers(client, irs.instanceReferences.getReferencesToSucceedingTitles()).onComplete(succeedingTitles -> {
                            makeRelationsFromIdentifiers(client, irs.instanceReferences.getReferencesToPrecedingTitles()).onComplete(precedingTitles -> {
                                StringBuilder errorMessages = new StringBuilder();
                                if (parents.succeeded()) {
                                    if (parents.result() != null) {
                                        irs.parentRelations = parents.result();
                                    }
                                } else {
                                    errorMessages.append(LF)
                                            .append("There was a problem looking up or creating Instance IDs to build parent relationships:")
                                            .append(LF)
                                            .append("  ")
                                            .append(parents.cause().getMessage());
                                }
                                if (children.succeeded()) {
                                    if (children.result() != null) {
                                        irs.childRelations = children.result();
                                    }
                                } else {
                                    errorMessages.append(LF)
                                            .append("There was a problem looking up or creating Instance IDs to build child relationships:")
                                            .append(LF)
                                            .append("  ")
                                            .append(children.cause().getMessage());
                                }
                                if (succeedingTitles.succeeded()) {
                                    if (succeedingTitles.result() != null) {
                                        irs.succeedingTitles = succeedingTitles.result();
                                    }
                                } else {
                                    errorMessages.append(LF)
                                            .append("There was a problem looking up or creating Instance IDs to build succeeding titles links:")
                                            .append(LF)
                                            .append("  ")
                                            .append(succeedingTitles.cause().getMessage());
                                }
                                if (precedingTitles.succeeded()) {
                                    if (precedingTitles.result() != null) {
                                        irs.precedingTitles = precedingTitles.result();
                                    }
                                } else {
                                    errorMessages.append(LF)
                                            .append("There was a problem looking up or creating Instance IDs to build preceding titles links:")
                                            .append(LF)
                                            .append("  ")
                                            .append(precedingTitles.cause().getMessage());
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
     * Planning. Use incoming identifiers (HRIDs or UUIDs) for creating relationship objects.
     * @param client Okapi client used for look-ups
     * @return A future list of prepared Instance relationship objects.
     */
    private static Future<List<InstanceToInstanceRelation>> makeRelationsFromIdentifiers(
            OkapiClient client, List<InstanceReference> references) {
        Promise<List<InstanceToInstanceRelation>> promise = Promise.promise();
        if (references != null) {
            @SuppressWarnings("rawtypes")
            List<Future> relationsFutures = new ArrayList<>();
            for (InstanceReference reference : references) {
                if (reference.hasReferenceHrid() || reference.hasReferenceUuid()) {
                    relationsFutures.add(makeInstanceRelationWithInstanceIdentifier(client, reference));
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
                                relations.add((InstanceToInstanceRelation) o);
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
     * @return The prepared Instance to Instance relationship object
     */
    private static Future<InstanceToInstanceRelation> makeInstanceRelationWithInstanceIdentifier(
            OkapiClient client,
            InstanceReference reference) {
        Promise<InstanceToInstanceRelation> promise = Promise.promise();
        InventoryQuery query;
        if (reference.hasReferenceHrid()) {
            query = new QueryByHrid(reference.getReferenceHrid());
        } else {
            query = new QueryByUUID(reference.getReferenceUuid());
        }
        InventoryStorage.lookupInstance(client, query).onComplete(existingInstance -> {
            if (existingInstance.succeeded()) {
                if (existingInstance.result() != null) {
                    JsonObject relatedInstanceJson = existingInstance.result();
                    reference.setReferencedInstanceId(relatedInstanceJson.getString("id"));
                }
                InstanceToInstanceRelation relation = reference.getInstanceToInstanceRelation();
                relation.setTransition(InventoryRecord.Transaction.CREATE);
                promise.complete(relation);
            } else {
                promise.fail(
                        "Error looking up Instance for creating Instance to Instance relation to it: " + existingInstance.cause().getMessage());
            }
        });
        return promise.future();
    }


    /**
     * Checks if requested/incoming Instance relations already exists, marks them for creation if they don't
     * and checks if existing Instance relations came in with the request, marks them for deletion if they did not.
     * @param updatingRecordSet The Inventory record set that is being prepared for updating Inventory
     * @param existingRecordSet The existing record set in Inventory
     */
    public static void prepareInstanceRelationTransactions(InventoryRecordSet updatingRecordSet, InventoryRecordSet existingRecordSet) {
        if (updatingRecordSet != null) {
            for (InstanceToInstanceRelation incomingRelation : updatingRecordSet.getInstanceToInstanceRelations()) {
                if (existingRecordSet != null) {
                    if (existingRecordSet.hasThisRelation(incomingRelation)) {
                        incomingRelation.skip();
                    }
                }
            }
        }
        if (existingRecordSet != null) {
            for (InstanceToInstanceRelation existingRelation : existingRecordSet.getInstanceToInstanceRelations()) {
                if (updatingRecordSet.isThisRelationOmitted(existingRelation)) {
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
                        promise.fail(ErrorReport.makeErrorReportFromJsonString(
                                allRelationsCreated.cause().getMessage())
                                .addDetail("context", "Creating instance-to-instance relations")
                                .asJsonString());
                    }
                });
            } else {
                promise.fail(ErrorReport.makeErrorReportFromJsonString(
                        allProvisionalInstancesCreated.cause().getMessage())
                        .addDetail("context","Creating provisional instances")
                        .asJsonString());
            }
        });
        return promise.future();
    }

    /**
     * Executing plan: Force Instance relation creation to fail; used when the planning logic detected a problem creating a
     * provisional record to relate to.
     * @param relation  the problematic Instance relation
     */
    public static Future<Void> failRelationCreation(InstanceToInstanceRelation relation) {
        Promise<Void> promise = Promise.promise();
        promise.fail(relation.getErrorAsJson().encodePrettily());
        return promise.future();
    }

    public static Future<Void> failProvisionalInstanceCreation (Instance provisionalInstance) {
        Promise<Void> promise = Promise.promise();
        promise.fail(provisionalInstance.getErrorAsJson().encodePrettily());
        return promise.future();
    }

    public List<InstanceToInstanceRelation> instanceRelationsToCreate() {
        return irs.getInstanceRelationsByTransactionType(InventoryRecord.Transaction.CREATE);
    }

    public List<Instance> provisionalInstancesToCreate() {
        ArrayList<Instance> provisionalInstances = new ArrayList<>();
        for (InstanceToInstanceRelation relation: irs.getInstanceRelationsByTransactionType(InventoryRecord.Transaction.CREATE)) {
            if (relation.requiresProvisionalInstanceToBeCreated()) {
                provisionalInstances.add(relation.provisionalInstance);
            }
        }
        return provisionalInstances;
    }

    @Override
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        for (InstanceToInstanceRelation relation : irs.getInstanceToInstanceRelations()) {
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


    public void writeToStats(UpdateMetrics metrics) {
        if (! irs.getInstanceToInstanceRelations().isEmpty()) {
            for ( InstanceToInstanceRelation record : irs.getInstanceToInstanceRelations() ) {
                logger.debug("Record: " + record.jsonRecord.encode());
                logger.debug("Transaction: " + record.getTransaction());
                logger.debug("Entity type: " + record.entityType);
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
        for (InstanceToInstanceRelation rel : irs.getInstanceToInstanceRelations()) {
            str.append(rel.toString());
        }
        return str.toString();
    }

}
