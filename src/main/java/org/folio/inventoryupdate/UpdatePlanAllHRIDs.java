package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.*;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;

import static org.folio.inventoryupdate.ErrorReport.UNPROCESSABLE_ENTITY;
import static org.folio.inventoryupdate.entities.InstanceRelations.failProvisionalInstanceCreation;
import static org.folio.inventoryupdate.entities.InstanceRelations.failRelationCreation;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.INSTANCE;

public class UpdatePlanAllHRIDs extends UpdatePlan {


    private static final String LF = System.lineSeparator();

    /**
     * Constructs deletion plane
     * @param existingInstanceQuery The query by which to find the instance to delete
     */
    private UpdatePlanAllHRIDs (InventoryQuery existingInstanceQuery) {
        super(existingInstanceQuery);
    }

    /**
     * Constructs upsert plan
     * @param repository Cache of Inventory records to build plan from.
     */
    private UpdatePlanAllHRIDs (RepositoryByHrids repository) {
        super(repository);
    }

    public static UpdatePlanAllHRIDs getUpsertPlan() {
        return new UpdatePlanAllHRIDs( new RepositoryByHrids());
    }

    public static UpdatePlanAllHRIDs getDeletionPlan(InventoryQuery existingInstanceQuery) {
        return  new UpdatePlanAllHRIDs( existingInstanceQuery);
    }

    public UpdatePlanAllHRIDs setIncomingRecordSets(JsonArray inventoryRecordSets) {
        repository.setIncomingRecordSets(inventoryRecordSets);
        return this;
    }

    public Future<Void> buildRepositoryFromStorage (RoutingContext routingContext) {
        return repository.buildRepositoryFromStorage(routingContext);
    }

    @Override
    public RequestValidation validateIncomingRecordSet(JsonObject inventoryRecordSet) {
        RequestValidation validationErrors = new RequestValidation();
        if (isDeletion) return validationErrors;
        String instanceHRID = inventoryRecordSet.getJsonObject("instance").getString("hrid");
        String instanceTitle = inventoryRecordSet.getJsonObject("instance").getString("title");
        if (instanceHRID == null || instanceHRID.isEmpty()) {
            logger.error("Missing or empty HRID. Instances must have a HRID to be processed by this API. Title: " + instanceTitle);
            validationErrors.registerError(
                    new ErrorReport(
                            ErrorReport.ErrorCategory.VALIDATION,
                            UNPROCESSABLE_ENTITY,
                            "HRID is missing or empty.")
                            .setEntityType(InventoryRecord.Entity.INSTANCE)
                            .setEntity(inventoryRecordSet.getJsonObject(INSTANCE)));
        }
        if (inventoryRecordSet.containsKey("holdingsRecords")) {
            inventoryRecordSet.getJsonArray("holdingsRecords")
                    .stream()
                    .map( rec -> (JsonObject) rec)
                    .forEach( record -> {
                        if (!record.containsKey("hrid")) {
                            logger.error("Holdings Records must have a HRID to be processed by this API. Received: " + record.encodePrettily());
                            validationErrors.registerError(
                                new ErrorReport(
                                    ErrorReport.ErrorCategory.VALIDATION,
                                    UNPROCESSABLE_ENTITY,
                                    "Holdings must have a HRID to be processed by this API. Title: " + instanceTitle)
                                    .setShortMessage("Missing HRID in holdings record")
                                    .setEntityType(InventoryRecord.Entity.HOLDINGS_RECORD)
                                    .setEntity(record)
                                    .setDetails(inventoryRecordSet));
                        }
                        if (record.containsKey("items")) {
                            record.getJsonArray("items")
                                    .stream()
                                    .map(item -> (JsonObject) item)
                                    .forEach(item -> {
                                        if (!item.containsKey("hrid")) {
                                            logger.error("Items must have a HRID to be processed by this API. Received: " + item.encodePrettily());
                                            validationErrors.registerError(
                                                 new ErrorReport(
                                                    ErrorReport.ErrorCategory.VALIDATION,
                                                    UNPROCESSABLE_ENTITY,
                                                    "Items must have a HRID to be processed by this API. Title: " + instanceTitle)
                                                    .setShortMessage("Missing HRID in Item")
                                                    .setEntity(item)
                                                    .setDetails(inventoryRecordSet));
                                        }
                                    });
                        }
                    });
        }
        return validationErrors;
    }

    public static void checkForUniqueHRIDsInBatch(RequestValidation validation, JsonArray inventoryRecordSets) {
        Set<String> instanceHrids = new HashSet<>();
        Set<String> holdingsHrids = new HashSet<>();
        Set<String> itemHrids = new HashSet<>();

        for (Object recordSetObject : inventoryRecordSets) {
            JsonObject recordSet = (JsonObject) recordSetObject;
            String instanceHrid = recordSet.getJsonObject("instance").getString("hrid");
            if (instanceHrid != null) {
                if (instanceHrids.contains(instanceHrid)) {
                    validation.registerError(
                            new ErrorReport(
                                    ErrorReport.ErrorCategory.VALIDATION,
                                    UNPROCESSABLE_ENTITY,
                                    "Instance HRID " + instanceHrid + " occurs more that once in this batch.")
                                    .setShortMessage("Instance HRID is repeated in this batch")
                                    .setEntityType(InventoryRecord.Entity.INSTANCE)
                                    .setEntity(recordSet.getJsonObject("instance")));
                } else {
                    instanceHrids.add(instanceHrid);
                }
            }
            if (recordSet.containsKey("holdingsRecords")) {
                for (Object holdingsObject : recordSet.getJsonArray("holdingsRecords")) {
                    JsonObject holdingsRecord = ((JsonObject) holdingsObject);
                    String holdingsHrid = holdingsRecord.getString("hrid");
                    if (holdingsHrid != null) {
                        if (holdingsHrids.contains(holdingsHrid)) {
                            validation.registerError(
                               new ErrorReport(
                                    ErrorReport.ErrorCategory.VALIDATION,
                                    UNPROCESSABLE_ENTITY,
                                    "Holdings record HRID " + holdingsHrid
                                            + " appears more that once in batch.")
                                    .setShortMessage("Recurring HRID detected in batch")
                                    .setEntity(holdingsRecord)
                                    .setEntityType(InventoryRecord.Entity.HOLDINGS_RECORD)
                            );
                        } else {
                            holdingsHrids.add(holdingsHrid);
                        }
                    }
                    if (holdingsRecord.containsKey("items")) {
                        for (Object itemObject : holdingsRecord.getJsonArray("items")) {
                            String itemHrid = ((JsonObject) itemObject).getString("hrid");
                            if (itemHrid != null) {
                                if (itemHrids.contains(itemHrid)) {
                                    validation.registerError(
                                            new ErrorReport(
                                                    ErrorReport.ErrorCategory.VALIDATION,
                                                    UNPROCESSABLE_ENTITY,
                                                    "Item HRID " + itemHrid
                                                            + " appears more than once in batch.")
                                                    .setShortMessage("Recurring HRID detected in batch")
                                                    .setEntityType(InventoryRecord.Entity.ITEM)
                                                    .setEntity((JsonObject) itemObject));
                                } else {
                                    itemHrids.add(itemHrid);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /* PLANNING CREATES, UPDATES, DELETES */
    /**
     * Creates an in-memory representation of the instance, holdings, and items
     * as well as possible instance-to-instance relationships, that need to be created,
     * updated, or deleted in Inventory storage.
     *
     * @return a Future to confirm that plan was created
     */
    public UpdatePlanAllHRIDs planInventoryUpdates() {
        logger.debug("Planning inventory updates using repository");
        logger.debug( "Got " + repository.getPairsOfRecordSets().size() + " pair(s)");
        try {
            for (PairedRecordSets pair : repository.getPairsOfRecordSets()) {
                planInstanceHoldingsAndItems(pair);
                planInstanceRelations(pair);
            }
        } catch (NullPointerException npe) {
            logger.error("Null pointer in planInventoryUpdatesFromRepo");
            // TODO: return stack trace
            npe.printStackTrace();
        }
        return this;
    }

    private void planInstanceRelations(PairedRecordSets pair) {
        // Plan creates and deletes
        if (pair.hasIncomingRecordSet()) {
            // Set UUIDs for from-instance and to-instance, create provisional instance if required and possible
            pair.getIncomingRecordSet().resolveIncomingInstanceRelationsUsingRepository((RepositoryByHrids) repository);

            // Plan storage transactions
            for (InstanceToInstanceRelation incomingRelation : pair.getIncomingRecordSet().getInstanceToInstanceRelations()) {
                if (pair.hasExistingRecordSet()) {
                    if (pair.getExistingRecordSet().hasThisRelation(incomingRelation)) {
                        incomingRelation.skip();
                    } else {
                        incomingRelation.setTransition(Transaction.CREATE);
                    }
                } else {
                    incomingRelation.setTransition(Transaction.CREATE);
                }
            }
        }
        if (pair.hasExistingRecordSet()) {
            for (InstanceToInstanceRelation existingRelation : pair.getExistingRecordSet().getInstanceToInstanceRelations()) {
                if (pair.getIncomingRecordSet().isThisRelationOmitted(existingRelation)) {
                    existingRelation.setTransition(Transaction.DELETE);
                } else {
                    existingRelation.setTransition(Transaction.NONE);
                }
            }
        }
    }

    @Override
    public Future<Void> planInventoryDelete(OkapiClient okapiClient) {
        Promise<Void> promisedPlan = Promise.promise();
        lookupExistingRecordSet(okapiClient, instanceQuery).onComplete(lookup -> {
            if (lookup.succeeded()) {
                this.existingSet = lookup.result();
                // Plan instance update
                if (foundExistingRecordSet()) {
                    getExistingInstance().setTransition(Transaction.DELETE);
                    for (HoldingsRecord holdings : getExistingInstance().getHoldingsRecords()) {
                        holdings.setTransition(Transaction.DELETE);
                        for (Item item : holdings.getItems()) {
                            item.setTransition(Transaction.DELETE);
                        }
                    }
                    getExistingRecordSet().prepareAllInstanceRelationsForDeletion();
                    promisedPlan.complete();
                } else {
                    promisedPlan.fail("Instance to delete not found");
                }
            } else {
                promisedPlan.fail(
                        ErrorReport
                                .makeErrorReportFromJsonString(lookup.cause().getMessage())
                                .setShortMessage(
                                        "There was a problem looking for an existing instance in Inventory Storage.")
                                .asJsonString());
            }
        });
        return promisedPlan.future();
    }

    private void planInstanceHoldingsAndItems(PairedRecordSets pair) {
        if (pair.hasIncomingRecordSet()) {
            InventoryRecordSet incomingSet = pair.getIncomingRecordSet();
            Instance incomingInstance = incomingSet.getInstance();
            if (pair.hasExistingRecordSet()) {
                // Updates, deletes
                Instance existingInstance = pair.getExistingRecordSet().getInstance();
                incomingInstance.setUUID(existingInstance.getUUID());
                incomingInstance.setTransition(Transaction.UPDATE);
                incomingInstance.setVersion(existingInstance.getVersion());
                if (!incomingInstance.ignoreHoldings()) {
                    // If a record set came in with a list of holdings records (even if it was an empty list)
                    for (HoldingsRecord existingHoldingsRecord :
                            pair.getExistingRecordSet().getInstance().getHoldingsRecords()) {
                        HoldingsRecord incomingHoldingsRecord = incomingInstance.getHoldingsRecordByHRID(
                                existingHoldingsRecord.getHRID());
                        // HoldingsRecord gone, mark for deletion and check for existing items to delete with it
                        if (incomingHoldingsRecord == null) {
                            existingHoldingsRecord.setTransition(Transaction.DELETE);
                        } else {
                            // There is an existing holdings record with the same HRID,
                            // on the same Instance, update
                            incomingHoldingsRecord.setUUID(existingHoldingsRecord.getUUID());
                            incomingHoldingsRecord.setTransition(Transaction.UPDATE);
                            incomingHoldingsRecord.setVersion(existingHoldingsRecord.getVersion());
                        }
                        for (Item existingItem : existingHoldingsRecord.getItems()) {
                            Item incomingItem = pair.getIncomingRecordSet().getItemByHRID(
                                    existingItem.getHRID());
                            if (incomingItem == null) {
                                // An existing Item is gone from the Instance, delete
                                existingItem.setTransition(Transaction.DELETE);
                            } else {
                                // Existing Item still exists in incoming record (possibly under
                                // a different holdings record), update
                                incomingItem.setUUID(existingItem.getUUID());
                                incomingItem.setVersion(existingItem.getVersion());
                                incomingItem.setTransition(Transaction.UPDATE);
                                ProcessingInstructions instr = new ProcessingInstructions(
                                        pair.getIncomingRecordSet().getProcessingInfoAsJson());
                                if (instr.retainThisStatus(existingItem.getStatusName())) {
                                    incomingItem.setStatus(existingItem.getStatusName());
                                }
                            }
                        }
                    }
                }
            } else {
                if (!incomingInstance.hasUUID()) {
                    incomingInstance.generateUUID();
                }
                incomingInstance.setTransition(Transaction.CREATE);
            }
            // Remaining holdings and item transactions: Creates, imports from other Instance(s)
            // Find incoming holdings we didn't already resolve above
            List<HoldingsRecord> holdingsRecords =
                    incomingSet.getHoldingsRecordsByTransactionType(Transaction.UNKNOWN);
            for (HoldingsRecord holdingsRecord : holdingsRecords) {
                if (repository.existingHoldingsRecordsByHrid.containsKey(holdingsRecord.getHRID())) {
                    // Import from different Instance
                    HoldingsRecord existing = repository.existingHoldingsRecordsByHrid.get(
                            holdingsRecord.getHRID());
                    holdingsRecord.setTransition(Transaction.UPDATE);
                    holdingsRecord.setUUID(existing.getUUID());
                    holdingsRecord.setVersion(existing.getVersion());
                } else {
                    // The HRID does not exist in Inventory, create
                    holdingsRecord.setTransition(Transaction.CREATE);
                    if (!holdingsRecord.hasUUID()) {
                        holdingsRecord.generateUUID();
                    }
                }

            }
            // Find incoming items we didn't already resolve (update or delete) above
            List<Item> items = incomingSet.getItemsByTransactionType(Transaction.UNKNOWN);
            for (Item item : items) {
                if (repository.existingItemsByHrid.containsKey(item.getHRID())) {
                    // Import from different Instance
                    Item existing = repository.existingItemsByHrid.get(item.getHRID());
                    item.setTransition(Transaction.UPDATE);
                    item.setUUID(existing.getUUID());
                    item.setVersion(existing.getVersion());
                    ProcessingInstructions instr = new ProcessingInstructions(
                            pair.getIncomingRecordSet().getProcessingInfoAsJson());
                    if (instr.retainThisStatus(existing.getStatusName())) {
                        item.setStatus(existing.getStatusName());
                    }

                } else {
                    // The HRID does not exist in Inventory, create
                    item.setTransition(Transaction.CREATE);
                    if (!item.hasUUID()) {
                        item.generateUUID();
                    }
                }
            }
        }
    }
    /* END OF PLANNING METHODS */


    // EXECUTE CREATES, UPDATES, DELETES.
    @Override
    public Future<Void> doInventoryDelete(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        handleSingleSetDelete(okapiClient).onComplete(deletes -> {
            if (deletes.succeeded()) {
                promise.complete();
            } else {
                promise.fail(deletes.cause().getMessage());
            }
        });
        return promise.future();
    }

    public Future<Void> doInventoryUpdates(OkapiClient okapiClient, boolean batchOfOne) {
        Promise<Void> promise = Promise.promise();
        doCreateRecordsWithDependants(okapiClient).onComplete(prerequisitesCreated -> {
            if (prerequisitesCreated.succeeded()) {
                doUpdateInstancesAndHoldingsInBatch(okapiClient).onComplete(instancesAndHoldingsUpdated -> {
                    doCreateInstanceRelations(okapiClient).onComplete(relationsCreated -> {
                        doUpdateOrCreateItemsInBatch(okapiClient).onComplete(itemsUpdatedAndCreated ->{
                            if (prerequisitesCreated.succeeded() && instancesAndHoldingsUpdated.succeeded() && itemsUpdatedAndCreated.succeeded()) {
                                doDeleteRelationsItemsHoldings(okapiClient).onComplete(deletes -> {
                                    if (deletes.succeeded()) {
                                        if (relationsCreated.succeeded()) {
                                            promise.complete();
                                        } else {
                                            promise.fail(relationsCreated.cause().getMessage());
                                        }
                                    } else {
                                        promise.fail(deletes.cause().getMessage());
                                    }
                                });
                            } else {
                                // error in batch upserts - switch to record by record upsert if processing more than one record.
                                // If processing one record, return the error.
                                if (batchOfOne) {
                                    promise.fail(
                                            "There was a problem creating records, no deletes performed if any requested:"
                                                    + LF
                                                    + "  "
                                                    + ( prerequisitesCreated.failed() ? prerequisitesCreated.cause().getMessage() : "" )
                                                    + ( instancesAndHoldingsUpdated.failed() ? " "
                                                    + instancesAndHoldingsUpdated.cause().getMessage() : "" )
                                                    + ( itemsUpdatedAndCreated.failed() ? " "
                                                    + itemsUpdatedAndCreated.cause().getMessage() : "" ));
                                } else {
                                    if (prerequisitesCreated.failed()) {
                                        promise.fail(prerequisitesCreated.cause().getMessage());
                                    } else if (instancesAndHoldingsUpdated.failed()) {
                                        promise.fail(instancesAndHoldingsUpdated.cause().getMessage());
                                    } else if (itemsUpdatedAndCreated.failed()) {
                                        promise.fail(itemsUpdatedAndCreated.cause().getMessage());
                                    }
                                }
                            }
                        });
                    });
                });

            } else {
                promise.fail(prerequisitesCreated.cause().getMessage());
            }
        });
        return promise.future();
    }

    public Future<Void> doCreateInstanceRelations(OkapiClient okapiClient){
        Promise<Void> promise = Promise.promise();

        @SuppressWarnings("rawtypes")
        List<Future> provisionalInstancesFutures = new ArrayList<>();
        for (InstanceToInstanceRelation relation : repository.getInstanceRelationsToCreate()) {
            if (relation.getProvisionalInstance() != null) {
                if (relation.getProvisionalInstance().failed()) {
                    provisionalInstancesFutures.add(
                            failProvisionalInstanceCreation(relation.getProvisionalInstance()));
                } else {
                    provisionalInstancesFutures.add(
                            InventoryStorage.postInventoryRecord(
                                    okapiClient, relation.getProvisionalInstance()));
                }
            }
        }
        CompositeFuture.join(provisionalInstancesFutures).onComplete( allProvisionalInstancesCreated -> {
            if (allProvisionalInstancesCreated.succeeded()) {
                @SuppressWarnings("rawtypes")
                List<Future> createFutures = new ArrayList<>();
                for (InstanceToInstanceRelation relation : repository.getInstanceRelationsToCreate()) {
                    if (!relation.failed()) {
                        createFutures.add(InventoryStorage.postInventoryRecord(okapiClient, relation));
                    } else {
                        createFutures.add(failRelationCreation(relation));
                    }
                }
                CompositeFuture.join(createFutures).onComplete( allRelationsCreated -> {
                    if (allRelationsCreated.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(
                                ErrorReport.makeErrorReportFromJsonString(
                                        allRelationsCreated.cause().getMessage())
                                        .setShortMessage(
                                                "UpdatePlan using repository: There was an error creating instance relations")
                                        .asJsonString());
                    }
                });
            } else {
                promise.fail(ErrorReport.makeErrorReportFromJsonString(allProvisionalInstancesCreated.cause().getMessage())
                        .setShortMessage("There was an error creating provisional Instances")
                        .asJsonString());
            }
        });
        return promise.future();
    }

    // END OF STORAGE METHODS

}