package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.*;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;

import static org.folio.inventoryupdate.entities.InstanceRelations.failRelationCreation;

public class UpdatePlanAllHRIDs extends UpdatePlan {


    private static final String LF = System.lineSeparator();

    private UpdatePlanAllHRIDs (InventoryRecordSet incomingInventoryRecordSet, InventoryQuery existingInstanceQuery) {
        super(incomingInventoryRecordSet, existingInstanceQuery);
    }

    public UpdatePlanAllHRIDs (Repository repo) {
        super(repo);
    }

    public static UpdatePlanAllHRIDs getUpsertPlan(InventoryRecordSet incomingInventoryRecordSet) {
        InventoryQuery queryByInstanceHrid = new QueryByHrid(incomingInventoryRecordSet.getInstanceHRID());
        return new UpdatePlanAllHRIDs( incomingInventoryRecordSet, queryByInstanceHrid );
    }

    public static UpdatePlanAllHRIDs getDeletionPlan(InventoryQuery existingInstanceQuery) {
        UpdatePlanAllHRIDs updatePlan =  new UpdatePlanAllHRIDs( null, existingInstanceQuery);
        updatePlan.isDeletion = true;
        return updatePlan;
    }

    public UpdatePlanAllHRIDs planInventoryUpdatesUsingRepository() {
        logger.info("Planning inventory updates");
        try {
            int pairs = 0;
            for (PairedRecordSets pair : repository.getPairsOfRecordSets()) {
                pairs++;
                logger.info("Plan instance holdings items for pair " + pairs);
                planInstanceHoldingsAndItemsUsingRepository(pair);
            }
            logger.info("Plan relations");
            planInstanceRelationsUsingRepository();
        } catch (NullPointerException npe) {
            logger.error("Null pointer in planInventoryUpdatesFromRepo");
            npe.printStackTrace();
        }
        return this;
    }

    private void planInstanceRelationsUsingRepository() {
        // Plan creates and deletes
        for (PairedRecordSets pair : repository.getPairsOfRecordSets()) {
            if (pair.hasIncomingRecordSet()) {
                // Set UUIDs for from-instance and to-instance, create provisional instance if required and possible
                pair.getIncomingRecordSet().resolveIncomingInstanceRelationsUsingRepository(repository);

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

    }
    /**
     * Creates an in-memory representation of the instance, holdings, and items
     * as well as possible instance-to-instance relationships, that need to be created,
     * updated, or deleted in Inventory storage.
     *
     * @return a Future to confirm that plan was created
    */
    @Override
    public Future<Void> planInventoryUpdates (OkapiClient okapiClient) {
        Promise<Void> promisedPlan = Promise.promise();
        RequestValidation validation = validateIncomingRecordSet(isDeletion ? new JsonObject() : updatingSet.getSourceJson());
        if (validation.passed()) {
            lookupExistingRecordSet(okapiClient, instanceQuery).onComplete(lookup -> {
                if (lookup.succeeded()) {
                    this.existingSet = lookup.result();
                    // Plan instance update
                    if (isDeletion) {
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
                        prepareTheUpdatingInstance();
                        // Plan holdings/items updates
                        if (foundExistingRecordSet()) {
                            prepareUpdatesDeletesAndLocalMoves();
                        }
                        Future<Void> relationsFuture = getUpdatingRecordSet().prepareIncomingInstanceRelationRecords(okapiClient, getUpdatingInstance().getUUID());
                        Future<Void> prepareNewRecordsAndImportsFuture = prepareNewRecordsAndImports(okapiClient);
                        CompositeFuture.join(relationsFuture, prepareNewRecordsAndImportsFuture).onComplete(done -> {
                            if (done.succeeded()) {
                                getUpdatingRecordSet().getInstanceRelationsController().prepareInstanceRelationTransactions(updatingSet, existingSet);
                                promisedPlan.complete();
                            } else {
                                promisedPlan.fail("There was a problem fetching existing relations, holdings and/or items from storage:" + LF + "  " + done.cause().getMessage());
                            }
                        });
                    }
                } else {
                    promisedPlan.fail("There was a problem looking for an existing instance in Inventory Storage" + lookup.cause().getMessage());
                }
            });
        } else {
            promisedPlan.fail("Request did not provide a valid record set: " + validation);
        }
        return promisedPlan.future();
    }

    @Override
    public Future<Void> doInventoryUpdates (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        Future<Void> promisedPrerequisites = createRecordsWithDependants(okapiClient);
        promisedPrerequisites.onComplete(prerequisites -> {
            logger.debug("Successfully created records referenced by other records if any");

            handleInstanceAndHoldingsUpdatesIfAny(okapiClient).onComplete( instanceAndHoldingsUpdates -> {

                handleInstanceRelationCreatesIfAny(okapiClient).onComplete( relationsCreated -> {
                    if (relationsCreated.succeeded()) {
                        logger.debug("Successfully processed relationship create requests if any");
                    } else {
                        logger.error(relationsCreated.cause().getMessage());
                    }
                    handleItemUpdatesAndCreatesIfAny(okapiClient).onComplete(itemUpdatesAndCreates -> {
                        if (prerequisites.succeeded() && instanceAndHoldingsUpdates.succeeded() && itemUpdatesAndCreates.succeeded()) {
                            logger.debug("Successfully processed record create requests if any");
                            handleDeletionsIfAny(okapiClient).onComplete(deletes -> {
                                if (deletes.succeeded()) {
                                    if (relationsCreated.succeeded()) {
                                        promise.complete();
                                    } else {
                                        promise.fail("There was a problem creating Instance relationships: " + LF + relationsCreated.cause().getMessage());
                                    }
                                } else {
                                    promise.fail("There was a problem processing Inventory deletes:" + LF + "  " + deletes.cause().getMessage());
                                }
                            });
                        } else {
                            promise.fail("There was a problem creating records, no deletes performed if any requested:" + LF + "  " +
                                    (prerequisites.failed() ? prerequisites.cause().getMessage() : "")
                                    + (instanceAndHoldingsUpdates.failed() ? " " + instanceAndHoldingsUpdates.cause().getMessage() : "")
                                    + (itemUpdatesAndCreates.failed() ? " " + itemUpdatesAndCreates.cause().getMessage(): ""));
                        }
                    });
                });
            });
        });
        return promise.future();
    }

    public Future<Void> doInventoryUpdatesUsingRepository(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        Future<Void> promisedPrerequisites = createRecordsWithDependantsUsingRepository(okapiClient);
        promisedPrerequisites.onComplete(prerequisitesCreated -> {
            if (prerequisitesCreated.succeeded()) {
                handleInstanceAndHoldingsUpdatesIfAnyUsingRepository(okapiClient).onComplete(
                        instancesAndHoldingsUpdated -> {
                            handleInstanceRelationCreatesIfAnyUsingRepository(okapiClient).onComplete(relationsCreated ->
                            {
                                if (relationsCreated.succeeded()) {
                                    logger.debug("Successfully processed relationship create requests if any");
                                } else {
                                    logger.error(relationsCreated.cause().getMessage());
                                }
                                handleItemUpdatesAndCreatesIfAnyUsingRepository(okapiClient)
                                        .onComplete(itemsUpdatedAndCreated ->{
                                            if (prerequisitesCreated.succeeded() && instancesAndHoldingsUpdated.succeeded() && itemsUpdatedAndCreated.succeeded()) {
                                                logger.debug("Successfully processed record create requests if any");
                                                handleDeletionsIfAnyUsingRepository(okapiClient).onComplete(deletes -> {
                                                    if (deletes.succeeded()) {
                                                        if (relationsCreated.succeeded()) {
                                                            promise.complete();
                                                        } else {
                                                            promise.fail("There was a problem creating Instance relationships: " + LF + relationsCreated.cause().getMessage());
                                                        }
                                                    } else {
                                                        promise.fail("There was a problem processing Inventory deletes:" + LF + "  " + deletes.cause().getMessage());
                                                    }
                                                });
                                            } else {
                                                promise.fail("There was a problem creating records, no deletes performed if any requested:" + LF + "  " +
                                                        (prerequisitesCreated.failed() ? prerequisitesCreated.cause().getMessage() : "")
                                                        + (instancesAndHoldingsUpdated.failed() ? " " + instancesAndHoldingsUpdated.cause().getMessage() : "")
                                                        + (itemsUpdatedAndCreated.failed() ? " " + itemsUpdatedAndCreated.cause().getMessage(): ""));
                                            }
                                        });
                            });
                        });

            } else {
                promise.fail("Error creating Instances or holdings records");
            }
        });
        return promise.future();
    }

    @Override
    public RequestValidation validateIncomingRecordSet(JsonObject inventoryRecordSet) {
        RequestValidation validationErrors = new RequestValidation();
        if (isDeletion) return validationErrors;
        if (!inventoryRecordSet.getJsonObject("instance").containsKey("hrid")
            || inventoryRecordSet.getJsonObject("instance").getString("hrid").isEmpty()) {
            logger.error("Missing or empty HRID. Instances must have a HRID to be processed by this API");
            validationErrors.registerError("Missing or empty HRID. Instances must have a HRID to be processed by this API " + inventoryRecordSet.encodePrettily());
        }
        if (inventoryRecordSet.containsKey("holdingsRecords")) {
            inventoryRecordSet.getJsonArray("holdingsRecords")
                    .stream()
                    .map( rec -> (JsonObject) rec)
                    .forEach( record -> {
                        if (!record.containsKey("hrid")) {
                            logger.error("Holdings Records must have a HRID to be processed by this API");
                            validationErrors.registerError("Holdings Records must have a HRID to be processed by this API, received: " + record.encodePrettily());
                        }
                        if (record.containsKey("items")) {
                            record.getJsonArray("items")
                                    .stream()
                                    .map(item -> (JsonObject) item)
                                    .forEach(item -> {
                                        if (!item.containsKey("hrid")) {
                                            logger.error("Items must have a HRID to be processed by this API");
                                            validationErrors.registerError("Items must have a HRID to be processed by this API, received: " + item.encodePrettily());
                                        }
                                    });
                        }
                    });
        }
        return validationErrors;
    }

    public Future<Void> handleInstanceRelationCreatesIfAnyUsingRepository (OkapiClient okapiClient){
        Promise<Void> promise = Promise.promise();

        @SuppressWarnings("rawtypes")
        List<Future> provisionalInstancesFutures = new ArrayList<>();
        for (InstanceToInstanceRelation relation : repository.getInstanceRelationsToCreate()) {
            if (relation.requiresProvisionalInstanceToBeCreated() && relation.hasPreparedProvisionalInstance()) {
                provisionalInstancesFutures.add(InventoryStorage.postInventoryRecord(okapiClient, relation.getProvisionalInstance()));
            }
        }
        CompositeFuture.join(provisionalInstancesFutures).onComplete( allProvisionalInstancesCreated -> {
            if (allProvisionalInstancesCreated.succeeded()) {
                @SuppressWarnings("rawtypes")
                List<Future> createFutures = new ArrayList<>();
                for (InstanceToInstanceRelation relation : repository.getInstanceRelationsToCreate()) {
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


    public Future<Void> handleInstanceRelationCreatesIfAny (OkapiClient okapiClient) {
        if (!isDeletion) {
            return getUpdatingRecordSet().getInstanceRelationsController().handleInstanceRelationCreatesIfAny(okapiClient);
        } else {
            return Future.succeededFuture();
        }
    }

    /* PLANNING METHODS */

    private void planInstanceHoldingsAndItemsUsingRepository(PairedRecordSets pair) {
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

    /**
     * For when there is an existing instance with the same ID already.
     * Mark existing records for update.
     * Find items that have moved between holdings locally and mark them for update.
     * Find records that have disappeared and mark them for deletion.
     */
    private void prepareUpdatesDeletesAndLocalMoves() {
        if (! getUpdatingInstance().ignoreHoldings()) { // If a record set came in with a list of holdings records (even if it was an empty list)
            for (HoldingsRecord existingHoldingsRecord : getExistingInstance().getHoldingsRecords()) {
                HoldingsRecord incomingHoldingsRecord = getUpdatingInstance().getHoldingsRecordByHRID(existingHoldingsRecord.getHRID());
                // HoldingsRecord gone, mark for deletion and check for existing items to delete with it
                if (incomingHoldingsRecord == null) {
                    existingHoldingsRecord.setTransition(Transaction.DELETE);
                } else {
                    // There is an existing holdings record with the same HRID, on the same Instance
                    incomingHoldingsRecord.setUUID(existingHoldingsRecord.getUUID());
                    incomingHoldingsRecord.setTransition(Transaction.UPDATE);
                    incomingHoldingsRecord.setVersion( existingHoldingsRecord.getVersion() );
                }
                for (Item existingItem : existingHoldingsRecord.getItems()) {
                    Item incomingItem = updatingSet.getItemByHRID(existingItem.getHRID());
                    if (incomingItem == null) {
                        existingItem.setTransition(Transaction.DELETE);
                    } else {
                        incomingItem.setUUID(existingItem.getUUID());
                        incomingItem.setVersion( existingItem.getVersion() );
                        incomingItem.setTransition(Transaction.UPDATE);
                        ProcessingInstructions instr = new ProcessingInstructions(updatingSet.getProcessingInfoAsJson());
                        if (instr.retainThisStatus(existingItem.getStatusName())) {
                            incomingItem.setStatus(existingItem.getStatusName());
                        }
                    }
                }

            }
        }
    }


    /**
     * Catch up records that were not matched within an existing Instance (Transition = UNKNOWN)
     * Look them up in other instances in storage and if not found generate UUIDs for them.
     * @param okapiClient client for looking up existing records
     * @return a future with all holdingsRecord and item lookups.
     */
    public Future<Void> prepareNewRecordsAndImports(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        @SuppressWarnings("rawtypes")
        List<Future> recordFutures = new ArrayList<>();
        List<HoldingsRecord> holdingsRecords = updatingSet.getHoldingsRecordsByTransactionType(Transaction.UNKNOWN);
        for (HoldingsRecord record : holdingsRecords) {
            recordFutures.add(flagAndIdHoldingsByStorageLookup(okapiClient, record));
        }
        List<Item> items = updatingSet.getItemsByTransactionType(Transaction.UNKNOWN);
        for (Item item : items) {
            recordFutures.add(flagAndIdItemsByStorageLookup(okapiClient, item));
        }
        CompositeFuture.all(recordFutures).onComplete( handler -> {
            if (handler.succeeded()) {
                promise.complete();
            } else {
                promise.fail("Failed to retrieve UUIDs:" + LF + "  " + handler.cause().getMessage());
            }
        });
        return promise.future();
    }

    /**
     * Looks up existing holdings record by HRID and set UUID and transition state on incoming records
     * according to whether they were matched with existing records or not
     * @param record The incoming record to match with an existing record if any
     * @return empty future for determining when look-up is complete
     */
    private Future<Void> flagAndIdHoldingsByStorageLookup (OkapiClient okapiClient, InventoryRecord record) {
        Promise<Void> promise = Promise.promise();
        InventoryQuery hridQuery = new QueryByHrid(record.getHRID());
        InventoryStorage.lookupHoldingsRecordByHRID(okapiClient, hridQuery).onComplete( result -> {
            if (result.succeeded()) {
                if (result.result() == null) {
                    if (!record.hasUUID()) {
                        record.generateUUID();
                    }
                    record.setTransition(Transaction.CREATE);
                } else {
                    JsonObject existingHoldingsRecord = result.result();
                    record.setUUID(existingHoldingsRecord.getString( "id" ));
                    record.setVersion( existingHoldingsRecord.getInteger( InventoryRecord.VERSION ));
                    record.setTransition(Transaction.UPDATE);
                }
                promise.complete();
            } else {
                promise.fail("Failed to look up holdings record by HRID:" + LF + "  " + result.cause().getMessage());
            }
        });
        return promise.future();
    }


    /**
     * Looks up existing item record by HRID and set UUID and transition state according to whether it's found or not
     * @param record The incoming record to match with an existing record if any
     * @return empty future for determining when look-up is complete
     */
    private Future<Void> flagAndIdItemsByStorageLookup (OkapiClient okapiClient, InventoryRecord record) {
        Promise<Void> promise = Promise.promise();
        InventoryQuery hridQuery = new QueryByHrid(record.getHRID());
        InventoryStorage.lookupItemByHRID(okapiClient, hridQuery).onComplete( result -> {
            if (result.succeeded()) {
                if (result.result() == null) {
                    if (!record.hasUUID()) {
                        record.generateUUID();
                    }
                    record.setTransition(Transaction.CREATE);
                } else {
                    JsonObject existingItem = result.result();
                    record.setUUID(existingItem.getString("id"));
                    record.setVersion( existingItem.getInteger( InventoryRecord.VERSION ));
                    record.setTransition(Transaction.UPDATE);
                    //TODO: apply item status override rules
                }
                promise.complete();
            } else {
                promise.fail("Failed to look up item by HRID:" + LF + "  " + result.cause().getMessage());
            }
        });
        return promise.future();
    }

    /* END OF PLANNING METHODS */


}