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

public class UpdatePlanAllHRIDs extends UpdatePlan {


    private static final String LF = System.lineSeparator();
    /**
     * Constructs plan for creating or updating an Inventory record set
     * @param incomingInventoryRecordSet The record set to create or update with
     * @param existingInstanceQuery query to find existing record set to update
     */
    public UpdatePlanAllHRIDs(InventoryRecordSet incomingInventoryRecordSet, InventoryQuery existingInstanceQuery) {
        super(incomingInventoryRecordSet, existingInstanceQuery);
    }

    /**
     * Constructs plan for deleting an existing Inventory record set
     * @param existingInstanceQuery query to find existing Inventory records to delete
     */
    public UpdatePlanAllHRIDs (InventoryQuery existingInstanceQuery) {
      super(null, existingInstanceQuery);
        logger.debug("This is a deletion");
      this.isDeletion = true;
    }

    /**
     * Creates an in-memory representation of the instance, holdings, and items
     * as well as possible instance-to-instance relationships, that need to be created,
     * updated, or deleted in Inventory storage.
     *
     * @param okapiClient
     * @return a Future to confirm that plan was created
    */
    @Override
    public Future<Void> planInventoryUpdates (OkapiClient okapiClient) {
        Promise<Void> promisedPlan = Promise.promise();
        RequestValidation validation = validateIncomingRecordSet(isDeletion ? new JsonObject() : updatingSet.getSourceJson());
        if (validation.passed()) {
            lookupExistingRecordSet(okapiClient, instanceQuery).onComplete(lookup -> {
                if (lookup.succeeded()) {
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
                                getUpdatingRecordSet().getInstanceRelationsController().prepareIncomingInstanceRelations(updatingSet, existingSet);
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
            promisedPlan.fail("Request did not provide a valid record set: " + validation.toString());
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

    @Override
    public RequestValidation validateIncomingRecordSet(JsonObject inventoryRecordSet) {
        RequestValidation validationErrors = new RequestValidation();
        if (isDeletion) return validationErrors;
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

    public Future<JsonObject> handleInstanceRelationCreatesIfAny (OkapiClient okapiClient) {
        if (!isDeletion) {
            return getUpdatingRecordSet().getInstanceRelationsController().handleInstanceRelationCreatesIfAny(okapiClient);
        } else {
            Promise<JsonObject> promise = Promise.promise();
            promise.complete(null);
            return promise.future();
        }
    }

    /* PLANNING METHODS */


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
                    for (Item existingItem : existingHoldingsRecord.getItems()) {
                        Item incomingItem = updatingSet.getItemByHRID(existingItem.getHRID());
                        if (incomingItem == null) {
                            existingItem.setTransition(Transaction.DELETE);
                        } else {
                            // Item appear to be moved to another holdings record in the instance
                            incomingItem.setUUID(existingItem.getUUID());
                            incomingItem.setTransition(Transaction.UPDATE);
                        }
                    }
                } else {
                    // There is an existing holdings record with the same HRID, on the same Instance
                    incomingHoldingsRecord.setUUID(existingHoldingsRecord.getUUID());
                    incomingHoldingsRecord.setTransition(Transaction.UPDATE);
                    for (Item existingItem : existingHoldingsRecord.getItems()) {
                        Item incomingItem = updatingSet.getItemByHRID(existingItem.getHRID());
                        if (incomingItem == null) {
                            // The item is gone from the instance
                            existingItem.setTransition(Transaction.DELETE);
                        } else {
                            // There is an incoming item with the same HRID somewhere in the instance
                            incomingItem.setUUID(existingItem.getUUID());
                            incomingItem.setTransition(Transaction.UPDATE);
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
        List<Future> recordFutures = new ArrayList<Future>();
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
     * @param okapiClient
     * @param record The incoming record to match with an existing record if any
     * @return empty future for determining when look-up is complete
     */
    private Future<Void> flagAndIdHoldingsByStorageLookup (OkapiClient okapiClient, InventoryRecord record) {
        Promise<Void> promise = Promise.promise();
        InventoryQuery hridQuery = new HridQuery(record.getHRID());
        InventoryStorage.lookupHoldingsRecordByHRID(okapiClient, hridQuery).onComplete( result -> {
            if (result.succeeded()) {
                if (result.result() == null) {
                    if (!record.hasUUID()) {
                        record.generateUUID();
                    }
                    record.setTransition(Transaction.CREATE);
                } else {
                    String existingHoldingsRecordId = result.result().getString("id");
                    record.setUUID(existingHoldingsRecordId);
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
     * @param okapiClient
     * @param record The incoming record to match with an existing record if any
     * @return empty future for determining when loook-up is complete
     */
    private Future<Void> flagAndIdItemsByStorageLookup (OkapiClient okapiClient, InventoryRecord record) {
        Promise<Void> promise = Promise.promise();
        InventoryQuery hridQuery = new HridQuery(record.getHRID());
        InventoryStorage.lookupItemByHRID(okapiClient, hridQuery).onComplete( result -> {
            if (result.succeeded()) {
                if (result.result() == null) {
                    if (!record.hasUUID()) {
                        record.generateUUID();
                    }
                    record.setTransition(Transaction.CREATE);
                } else {
                    String existingItemId = result.result().getString("id");
                    record.setUUID(existingItemId);
                    record.setTransition(Transaction.UPDATE);
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