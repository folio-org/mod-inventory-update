package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.List;

import org.folio.inventoryupdate.entities.*;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

public class UpdatePlanAllHRIDs extends UpdatePlan {


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
        // compose with look-up of instance relationships
        lookupExistingRecordSet(okapiClient, instanceQuery).onComplete( lookup -> {
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
                    for (InstanceRelationship relation : getExistingInstance().getRelations()) {
                        relation.setTransition(Transaction.DELETE);
                    }
                  }
                  promisedPlan.complete();
                } else {
                  flagAndIdTheUpdatingInstance();
                  // Plan holdings/items updates
                  if (foundExistingRecordSet()) {
                      flagAndIdUpdatesDeletesAndLocalMoves();
                  }
                  flagAndIdNewRecordsAndImports(okapiClient).onComplete( done -> {
                      if (done.succeeded()) {
                          promisedPlan.complete();
                      }
                  });
                }
            }
        });
        return promisedPlan.future();
    }

    @Override
    public Future<Void> doInventoryUpdates (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        Future<Void> promisedPrerequisites = createRecordsWithDependants(okapiClient);
        promisedPrerequisites.onComplete(prerequisites -> {
            logger.debug("Successfully created records referenced by other records if any");

            handleInstanceAndHoldingsUpdatesIfAny(okapiClient).onComplete( instanceAndHoldingsUpdates -> {
                handleItemUpdatesAndCreatesIfAny (okapiClient).onComplete(itemUpdatesAndCreates -> {
                    if (prerequisites.succeeded() && instanceAndHoldingsUpdates.succeeded() && itemUpdatesAndCreates.succeeded()) {
                        logger.debug("Successfully processed record create requests if any");
                        handleDeletionsIfAny(okapiClient).onComplete(deletes -> {
                            if (deletes.succeeded()) {
                                promise.complete();
                            } else {
                                promise.fail("There was a problem processing Inventory deletes: " + deletes.cause().getMessage());
                            }
                        });
                    } else {
                        promise.fail("There was a problem creating records, no deletes performed if any requested: " + prerequisites.cause().getMessage());
                    }
                });

            });
        });
        return promise.future();
    }

    /* PLANNING METHODS */


    /**
     * For when there is an existing instance with the same ID already.
     * Mark existing records for update.
     * Find items that have moved between holdings locally and mark them for update.
     * Find records that have disappeared and mark them for deletion.
     */
    private void flagAndIdUpdatesDeletesAndLocalMoves() {

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

    /**
     * Catch up records that were not matched within an existing Instance (Transition = UNKNOWN)
     * Look them up in other instances in storage and if not found generate UUIDs for them.
     * @param okapiClient client for looking up existing records
     * @return a future with all holdingsRecord and item lookups.
     */
    public Future<Void> flagAndIdNewRecordsAndImports(OkapiClient okapiClient) {
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
                promise.fail("Failed to retrieve UUIDs: " + handler.cause().getMessage());
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
                promise.fail("Failed to look up holdings record by HRID: " + result.cause().getMessage());
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
                promise.fail("Failed to look up item by HRID: " + result.cause().getMessage());
            }
        });
        return promise.future();
    }

    /* END OF PLANNING METHODS */


}