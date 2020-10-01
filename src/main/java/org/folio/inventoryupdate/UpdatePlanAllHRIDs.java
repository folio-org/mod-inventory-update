package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.List;

import org.folio.inventoryupdate.entities.HoldingsRecord;
import org.folio.inventoryupdate.entities.InventoryRecord;
import org.folio.inventoryupdate.entities.Item;
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
     * Creates an in-memory representation of all instances, holdings, and items
     * that need to be created, updated, or deleted in Inventory storage.
     * @param okapiClient
     * @return a Future to confirm that plan was created
    */
    @Override
    public Future<Void> planInventoryUpdates (OkapiClient okapiClient) {
        Promise<Void> promisedPlan = Promise.promise();

        Future<Void> promisedInstanceLookup = lookupExistingRecordSet(okapiClient, instanceQuery);
        promisedInstanceLookup.onComplete( lookup -> {
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
                  }
                  promisedPlan.complete();
                } else {
                  flagAndIdTheUpdatingInstance();
                  // Plan holdings/items updates
                  if (foundExistingRecordSet()) {
                      flagAndIdUpdatesDeletesAndLocalMoves();
                  }
                  flagAndIdCreatesAndImports(okapiClient).onComplete( done -> {
                      if (done.succeeded()) {
                          promisedPlan.complete();
                      }
                  });
                }

            } else {
              if (isDeletion) {
                 // TODO: ?
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

            Future<Void> promisedInstanceAndHoldingsUpdates = handleInstanceAndHoldingsUpdatesIfAny(okapiClient);
            promisedInstanceAndHoldingsUpdates.onComplete( instanceAndHoldingsUpdates -> {
                Future<JsonObject> promisedItemUpdates = handleItemUpdatesAndCreatesIfAny (okapiClient);
                promisedItemUpdates.onComplete(itemUpdatesAndCreates -> {
                    if (prerequisites.succeeded() && instanceAndHoldingsUpdates.succeeded() && itemUpdatesAndCreates.succeeded()) {
                        logger.debug("Successfully processed record create requests if any");
                        Future<Void> promisedDeletes = handleDeletionsIfAny(okapiClient);
                        promisedDeletes.onComplete(deletes -> {
                            if (deletes.succeeded()) {
                                promise.complete();
                            } else {
                                promise.fail("There was a problem processing Inventory updates ");
                            }
                        });
                    } else {
                        promise.fail("There was a problem creating records, no deletes performed if any requested.");
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
       Mark non-previously-existing records for creation.
       Lookup holdings/items that could be migrating here from other existing instance(s)
       and mark them for update, if any.
    */
    private Future<Void> flagAndIdCreatesAndImports(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        CompositeFuture future = flagAndIdNewRecordsAndImports(okapiClient);
        future.onComplete(handler -> {
            if (handler.succeeded()) {
                promise.complete();
            } else {
                promise.fail("Failed to retrieve UUIDs: " + handler.cause().getMessage());
            }
        });
        return promise.future();
    }

    /**
     * Catch up records that were not matched within an existing Instance (Transition = UNKNOWN)
     * Look them up in other instances in storage and if not found generate UUIDs for them.
     * @param okapiClient client for looking up existing records
     * @return a future with all holdingsRecord and item lookups.
     */
    public CompositeFuture flagAndIdNewRecordsAndImports(OkapiClient okapiClient) {
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
        return CompositeFuture.all(recordFutures);
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
        Future<JsonObject> promisedHoldingsRecord = InventoryStorage.lookupHoldingsRecordByHRID(okapiClient, hridQuery);
        promisedHoldingsRecord.onComplete( result -> {
            if (result.succeeded()) {
                if (result.result() == null) {
                    record.generateUUID();
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
        Future<JsonObject> promisedItem = InventoryStorage.lookupItemByHRID(okapiClient, hridQuery);
        promisedItem.onComplete( result -> {
            if (result.succeeded()) {
                if (result.result() == null) {
                    record.generateUUID();
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