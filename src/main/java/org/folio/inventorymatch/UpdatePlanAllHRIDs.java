package org.folio.inventorymatch;

import java.util.ArrayList;
import java.util.List;

import org.folio.inventorymatch.InventoryRecordSet.HoldingsRecord;
import org.folio.inventorymatch.InventoryRecordSet.InventoryRecord;
import org.folio.inventorymatch.InventoryRecordSet.Item;
import org.folio.inventorymatch.InventoryRecordSet.Transition;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

public class UpdatePlanAllHRIDs extends UpdatePlan {



    public UpdatePlanAllHRIDs(InventoryRecordSet incomingInventoryRecordSet, InventoryQuery existingInstanceQuery) {
        super(incomingInventoryRecordSet, existingInstanceQuery);
    }

    /**
     * Creates an in-memory representation of all instances, holdings, and items
     * that need to be created, updated, or deleted in Inventory storage.
     */
    public Future<Void> planInventoryUpdates (OkapiClient okapiClient) {
        Promise<Void> promisedPlan = Promise.promise();
        Future<Void> promisedInstanceLookup = lookupExistingRecordSet(okapiClient, instanceQuery);
        promisedInstanceLookup.onComplete( lookup -> {
            if (lookup.succeeded()) {
                // Plan instance update
                flagAndIdTheInstance();

                // Plan holdings/items updates
                if (existingSet.getInstance() != null) {
                    flagAndIdUpdatesDeletesAndLocalMoves();
                }
                flagAndIdCreatesAndImports(okapiClient).onComplete( done -> {
                    if (done.succeeded()) {
                        promisedPlan.complete();
                    }
                });

            }
        });     
        return promisedPlan.future();
    }

    public Future<Void> doInventoryUpdates (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        Future<Void> promisedPrerequisites = createRecordsWithDependants(okapiClient);
        promisedPrerequisites.onComplete(prerequisites -> {
          if (prerequisites.succeeded()) {
              logger.debug("Successfully created records referenced by other records if any");
  
              // this has issues for updating holdings and items concurrently
              /*
              @SuppressWarnings("rawtypes")
              List<Future> testFutures = new ArrayList<Future>();
              testFutures.add(handleInstanceAndHoldingsUpdatesIfAny(okapiClient));
              testFutures.add(handleItemUpdatesAndCreatesIfAny(okapiClient));
              CompositeFuture.all(testFutures).onComplete(composite -> {
                  if (composite.succeeded()) {
                      Future<Void> promisedDeletes = handleDeletionsIfAny(okapiClient);
                      promisedDeletes.onComplete(deletes -> {
                          if (deletes.succeeded()) {
                              logger.debug("Successfully processed deletions if any.");
                              promise.complete();
                          } else {
                              promise.fail("There was a problem processing deletes " + deletes.cause().getMessage());
                          }
                      });
                  } else {
                      promise.fail("Failed to successfully process instance, holdings, item updates: " + composite.cause().getMessage());
                  }
              });
              */
  
              /* This works by updating holdings and items non-concurrently */
              Future<Void> promisedInstanceAndHoldingsUpdates = handleInstanceAndHoldingsUpdatesIfAny(okapiClient);
              promisedInstanceAndHoldingsUpdates.onComplete( instanceAndHoldingsUpdates -> {
                  if (instanceAndHoldingsUpdates.succeeded()) {
                      logger.debug("Successfully processed instance and holdings updates if any");
                      Future<Void> promisedItemUpdates = handleItemUpdatesAndCreatesIfAny (okapiClient);
                      promisedItemUpdates.onComplete(itemUpdatesAndCreates -> {
                          if (itemUpdatesAndCreates.succeeded()) {
                              Future<Void> promisedDeletes = handleDeletionsIfAny(okapiClient);
                              promisedDeletes.onComplete(deletes -> {
                                  if (deletes.succeeded()) {
                                      logger.debug("Successfully processed deletions if any.");
                                      promise.complete();
                                  } else {
                                      promise.fail("There was a problem processing deletes " + deletes.cause().getMessage());
                                  }
                              });
                          } else {
                              promise.fail("Error updating items: " + itemUpdatesAndCreates.cause().getMessage());
                          }
                      });
                  } else {
                      promise.fail("Failed to process reference record(s) (instances,holdings): " + prerequisites.cause().getMessage());
                  }
              });
              /* end */
          } else {
              promise.fail("Failed to create prerequisites");
          }
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
            HoldingsRecord incomingHoldingsRecord = getIncomingInstance().getHoldingsRecordByHRID(existingHoldingsRecord.getHRID());
            // HoldingsRecord gone, mark for deletion and check for existing items to delete with it
            if (incomingHoldingsRecord == null) {
                existingHoldingsRecord.setTransition(Transition.DELETING);
                for (Item existingItem : existingHoldingsRecord.getItems()) {
                    Item incomingItem = incomingSet.getItemByHRID(existingItem.getHRID());
                    if (incomingItem == null) {
                        existingItem.setTransition(Transition.DELETING);
                    } else {
                        // Item appear to be moved to another holdings record in the instance
                        incomingItem.setUUID(existingItem.getUUID());
                        incomingItem.setTransition(Transition.UPDATING);
                    }
                }
            } else {
                // There is an existing holdings record with the same HRID, on the same Instance
                incomingHoldingsRecord.setUUID(existingHoldingsRecord.getUUID());
                incomingHoldingsRecord.setTransition(Transition.UPDATING);
                for (Item existingItem : existingHoldingsRecord.getItems()) {
                    Item incomingItem = incomingSet.getItemByHRID(existingItem.getHRID());
                    if (incomingItem == null) {
                        // The item is gone from the instance
                        existingItem.setTransition(Transition.DELETING);
                    } else {
                        // There is an incoming item with the same HRID somewhere in the instance
                        incomingItem.setUUID(existingItem.getUUID());
                        incomingItem.setTransition(Transition.UPDATING);
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
        List<HoldingsRecord> holdingsRecords = incomingSet.getHoldingsRecordsByTransitionType(Transition.UNKNOWN);
        for (HoldingsRecord record : holdingsRecords) {
            recordFutures.add(flagAndIdHoldingsByStorageLookup(okapiClient, record));
        }
        List<Item> items = incomingSet.getItemsByTransitionType(Transition.UNKNOWN);
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
     * @return empty future for determining when loook-up is complete
     */
    private Future<Void> flagAndIdHoldingsByStorageLookup (OkapiClient okapiClient, InventoryRecord record) {
        Promise<Void> promise = Promise.promise();
        InventoryQuery hridQuery = new HridQuery(record.getHRID());
        Future<JsonObject> promisedHoldingsRecord = InventoryStorage.lookupHoldingsRecordByHRID(okapiClient, hridQuery);
        promisedHoldingsRecord.onComplete( result -> {
            if (result.succeeded()) {
                if (result.result() == null) {
                    record.generateUUID();
                    record.setTransition(Transition.CREATING);
                } else {
                    String existingHoldingsRecordId = result.result().getString("id");
                    record.setUUID(existingHoldingsRecordId);
                    record.setTransition(Transition.UPDATING);
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
                    record.setTransition(Transition.CREATING);
                } else {
                    String existingItemId = result.result().getString("id");
                    record.setUUID(existingItemId);
                    record.setTransition(Transition.UPDATING);
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