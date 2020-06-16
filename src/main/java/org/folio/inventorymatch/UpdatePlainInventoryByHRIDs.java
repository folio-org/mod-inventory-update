package org.folio.inventorymatch;

import java.util.ArrayList;
import java.util.List;

import org.folio.inventorymatch.InventoryRecordSet.HoldingsRecord;
import org.folio.inventorymatch.InventoryRecordSet.Instance;
import org.folio.inventorymatch.InventoryRecordSet.InventoryRecord;
import org.folio.inventorymatch.InventoryRecordSet.Item;
import org.folio.inventorymatch.InventoryRecordSet.Transition;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

public class UpdatePlainInventoryByHRIDs extends UpdatePlan {


    public UpdatePlainInventoryByHRIDs(InventoryRecordSet incomingInventoryRecordSet,
                                       InventoryRecordSet existingInventoryRecordSet,
                                       OkapiClient okapiClient) {

        super(incomingInventoryRecordSet, existingInventoryRecordSet, okapiClient);
    }

    /**
     * Creates an in-memory representation of all instances, holdings, and items
     * that need to be created, updated, or deleted in Inventory storage.
     */
    public Future<Void> planInventoryUpdates (OkapiClient okapiClient) {
        Instance existingInstance = existingSet.getInstance();
        Instance incomingInstance = incomingSet.getInstance();

        if (existingInstance == null) {
            incomingInstance.generateUUID();
            incomingInstance.setTransition(Transition.CREATING);
        } else {
            incomingInstance.setUUID(existingInstance.getUUID());
            incomingInstance.setTransition(Transition.UPDATING);
        }

        if (existingInstance != null) {
            tagUpdatesDeletesAndLocalMoves(existingInstance, incomingInstance);
        }

        // Mark records to be created for an entirely new record set and/or holdings/items to be imported from other instance(s)
        return tagCreatesAndImports(okapiClient);
    }

    /**
     * For when there is an existing instance with the same ID already.
     * Mark existing records for update.
     * Find items that have moved between holdings locally and mark them for update.
     * Find records that have disappeared and mark them for deletion.
     */
    private void tagUpdatesDeletesAndLocalMoves(Instance existingInstance, Instance incomingInstance) {
        for (HoldingsRecord existingHoldingsRecord : existingInstance.getHoldingsRecords()) {
            HoldingsRecord incomingHoldingsRecord = incomingInstance.getHoldingsRecordByHRID(existingHoldingsRecord.getHRID());
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
       Lookup holdings/items that are migrating from other existing instances and
       mark them for update if any.
    */
    private Future<Void> tagCreatesAndImports(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        CompositeFuture future = mapHRIDtoUUIDonNewRecordsAndImports(okapiClient);
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
    public CompositeFuture mapHRIDtoUUIDonNewRecordsAndImports(OkapiClient okapiClient) {
        @SuppressWarnings("rawtypes")
        List<Future> recordFutures = new ArrayList<Future>();
        List<HoldingsRecord> holdingsRecords = incomingSet.getHoldingsRecordsByTransitionType(Transition.UNKNOWN);
        for (HoldingsRecord record : holdingsRecords) {
            recordFutures.add(setHoldingsUUIDandTransition(okapiClient, record));
        }
        List<Item> items = incomingSet.getItemsByTransitionType(Transition.UNKNOWN);
        for (Item item : items) {
            recordFutures.add(setItemUUIDandTransition(okapiClient, item));
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
    private Future<Void> setHoldingsUUIDandTransition (OkapiClient okapiClient, InventoryRecord record) {
        Promise<Void> promise = Promise.promise();
        InventoryQuery hridQuery = new HridQuery(record.getHRID());
        Future<JsonObject> promisedHoldingsRecord = InventoryStorage.lookupHoldingsRecordByHRID(okapiClient, hridQuery);
        promisedHoldingsRecord.onComplete( result -> {
            if (result.succeeded()) {
                if (result.result() == null) {
                    record.generateUUID();
                    record.setTransition(Transition.CREATING);
                } else {
                    record.setUUID(result.result().getString("id"));
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
    private Future<Void> setItemUUIDandTransition (OkapiClient okapiClient, InventoryRecord record) {
        Promise<Void> promise = Promise.promise();
        InventoryQuery hridQuery = new HridQuery(record.getHRID());
        Future<JsonObject> promisedItem = InventoryStorage.lookupItemByHRID(okapiClient, hridQuery);
        promisedItem.onComplete( result -> {
            if (result.succeeded()) {
                if (result.result() == null) {
                    record.generateUUID();
                    record.setTransition(Transition.CREATING);
                } else {
                    record.setUUID(result.result().getString("id"));
                    record.setTransition(Transition.UPDATING);
                }
                promise.complete();
            } else {
                promise.fail("Failed to look up item by HRID: " + result.cause().getMessage());
            }
        });
        return promise.future();
    }

}