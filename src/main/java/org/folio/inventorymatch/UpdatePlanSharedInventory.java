package org.folio.inventorymatch;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.folio.inventorymatch.InventoryRecordSet.HoldingsRecord;
import org.folio.inventorymatch.InventoryRecordSet.Item;
import org.folio.inventorymatch.InventoryRecordSet.Transition;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class UpdatePlanSharedInventory extends UpdatePlan {


    private static final Map<String,String> locationsToInstitutionsMap = new HashMap<String,String>();

    public UpdatePlanSharedInventory(InventoryRecordSet incomingSet, InventoryQuery existingInstanceQuery) {
        super(incomingSet, existingInstanceQuery);
    }

    @Override
    public Future<Void> planInventoryUpdates(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        Future<JsonObject> promisedExistingInventoryRecordSet = InventoryStorage.lookupSingleInventoryRecordSet(okapiClient, instanceQuery);
        promisedExistingInventoryRecordSet.onComplete( recordSet -> {
            if (recordSet.succeeded()) {
                JsonObject existingInventoryRecordSetJson = recordSet.result();
                this.existingSet = new InventoryRecordSet(existingInventoryRecordSetJson);
                if (existingInventoryRecordSetJson != null) {
                    JsonObject mergedInstance = mergeInstances(getExistingInstance().getJson(), getIncomingInstance().getJson());
                    getIncomingRecordSet().updateInstance(mergedInstance);
                }
                Future<Void> locationsMapReady = mapLocationsToInstitutions(okapiClient);
                locationsMapReady.onComplete( handler -> {
                    if (handler.succeeded()) {
                        logger.info("got institutions map: " + locationsToInstitutionsMap.toString());
                        flagAndIdRecords();
                        promise.complete();
                    } else {
                        promise.fail("There was a problem retrieving locations map, cannot perform updates");
                    }

                });

            } else {
                promise.fail("Error looking up existing record set");
            }
        });
        return promise.future();
    }

    /**
     * Merges properties of incoming instance with select properties of existing instance
     * (without mutating the original JSON objects)
     * @param existingInstance Existing instance
     * @param newInstance Instance coming in on the request
     * @return merged Instance
     */
    private JsonObject mergeInstances (JsonObject existingInstance, JsonObject newInstance) {
        JsonObject mergedInstance = newInstance.copy();

        // Merge both identifier lists into list of distinct identifiers
        JsonArray uniqueIdentifiers = mergeUniquelyTwoArraysOfObjects(
                existingInstance.getJsonArray("identifiers"),
                newInstance.getJsonArray("identifiers"));
        mergedInstance.put("identifiers", uniqueIdentifiers);
        mergedInstance.put("hrid", existingInstance.getString("hrid"));
        return mergedInstance;
    }

    private JsonArray mergeUniquelyTwoArraysOfObjects (JsonArray array1, JsonArray array2) {
        JsonArray merged = new JsonArray();
        if (array1 != null) {
        merged = array1.copy();
        }
        if (array2 != null) {
        for (int i=0; i<array2.size(); i++) {
            if (arrayContainsValue(merged,array2.getJsonObject(i))) {
            continue;
            } else {
            merged.add(array2.getJsonObject(i).copy());
            }
        }
        }
        return merged;
    }

    private boolean arrayContainsValue(JsonArray array, JsonObject value) {
        for (int i=0; i<array.size(); i++) {
        if (array.getJsonObject(i).equals(value)) return true;
        }
        return false;
    }

    private Future<Void> mapLocationsToInstitutions (OkapiClient okapiClient) {
        Promise<Void> mapReady = Promise.promise();
        boolean missMappings = false;
        for (HoldingsRecord record : incomingSet.getHoldingsRecords()) {
            if (! (locationsToInstitutionsMap.containsKey(record.getPermanentLocationId()))) {
                missMappings = true;
                break;
            }
        }
        if (missMappings) {
            logger.info("Miss mappings for at least one location, retrieving locations from Inventory storage");
            Future<JsonArray> promisedLocations = InventoryStorage.getLocations(okapiClient);
            promisedLocations.onComplete(gotLocations -> {
                if (gotLocations.succeeded()) {
                    JsonArray locationsJson = gotLocations.result();
                    if (locationsJson == null || locationsJson.isEmpty()) {
                        mapReady.fail("Retrieved a null or zero length array of locations from storage. Cannot map locations to institutions.");
                    } else {
                        Iterator<?> locationsIterator = locationsJson.iterator();
                        while (locationsIterator.hasNext()) {
                            JsonObject location = (JsonObject) locationsIterator.next();
                            locationsToInstitutionsMap.put(location.getString("id"), location.getString("institutionId"));

                        }
                        logger.info("Updated a map of " + locationsToInstitutionsMap.size() + " FOLIO locations to institutions.");
                        mapReady.complete();
                    }
                } else {
                    mapReady.fail("There was an error retrieving locations from Inventory storage");
                }
            });
        } else {
            mapReady.complete();
        }
        return mapReady.future();
    }

    protected void flagAndIdRecords () {
        // Plan instance update
        flagAndIdTheInstance();
        // Plan to clean out existing holdings and items
        flagExistingHoldingsAndItemsForDeletion();
        // Plan to (re-)create holdings and items
        flagAndIdIncomingHoldingsAndItemsForCreation();
    }

    private void flagExistingHoldingsAndItemsForDeletion () {
        for (HoldingsRecord existingHoldingsRecord : existingSet.getHoldingsRecords()) {
            if (existingHoldingsRecord.getInstitutionId(locationsToInstitutionsMap) != null 
                && existingHoldingsRecord.getInstitutionId(locationsToInstitutionsMap)
                  .equals(incomingSet.getInstitutionIdFromArbitraryHoldingsRecord(locationsToInstitutionsMap))) {
                existingHoldingsRecord.setTransition(Transition.DELETING);
                for (Item item : existingHoldingsRecord.getItems()) {
                    item.setTransition(Transition.DELETING);
                }
            } else {
                existingHoldingsRecord.setTransition(Transition.NONE);
                for (Item item : existingHoldingsRecord.getItems()) {
                    item.setTransition(Transition.NONE);
                }
            }
        }
    }

    private void flagAndIdIncomingHoldingsAndItemsForCreation () {
        logger.info("Got " + incomingSet.getHoldingsRecords().size() + " incoming holdings records. Instance's ID is currently " + incomingSet.getInstanceUUID());
        for (HoldingsRecord holdingsRecord : incomingSet.getHoldingsRecords()) {
            holdingsRecord.generateUUID();
            holdingsRecord.setTransition(Transition.CREATING);
            for (Item item : holdingsRecord.getItems()) {
                item.generateUUID();
                item.setTransition(Transition.CREATING);
            }
        }
    }

    @Override
    public Future<Void> doInventoryUpdates(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        Future<Void> promisedDeletes = handleDeletionsIfAny(okapiClient);
        promisedDeletes.onComplete(deletes -> {
            if (deletes.succeeded()) {
                Future<Void> promisedPrerequisites = createRecordsWithDependants(okapiClient);
                promisedPrerequisites.onComplete(prerequisites -> {
                    if (prerequisites.succeeded()) {
                        Future<Void> promisedInstanceAndHoldingsUpdates = handleInstanceAndHoldingsUpdatesIfAny(okapiClient);
                        promisedInstanceAndHoldingsUpdates.onComplete( instanceAndHoldingsUpdates -> {
                            if (instanceAndHoldingsUpdates.succeeded()) {
                                logger.debug("Successfully processed instance and holdings updates if any");
                                Future<Void> promisedItemUpdates = handleItemUpdatesAndCreatesIfAny (okapiClient);
                                promisedItemUpdates.onComplete(itemUpdatesAndCreates -> {
                                    if (itemUpdatesAndCreates.succeeded()) {
                                        promise.complete();
                                    } else {
                                        promise.fail("Error updating items: " + itemUpdatesAndCreates.cause().getMessage());
                                    }
                                });
                            } else {
                                promise.fail("Failed to process reference record(s) (instances,holdings): " + prerequisites.cause().getMessage());
                            }
                        });
                        logger.debug("Successfully created records referenced by other records if any");
                    } else {
                        promise.fail("There was an error while attempting to create instance and/or holdings");
                    }
                });
            } else {
                promise.fail("There was a problem processing deletes " + deletes.cause().getMessage());
            }
        });
        return promise.future();
    }

}