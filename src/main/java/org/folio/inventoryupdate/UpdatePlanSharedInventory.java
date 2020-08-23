package org.folio.inventoryupdate;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.folio.inventoryupdate.entities.DeletionIdentifiers;
import org.folio.inventoryupdate.entities.HoldingsRecord;
import org.folio.inventoryupdate.entities.Item;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class UpdatePlanSharedInventory extends UpdatePlan {


    private static final Map<String,String> locationsToInstitutionsMap = new HashMap<String,String>();
    private DeletionIdentifiers deletionIdentifiers;

    /**
     * Constructor for create/update
     * @param incomingSet Records to create/update
     * @param existingInstanceQuery  Query to find possibly existing record to update
     */
    public UpdatePlanSharedInventory(InventoryRecordSet incomingSet, InventoryQuery existingInstanceQuery) {
        super(incomingSet, existingInstanceQuery);
    }

    /**
     * Constructor for delete
     * @param existingInstanceQuery Query to find the record to delete
     * @param deletionIdentifiers
     */
    public UpdatePlanSharedInventory(DeletionIdentifiers deletionIdentifiers, InventoryQuery existingInstanceQuery) {
      super(null, existingInstanceQuery);
      this.isDeletion = true;
      this.deletionIdentifiers = deletionIdentifiers;
    }

    @Override
    public Future<Void> planInventoryUpdates(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();

        Future<Void> promisedExistingInventoryRecordSet = lookupExistingRecordSet(okapiClient, instanceQuery);
        promisedExistingInventoryRecordSet.onComplete( recordSet -> {
            if (recordSet.succeeded()) {
                if (foundExistingRecordSet()) {
                  if (isDeletion) {
                    logger.debug("UpdatePlanSharedInventory: received deletion");
                    this.updatingSet = createUpdatingRecordSetFromExistingSet(existingSet, deletionIdentifiers);
                  } else { // create or update
                    JsonObject mergedInstance = mergeInstances(getExistingInstance().asJson(), getUpdatingInstance().asJson());
                    getUpdatingRecordSet().modifyInstance(mergedInstance);
                  }
                }
                Future<Void> locationsMapReady = mapLocationsToInstitutions(okapiClient);
                locationsMapReady.onComplete( handler -> {
                    if (handler.succeeded()) {
                        logger.debug("got institutions map: " + locationsToInstitutionsMap.toString());
                        flagAndIdRecordsForInventoryUpdating();
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

    private static InventoryRecordSet createUpdatingRecordSetFromExistingSet (InventoryRecordSet existingSet, DeletionIdentifiers deletionIdentifiers) {
      JsonObject inventoryRecordSetForInstanceDeletion = new JsonObject();
      inventoryRecordSetForInstanceDeletion.put("instance", existingSet.getInstance().asJson());
      inventoryRecordSetForInstanceDeletion.put("holdingsRecords", new JsonArray());
      InventoryRecordSet updatingRecordSetBasedOnExistingSet = new InventoryRecordSet(inventoryRecordSetForInstanceDeletion);
      removeIdentifierFromInstanceForInstitution(deletionIdentifiers, updatingRecordSetBasedOnExistingSet.getInstance().asJson());
      return updatingRecordSetBasedOnExistingSet;
    }

    private static void removeIdentifierFromInstanceForInstitution(DeletionIdentifiers deletionIdentifiers, JsonObject instance) {
      JsonArray identifiers = (JsonArray)instance.getJsonArray("identifiers");
      for (int i=0; i<identifiers.size(); i++) {
        JsonObject identifierObject = identifiers.getJsonObject(i);
        if (deletionIdentifiers.identifierTypeId().equals(identifierObject.getString("identifierTypeId"))
           && deletionIdentifiers.localIdentifier().equals(identifierObject.getString("value"))) {
          identifiers.remove(i);
          break;
        }
      }
    }

    /**
     * Merges properties of incoming instance with select properties of existing instance
     * (without mutating the original JSON objects)
     * @param existingInstance Existing instance
     * @param newInstance Instance coming in on the request
     * @return merged Instance
     */
    private static JsonObject mergeInstances (JsonObject existingInstance, JsonObject newInstance) {
        JsonObject mergedInstance = newInstance.copy();

        // Merge both identifier lists into list of distinct identifiers
        JsonArray uniqueIdentifiers = mergeUniquelyTwoArraysOfObjects(
                existingInstance.getJsonArray("identifiers"),
                newInstance.getJsonArray("identifiers"));
        mergedInstance.put("identifiers", uniqueIdentifiers);
        mergedInstance.put("hrid", existingInstance.getString("hrid"));
        return mergedInstance;
    }

    private static JsonArray mergeUniquelyTwoArraysOfObjects (JsonArray array1, JsonArray array2) {
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

    private static boolean arrayContainsValue(JsonArray array, JsonObject value) {
        for (int i=0; i<array.size(); i++) {
        if (array.getJsonObject(i).equals(value)) return true;
        }
        return false;
    }

    private Future<Void> mapLocationsToInstitutions (OkapiClient okapiClient) {
        Promise<Void> mapReady = Promise.promise();
        boolean missMappings = false;
        if (gotUpdatingRecordSet()) {
          for (HoldingsRecord record : updatingSet.getHoldingsRecords()) {
              if (! (locationsToInstitutionsMap.containsKey(record.getPermanentLocationId()))) {
                  missMappings = true;
                  break;
              }
          }
        }
        if (foundExistingRecordSet()) {
          for (HoldingsRecord record : existingSet.getHoldingsRecords()) {
              if (! (locationsToInstitutionsMap.containsKey(record.getPermanentLocationId()))) {
                  missMappings = true;
                  break;
              }
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

    protected void flagAndIdRecordsForInventoryUpdating () {
      // Plan instance update/deletion
      if (isDeletion && foundExistingRecordSet()) {
        getUpdatingInstance().setTransition(Transaction.UPDATE);
        flagExistingHoldingsAndItemsForDeletion();
      } else if (!isDeletion) {
        flagAndIdTheUpdatingInstance();
        // Plan to clean out existing holdings and items
        flagExistingHoldingsAndItemsForDeletion();
        // Plan to (re-)create holdings and items
        flagAndIdIncomingHoldingsAndItemsForCreation();
      }
    }

    private void flagExistingHoldingsAndItemsForDeletion () {
        String institutionId = (isDeletion ? deletionIdentifiers.institutionId() : updatingSet.getInstitutionIdFromArbitraryHoldingsRecord(locationsToInstitutionsMap));
        for (HoldingsRecord existingHoldingsRecord : existingSet.getHoldingsRecords()) {
            if (existingHoldingsRecord.getInstitutionId(locationsToInstitutionsMap) != null
                && existingHoldingsRecord.getInstitutionId(locationsToInstitutionsMap)
                  .equals(institutionId)) {
                existingHoldingsRecord.setTransition(Transaction.DELETE);
                for (Item item : existingHoldingsRecord.getItems()) {
                    item.setTransition(Transaction.DELETE);
                }
            } else {
                existingHoldingsRecord.setTransition(Transaction.NONE);
                for (Item item : existingHoldingsRecord.getItems()) {
                    item.setTransition(Transaction.NONE);
                }
            }
        }
    }

    private void flagAndIdIncomingHoldingsAndItemsForCreation () {
        logger.debug("Got " + updatingSet.getHoldingsRecords().size() + " incoming holdings records. Instance's ID is currently " + updatingSet.getInstanceUUID());
        for (HoldingsRecord holdingsRecord : updatingSet.getHoldingsRecords()) {
            holdingsRecord.generateUUID();
            holdingsRecord.setTransition(Transaction.CREATE);
            for (Item item : holdingsRecord.getItems()) {
                item.generateUUID();
                item.setTransition(Transaction.CREATE);
            }
        }
    }

    @Override
    public Future<Void> doInventoryUpdates(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        if (isDeletion && !foundExistingRecordSet()) {
          promise.complete();
        } else {
          Future<Void> promisedDeletes = handleDeletionsIfAny(okapiClient);
          promisedDeletes.onComplete(deletes -> {
              if (deletes.succeeded()) {
                  Future<Void> promisedPrerequisites = createRecordsWithDependants(okapiClient);
                  promisedPrerequisites.onComplete(prerequisites -> {
                      Future<Void> promisedInstanceAndHoldingsUpdates = handleInstanceAndHoldingsUpdatesIfAny(okapiClient);
                      promisedInstanceAndHoldingsUpdates.onComplete( instanceAndHoldingsUpdates -> {
                          Future<JsonObject> promisedItemUpdates = handleItemUpdatesAndCreatesIfAny (okapiClient);
                          promisedItemUpdates.onComplete(itemUpdatesAndCreates -> {
                              if (prerequisites.succeeded() && instanceAndHoldingsUpdates.succeeded() && itemUpdatesAndCreates.succeeded() ) {
                                  promise.complete();
                              } else {
                                  promise.fail("One or more errors occurred updating Inventory records");
                              }
                          });
                      });
                  });
              } else {
                  promise.fail("There was a problem processing deletes - all other updates skipped." );
              }
          });
        }
        return promise.future();
    }

}