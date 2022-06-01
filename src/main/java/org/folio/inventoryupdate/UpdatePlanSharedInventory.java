package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.folio.inventoryupdate.entities.RecordIdentifiers;
import org.folio.inventoryupdate.entities.HoldingsRecord;
import org.folio.inventoryupdate.entities.InventoryRecordSet;
import org.folio.inventoryupdate.entities.Item;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class UpdatePlanSharedInventory extends UpdatePlan {


    public static final Map<String,String> locationsToInstitutionsMap = new HashMap<>();
    private RecordIdentifiers deletionIdentifiers;
    private ShiftingMatchKeyManager shiftingMatchKeyManager;

    private UpdatePlanSharedInventory(InventoryRecordSet incomingSet, InventoryQuery existingInstanceQuery) {
        super(incomingSet, existingInstanceQuery);
    }

    public static UpdatePlanSharedInventory getUpsertPlan(InventoryRecordSet incomingSet) {
        MatchKey matchKey = new MatchKey(incomingSet.getInstance().asJson());
        incomingSet.getInstance().asJson().put("matchKey", matchKey.getKey());
        InventoryQuery instanceByMatchKeyQuery = new QueryMatchKey(matchKey.getKey());
        UpdatePlanSharedInventory updatePlan = new UpdatePlanSharedInventory( incomingSet, instanceByMatchKeyQuery );
        updatePlan.shiftingMatchKeyManager = new ShiftingMatchKeyManager( incomingSet, updatePlan.secondaryExistingSet, true );
        return updatePlan;
    }

    public static UpdatePlanSharedInventory getDeletionPlan(RecordIdentifiers deletionIdentifiers) {
        InventoryQuery existingInstanceQuery = new QuerySharedInstanceByLocalIdentifier(
                deletionIdentifiers.localIdentifier(), deletionIdentifiers.identifierTypeId());
        UpdatePlanSharedInventory updatePlan = new UpdatePlanSharedInventory( null, existingInstanceQuery );
        updatePlan.isDeletion = true;
        updatePlan.deletionIdentifiers = deletionIdentifiers;
        updatePlan.shiftingMatchKeyManager = new ShiftingMatchKeyManager( null, null,false );
        return updatePlan;
    }

    @Override
    public Future<Void> planInventoryUpdates(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        validateIncomingRecordSet(isDeletion ? new JsonObject() : updatingSet.getSourceJson());
        logger.debug((isDeletion ? "Deletion" : "Upsert" ) + ": Look up existing record set ");
        lookupExistingRecordSet(okapiClient, instanceQuery).onComplete( lookup -> {
            if (lookup.succeeded()) {
                this.existingSet = lookup.result();
                if (isDeletion && !foundExistingRecordSet()) {
                    promise.fail("Record to be deleted was not found");
                } else  {
                    if (foundExistingRecordSet()) {
                        if (isDeletion) {
                            this.updatingSet = createUpdatingRecordSetFromExistingSet(existingSet, deletionIdentifiers );
                        } else { // create or update
                            JsonObject mergedInstance = mergeInstances(getExistingInstance().asJson(), getUpdatingInstance().asJson());
                            getUpdatingRecordSet().modifyInstance(mergedInstance);
                        }
                    }
                    mapLocationsToInstitutions(okapiClient).onComplete(handler -> {
                        if (handler.succeeded()) {
                            // look for abandoned matches here
                            if (isDeletion) {
                                flagAndIdRecordsForInventoryUpdating();
                                promise.complete();
                            } else {
                                shiftingMatchKeyManager
                                        .findPreviousMatchKeyByRecordIdentifier( okapiClient )
                                        .onComplete(previousMatchKeyLookUp -> {
                                    if (previousMatchKeyLookUp.succeeded() && previousMatchKeyLookUp.result() != null) {
                                        secondaryExistingSet = previousMatchKeyLookUp.result();
                                    }
                                    flagAndIdRecordsForInventoryUpdating();
                                    promise.complete();
                                });
                            }
                        } else {
                            promise.fail("There was a problem retrieving locations map, cannot perform updates: " + handler.cause().getMessage());
                        }
                    });
                }
            } else {
                promise.fail("Error looking up existing record set: " + lookup.cause().getMessage());
            }
        });
        return promise.future();
    }


    /**
     * In case of a delete request, build a 'updating record set' from the existing set from which records should be
     * updated and deleted
     * @param existingSet  The set of records that the delete request is targeting
     * @param recordIdentifiers identifiers for finding the pieces of data to remove from the targeted shared Instance
     * @return
     */
    private static InventoryRecordSet createUpdatingRecordSetFromExistingSet (InventoryRecordSet existingSet, RecordIdentifiers recordIdentifiers) {
      JsonObject inventoryRecordSetForInstanceDeletion = new JsonObject();
      inventoryRecordSetForInstanceDeletion.put(InventoryRecordSet.INSTANCE, existingSet.getInstance().asJson());
      inventoryRecordSetForInstanceDeletion.put(InventoryRecordSet.HOLDINGS_RECORDS, new JsonArray());
      JsonObject processing = new JsonObject();
      processing.put(InventoryRecordSet.LOCAL_IDENTIFIER, recordIdentifiers.localIdentifier());
      processing.put(InventoryRecordSet.IDENTIFIER_TYPE_ID, recordIdentifiers.identifierTypeId());
      inventoryRecordSetForInstanceDeletion.put(InventoryRecordSet.PROCESSING, processing);
      InventoryRecordSet updatingRecordSetBasedOnExistingSet = new InventoryRecordSet(inventoryRecordSetForInstanceDeletion);
      removeIdentifierFromInstanceForInstitution(recordIdentifiers, updatingRecordSetBasedOnExistingSet.getInstance().asJson());
      return updatingRecordSetBasedOnExistingSet;
    }

    public static void removeIdentifierFromInstanceForInstitution( RecordIdentifiers recordIdentifiers, JsonObject instance) {
      JsonArray identifiers = instance.getJsonArray("identifiers");
      for (int i=0; i<identifiers.size(); i++) {
        JsonObject identifierObject = identifiers.getJsonObject(i);
        if ( recordIdentifiers.identifierTypeId().equals(identifierObject.getString("identifierTypeId"))
           && recordIdentifiers.localIdentifier().equals(identifierObject.getString("value"))) {
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
        List<HoldingsRecord> allHoldingsRecords = new ArrayList<>();
        if (gotUpdatingRecordSet()) allHoldingsRecords.addAll(updatingSet.getHoldingsRecords());
        if (foundExistingRecordSet()) allHoldingsRecords.addAll(existingSet.getHoldingsRecords());
        if (foundSecondaryExistingSet()) allHoldingsRecords.addAll( secondaryExistingSet.getHoldingsRecords() );
        if (gotUpdatingRecordSet()) {
          for (HoldingsRecord record : allHoldingsRecords) {
              if (! (locationsToInstitutionsMap.containsKey(record.getPermanentLocationId()))) {
                  missMappings = true;
                  break;
              }
          }
        }
        if (missMappings) {
            logger.debug("Miss mappings for at least one location, retrieving locations from Inventory storage");
            InventoryStorage.getLocations(okapiClient).onComplete(gotLocations -> {
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
                        logger.debug("Updated a map of " + locationsToInstitutionsMap.size() + " FOLIO locations to institutions.");
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
          logger.debug("Not a deletion");
        prepareTheUpdatingInstance();
        if (foundExistingRecordSet() || foundSecondaryExistingSet()) {
          // Plan to clean out existing holdings and items
          flagExistingHoldingsAndItemsForDeletion();
        }
        if (gotUpdatingRecordSet()) {
          // Plan to (re-)create holdings and items
          flagAndIdIncomingHoldingsAndItemsForCreation();
        }
      }
    }

    private void flagExistingHoldingsAndItemsForDeletion () {
        String institutionId = (isDeletion ? deletionIdentifiers.institutionId() : updatingSet.getInstitutionIdFromArbitraryHoldingsRecord(locationsToInstitutionsMap));
        List<HoldingsRecord> existingHoldingsRecords = new ArrayList<>();
        if (foundExistingRecordSet()) {
            existingHoldingsRecords.addAll( existingSet.getHoldingsRecords() );
        }
        if (foundSecondaryExistingSet()) {
            existingHoldingsRecords.addAll(secondaryExistingSet.getHoldingsRecords());
        }
        for (HoldingsRecord existingHoldingsRecord : existingHoldingsRecords) {
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
        for (HoldingsRecord holdingsRecord : updatingSet.getHoldingsRecords()) {
            if (!holdingsRecord.hasUUID()) {
                holdingsRecord.generateUUID();
            }
            holdingsRecord.setTransition(Transaction.CREATE);
            for (Item item : holdingsRecord.getItems()) {
                if (!item.hasUUID()) {
                    item.generateUUID();
                }
                item.setTransition(Transaction.CREATE);
            }
        }
    }

    @Override
    public Future<Void> doInventoryUpdates(OkapiClient okapiClient) {
        logger.debug("Doing Inventory updates");
        Promise<Void> promise = Promise.promise();
        handleDeletionsIfAny(okapiClient).onComplete(deletes -> {
          if (deletes.succeeded()) {
              createRecordsWithDependants(okapiClient).onComplete(prerequisites -> {
                  handleInstanceAndHoldingsUpdatesIfAny(okapiClient).onComplete( instanceAndHoldingsUpdates -> {
                      shiftingMatchKeyManager.handleUpdateOfInstanceWithPreviousMatchKeyIfAny(okapiClient).onComplete( previousInstanceUpdated -> {
                          handleItemUpdatesAndCreatesIfAny( okapiClient ).onComplete( itemUpdatesAndCreates -> {
                              if ( prerequisites.succeeded() && instanceAndHoldingsUpdates.succeeded() && itemUpdatesAndCreates.succeeded() )
                              {
                                  promise.complete();
                              } else {
                                  promise.fail( "One or more errors occurred updating Inventory records" );
                              }
                          });
                      });
                  });
              });
          } else {
              promise.fail("There was a problem processing deletes - all other updates skipped." );
          }
        });
        return promise.future();
    }

    @Override
    public List<HoldingsRecord> holdingsToDelete () {
        List<HoldingsRecord> holdingsRecords = super.holdingsToDelete();
        if (foundSecondaryExistingSet()) holdingsRecords.addAll( secondaryExistingSet.getHoldingsRecordsByTransactionType(Transaction.DELETE) );
        return holdingsRecords;
    }

    @Override
    public List<Item> itemsToDelete() {
        List<Item> items = super.itemsToDelete();
        if (foundSecondaryExistingSet()) items.addAll(secondaryExistingSet.getItemsByTransactionType(Transaction.DELETE));
        return items;
    }

    @Override
    public RequestValidation validateIncomingRecordSet(JsonObject inventoryRecordSet) {
        return new RequestValidation();
    }
}