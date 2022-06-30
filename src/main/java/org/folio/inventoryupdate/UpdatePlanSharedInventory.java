package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.Instance;
import org.folio.inventoryupdate.entities.InventoryRecord;
import org.folio.inventoryupdate.entities.PairedRecordSets;
import org.folio.inventoryupdate.entities.RecordIdentifiers;
import org.folio.inventoryupdate.entities.HoldingsRecord;
import org.folio.inventoryupdate.entities.InventoryRecordSet;
import org.folio.inventoryupdate.entities.Item;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.inventoryupdate.entities.RepositoryByMatchKey;
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

    private UpdatePlanSharedInventory (RepositoryByMatchKey repository) {
        super(repository);
    }

    public static UpdatePlanSharedInventory getUpsertPlan () {
        return new UpdatePlanSharedInventory(new RepositoryByMatchKey());
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

    public UpdatePlanSharedInventory setIncomingRecordSets (JsonArray inventoryRecordSets) {
        long setIncomingMs = System.currentTimeMillis();
        repository.setIncomingRecordSets(inventoryRecordSets);
        long timing = System.currentTimeMillis() - setIncomingMs;
        logger.debug("Incoming records set in " + timing + " ms.");
        return this;


    }

    public Future<Void> buildRepositoryFromStorage (RoutingContext routingContext) {
        long buildRepoStart = System.currentTimeMillis();
        Promise<Void> promise = Promise.promise();
        repository.buildRepositoryFromStorage(routingContext).onComplete(repositoryBuilt -> {
            if (repositoryBuilt.succeeded()) {
                long builtMs = System.currentTimeMillis() - buildRepoStart;
                logger.debug("Repo built in " +  builtMs + " ms.");
                promise.complete();
            } else {
                promise.fail(repositoryBuilt.cause().getMessage());
            }
        });
        return promise.future();
    }


    @Override
    public Future<Void> planInventoryDelete(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        lookupExistingRecordSet(okapiClient, instanceQuery).onComplete( lookup -> {
            if (lookup.succeeded()) {
                this.existingSet = lookup.result();
                if (!foundExistingRecordSet()) {
                    promise.fail("Record to be deleted was not found");
                } else  {
                    if (foundExistingRecordSet()) {
                        this.updatingSet = createUpdatingRecordSetFromExistingSet(existingSet, deletionIdentifiers );
                        mapLocationsToInstitutions(okapiClient,updatingSet,existingSet,null).onComplete(handler -> {
                            if (handler.succeeded()) {
                                // look for abandoned matches here
                                flagAndIdRecordsForInventoryUpdating(updatingSet,existingSet,null,isDeletion,deletionIdentifiers);
                                promise.complete();
                            } else {
                                promise.fail(ErrorReport.makeErrorReportFromJsonString(handler.cause().getMessage())
                                        .setShortMessage("There was a problem retrieving locations map, cannot perform updates.")
                                        .asJsonString());
                            }
                        });
                    }

                }
            } else {
                promise.fail(ErrorReport.makeErrorReportFromJsonString(lookup.cause().getMessage())
                        .setShortMessage("Error looking up existing record set")
                        .asJsonString());
            }
        });
        return promise.future();
    }

    @Override
    public UpdatePlan planInventoryUpdates() {
        logger.debug("Planning inventory updates using repository");
        logger.debug( "Got " + repository.getPairsOfRecordSets().size() + " pair(s)");
        try {
            long startPlanning = System.currentTimeMillis();
            for (PairedRecordSets pair : repository.getPairsOfRecordSets()) {
                Instance secondaryInstance = null;
                if (pair.hasIncomingRecordSet()) {
                    secondaryInstance = ((RepositoryByMatchKey) repository).secondaryInstancesByLocalIdentifier.get(pair.getIncomingRecordSet().getLocalIdentifier());
                    if (secondaryInstance != null) {
                        for (HoldingsRecord holdingsRecord : repository.existingHoldingsRecordsByInstanceId.get(secondaryInstance.getUUID()).values()) {
                            for (Item item : repository.existingItemsByHoldingsRecordId.get(holdingsRecord.getUUID()).values()) {
                                holdingsRecord.addItem(item);
                            }
                            secondaryInstance.addHoldingsRecord(holdingsRecord);
                        }
                    }
                }
                planInstanceHoldingsAndItems(pair, secondaryInstance, isDeletion, deletionIdentifiers);
            }
            long planningMs = System.currentTimeMillis() - startPlanning;
            logger.debug("Planning done in " + planningMs + " ms.");
        } catch (NullPointerException npe) {
            logger.error("Null pointer in planInventoryUpdatesFromRepo");
            npe.printStackTrace();
        }
        return this;

    }

    private static void planInstanceHoldingsAndItems(
            PairedRecordSets pair, Instance secondaryInstance, boolean deletion, RecordIdentifiers deletionIdentifiers) {
        if (pair.hasExistingRecordSet()) {
            JsonObject mergedInstance = mergeInstances(pair.getExistingRecordSet().getInstance().asJson(),
                    pair.getIncomingRecordSet().getInstance().asJson());
            pair.getIncomingRecordSet().modifyInstance(mergedInstance);
        }
        flagAndIdRecordsForInventoryUpdating(
                pair.getIncomingRecordSet(),
                pair.getExistingRecordSet(),
                secondaryInstance,
                deletion,
                deletionIdentifiers);

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
      InventoryRecordSet updatingRecordSetBasedOnExistingSet = InventoryRecordSet.makeIncomingRecordSet(inventoryRecordSetForInstanceDeletion);
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

    private static Future<Void> mapLocationsToInstitutions (OkapiClient okapiClient, InventoryRecordSet incomingSet, InventoryRecordSet existingSet, Instance secondaryInstance) {
        Promise<Void> mapReady = Promise.promise();
        boolean missMappings = false;
        List<HoldingsRecord> allHoldingsRecords = new ArrayList<>();
        if (incomingSet != null) allHoldingsRecords.addAll(incomingSet.getHoldingsRecords());
        if (existingSet != null) allHoldingsRecords.addAll(existingSet.getHoldingsRecords());
        if (secondaryInstance != null) allHoldingsRecords.addAll( secondaryInstance.getHoldingsRecords() );
        if (incomingSet != null) {
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

    protected static void flagAndIdRecordsForInventoryUpdating (
            InventoryRecordSet updatingSet,
            InventoryRecordSet existingSet,
            Instance secondaryInstance,
            boolean deletion,
            RecordIdentifiers deletionIdentifiers) {
      // Plan instance update/deletion
      if (deletion && existingSet != null) {
        updatingSet.getInstance().setTransition(Transaction.UPDATE);
        flagExistingHoldingsAndItemsForDeletion(updatingSet,existingSet,secondaryInstance,true,deletionIdentifiers);
      } else if (!deletion) {
        prepareTheUpdatingInstance(updatingSet,existingSet);
        if (existingSet != null || secondaryInstance != null) {
          // Plan to clean out existing holdings and items
          flagExistingHoldingsAndItemsForDeletion(updatingSet,existingSet,secondaryInstance,false,deletionIdentifiers);
        }
        if (updatingSet != null) {
          // Plan to (re-)create holdings and items
          flagAndIdIncomingHoldingsAndItemsForCreation(updatingSet);
        }
      }
      if (secondaryInstance != null) {
        RecordIdentifiers identifiers =
                  RecordIdentifiers.identifiersWithLocalIdentifier(
                          null,
                          updatingSet.getLocalIdentifierTypeId(),
                          updatingSet.getLocalIdentifier() );
        UpdatePlanSharedInventory.removeIdentifierFromInstanceForInstitution(
                identifiers, secondaryInstance.asJson() );
        secondaryInstance.setTransition( InventoryRecord.Transaction.UPDATE );
      }
    }

    private static void flagExistingHoldingsAndItemsForDeletion (
            InventoryRecordSet updatingSet,
            InventoryRecordSet existingSet,
            Instance secondaryInstance,
            boolean deletion,
            RecordIdentifiers deletionIdentifiers) {
        String institutionId = (deletion ? deletionIdentifiers.institutionId() : updatingSet.getInstitutionIdFromArbitraryHoldingsRecord(locationsToInstitutionsMap));
        List<HoldingsRecord> existingHoldingsRecords = new ArrayList<>();
        if (existingSet != null) {
            existingHoldingsRecords.addAll( existingSet.getHoldingsRecords() );
        }
        if (secondaryInstance != null) {
            existingHoldingsRecords.addAll(secondaryInstance.getHoldingsRecords());
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

    private static void flagAndIdIncomingHoldingsAndItemsForCreation (InventoryRecordSet incomingSet) {
        for (HoldingsRecord holdingsRecord : incomingSet.getHoldingsRecords()) {
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
    public Future<Void> doInventoryDelete(OkapiClient okapiClient) {
        logger.debug("Doing Inventory updates");
        Promise<Void> promise = Promise.promise();
        handleSingleSetDelete(okapiClient).onComplete(deletes -> {
          if (deletes.succeeded()) {
              handleSingleInstanceUpdate(okapiClient).onComplete(instanceAndHoldingsUpdates -> {
                  shiftingMatchKeyManager.handleUpdateOfInstanceWithPreviousMatchKeyIfAny(okapiClient).onComplete( previousInstanceUpdated -> {
                      if ( instanceAndHoldingsUpdates.succeeded())
                      {
                          promise.complete();
                      } else {
                          promise.fail(ErrorReport.makeErrorReportFromJsonString(
                                  instanceAndHoldingsUpdates.cause().getMessage())
                                  .setShortMessage("One or more errors occurred updating Inventory records")
                                  .asJsonString());
                      }
                  });
             });

          } else {
              promise.fail(ErrorReport.makeErrorReportFromJsonString(deletes.cause().getMessage())
                      .setShortMessage("There was a problem processing deletes - all other updates skipped." )
                      .asJsonString());
          }
        });
        return promise.future();
    }

    public Future<Void> doCreateInstanceRelations(OkapiClient okapiClient) {
        return Future.succeededFuture();
    }

    public Future<Void> doInventoryUpdates(OkapiClient okapiClient, boolean batchOfOne) {
        long startUpdates = System.currentTimeMillis();
        logger.debug("Doing Inventory updates using repository");
        Promise<Void> promise = Promise.promise();
        doDeleteRelationsItemsHoldings(okapiClient).onComplete(deletes -> {
            if (deletes.succeeded()) {
                doCreateRecordsWithDependants(okapiClient).onComplete(prerequisites -> {
                    doUpdateInstancesAndHoldingsInBatch(okapiClient).onComplete(instanceAndHoldingsUpdates -> {
                        doCreateInstanceRelations(okapiClient).onComplete(relationsCreated -> {
                            doUpdateOrCreateItemsInBatch(okapiClient).onComplete(itemUpdatesAndCreates -> {
                                if (prerequisites.succeeded() && instanceAndHoldingsUpdates.succeeded() && itemUpdatesAndCreates.succeeded()) {
                                    long updatesDone = System.currentTimeMillis() - startUpdates;
                                    logger.debug("Updates performed in " + updatesDone + " ms.");
                                    promise.complete();
                                } else {
                                    promise.fail(
                                            "One or more errors occurred updating Inventory records");
                                }

                            });
                        });
                    });
                });
            } else {
                promise.fail(
                        ErrorReport.makeErrorReportFromJsonString(
                                deletes.cause().getMessage())
                                .setShortMessage(
                                        "There was a problem processing deletes - all other updates skipped." )
                                .asJsonString());
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