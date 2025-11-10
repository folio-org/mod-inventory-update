package org.folio.inventoryupdate.updating;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.folio.inventoryupdate.updating.entities.Instance;
import org.folio.inventoryupdate.updating.entities.InventoryRecord;
import org.folio.inventoryupdate.updating.entities.PairedRecordSets;
import org.folio.inventoryupdate.updating.entities.RecordIdentifiers;
import org.folio.inventoryupdate.updating.entities.HoldingsRecord;
import org.folio.inventoryupdate.updating.entities.InventoryRecordSet;
import org.folio.inventoryupdate.updating.entities.Item;
import org.folio.inventoryupdate.updating.entities.InventoryRecord.Transaction;
import org.folio.inventoryupdate.updating.entities.Repository;
import org.folio.inventoryupdate.updating.entities.RepositoryByMatchKey;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import static org.folio.inventoryupdate.updating.ErrorReport.UNPROCESSABLE_ENTITY;
import static org.folio.inventoryupdate.updating.entities.InventoryRecordSet.*;

public class UpdatePlanSharedInventory extends UpdatePlan {

  static final Map<String, String> LOCATIONS_TO_INSTITUTIONS_MAP = new HashMap<>();
  public static Map<String, String> locationsToInstitutionsMap () {
    return LOCATIONS_TO_INSTITUTIONS_MAP;
  }

  public UpdatePlanSharedInventory() {
    repository = new RepositoryByMatchKey();
  }


  @Override
  public Repository getNewRepository() {
    return new RepositoryByMatchKey();
  }

  @Override
  public RequestValidation validateIncomingRecordSets(JsonArray incomingRecordSets) {
    RequestValidation requestValidation = super.validateIncomingRecordSets(incomingRecordSets);
    UpdatePlanSharedInventory.checkForUniqueIdentifiersInBatch(requestValidation, incomingRecordSets);
    return requestValidation;
  }

  public static void checkForUniqueIdentifiersInBatch(RequestValidation validation, JsonArray inventoryRecordSets) {
    Set<String> localIdentifiers = new HashSet<>();
    Set<String> matchKeys = new HashSet<>();
    for (Object recordSetObject : inventoryRecordSets) {
      JsonObject recordSet = (JsonObject) recordSetObject;
      InventoryRecordSet set = InventoryRecordSet.makeIncomingRecordSet(recordSet);
      if (set.getLocalIdentifier() != null && !set.getLocalIdentifier().isEmpty() && localIdentifiers.contains(set.getLocalIdentifier())) {
        validation.registerError(
            new ErrorReport(
                ErrorReport.ErrorCategory.VALIDATION,
                UNPROCESSABLE_ENTITY,
                "Local identifier " + set.getLocalIdentifier() + " occurs more that once in this batch.")
                .setShortMessage("A local identifier is repeated in this batch")
                .setEntityType(InventoryRecord.Entity.INSTANCE)
                .setEntity(recordSet.getJsonObject(PROCESSING)));
      } else {
        localIdentifiers.add(set.getLocalIdentifier());
      }
      if (matchKeys.contains(set.getInstance().getMatchKey())) {
        validation.registerError(
            new ErrorReport(
                ErrorReport.ErrorCategory.VALIDATION,
                UNPROCESSABLE_ENTITY,
                "MatchKey " + set.getInstance().getMatchKey() + " occurs more that once in this batch.")
                .setShortMessage("A match key is repeated in this batch")
                .setEntityType(InventoryRecord.Entity.INSTANCE)
                .setEntity(recordSet.getJsonObject(INSTANCE)));
      } else {
        matchKeys.add(set.getInstance().getMatchKey());
      }
    }
  }

  @Override
  public UpdatePlan planInventoryUpdates() {
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
      planInstanceHoldingsAndItems(pair, secondaryInstance);
    }
    return this;

  }

  private static void planInstanceHoldingsAndItems(
      PairedRecordSets pair, Instance secondaryInstance) {
    if (pair.hasExistingRecordSet()) {
      JsonObject mergedInstance = mergeInstances(pair.getExistingRecordSet().getInstance().asJson(),
          pair.getIncomingRecordSet().getInstance().asJson());
      pair.getIncomingRecordSet().modifyInstance(mergedInstance);
    }
    flagAndIdRecordsForInventoryUpdating(
        pair.getIncomingRecordSet(),
        pair.getExistingRecordSet(),
        secondaryInstance);

  }

  public static void removeIdentifierFromInstanceForInstitution(RecordIdentifiers recordIdentifiers, JsonObject instance) {
    JsonArray identifiers = instance.getJsonArray("identifiers");
    for (int i = 0; i < identifiers.size(); i++) {
      JsonObject identifierObject = identifiers.getJsonObject(i);
      if (recordIdentifiers.identifierTypeId().equals(identifierObject.getString(IDENTIFIER_TYPE_ID))
          && recordIdentifiers.localIdentifier().equals(identifierObject.getString("value"))) {
        identifiers.remove(i);
        break;
      }
    }
  }

  /**
   * Merges properties of incoming instance with select properties of existing instance
   * (without mutating the original JSON objects)
   *
   * @param existingInstance Existing instance
   * @param newInstance      Instance coming in on the request
   * @return merged Instance
   */
  private static JsonObject mergeInstances(JsonObject existingInstance, JsonObject newInstance) {
    JsonObject mergedInstance = newInstance.copy();

    // Merge both identifier lists into list of distinct identifiers
    JsonArray uniqueIdentifiers = mergeUniquelyTwoArraysOfObjects(
        existingInstance.getJsonArray("identifiers"),
        newInstance.getJsonArray("identifiers"));
    mergedInstance.put("identifiers", uniqueIdentifiers);
    mergedInstance.put(HRID_IDENTIFIER_KEY, existingInstance.getString(HRID_IDENTIFIER_KEY));
    return mergedInstance;
  }

  private static JsonArray mergeUniquelyTwoArraysOfObjects(JsonArray array1, JsonArray array2) {
    JsonArray merged = new JsonArray();
    if (array1 != null) {
      merged = array1.copy();
    }
    if (array2 != null) {
      for (int i = 0; i < array2.size(); i++) {
        if (!arrayContainsValue(merged, array2.getJsonObject(i))) {
          merged.add(array2.getJsonObject(i).copy());
        }
      }
    }
    return merged;
  }

  private static boolean arrayContainsValue(JsonArray array, JsonObject value) {
    for (int i = 0; i < array.size(); i++) {
      if (array.getJsonObject(i).equals(value)) return true;
    }
    return false;
  }

  @Override
  public Future<List<InventoryUpdateOutcome>> multipleSingleRecordUpserts(UpdateRequest request, JsonArray inventoryRecordSets) {
    List<JsonArray> arraysOfOneRecordSet = new ArrayList<>();
    for (Object o : inventoryRecordSets) {
      JsonArray batchOfOne = new JsonArray().add(o);
      arraysOfOneRecordSet.add(batchOfOne);
    }
    return chainSingleRecordUpserts(request, arraysOfOneRecordSet, new UpdatePlanSharedInventory()::upsertBatch);
  }

  protected static void flagAndIdRecordsForInventoryUpdating(
      InventoryRecordSet updatingSet,
      InventoryRecordSet existingSet,
      Instance secondaryInstance) {
    prepareTheUpdatingInstance(updatingSet, existingSet);
    if (existingSet != null || secondaryInstance != null) {
      // Plan to clean out existing holdings and items
      flagExistingHoldingsAndItemsForDeletion(updatingSet, existingSet, secondaryInstance);
    }
    if (updatingSet != null) {
      // Plan to (re-)create holdings and items
      flagAndIdIncomingHoldingsAndItemsForCreation(updatingSet);
    }
    if (secondaryInstance != null) {
      RecordIdentifiers identifiers =
          RecordIdentifiers.identifiersWithLocalIdentifier(
              null,
              updatingSet.getLocalIdentifierTypeId(),
              updatingSet.getLocalIdentifier());
      UpdatePlanSharedInventory.removeIdentifierFromInstanceForInstitution(
          identifiers, secondaryInstance.asJson());
      secondaryInstance.setTransition(InventoryRecord.Transaction.UPDATE);
    }
  }

  private static void flagExistingHoldingsAndItemsForDeletion(
      InventoryRecordSet updatingSet,
      InventoryRecordSet existingSet,
      Instance secondaryInstance) {
    String institutionId = updatingSet.getInstitutionIdFromArbitraryHoldingsRecord(LOCATIONS_TO_INSTITUTIONS_MAP);
    List<HoldingsRecord> existingHoldingsRecords = new ArrayList<>();
    if (existingSet != null) {
      existingHoldingsRecords.addAll(existingSet.getHoldingsRecords());
    }
    if (secondaryInstance != null) {
      existingHoldingsRecords.addAll(secondaryInstance.getHoldingsRecords());
    }
    for (HoldingsRecord existingHoldingsRecord : existingHoldingsRecords) {
      if (existingHoldingsRecord.getInstitutionId(LOCATIONS_TO_INSTITUTIONS_MAP) != null
          && existingHoldingsRecord.getInstitutionId(LOCATIONS_TO_INSTITUTIONS_MAP)
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

  private static void flagAndIdIncomingHoldingsAndItemsForCreation(InventoryRecordSet incomingSet) {
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
  public Future<Void> doCreateInstanceRelations(OkapiClient okapiClient) {
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> doInventoryUpdates(OkapiClient okapiClient) {
    long startUpdates = System.currentTimeMillis();
    logger.debug("Doing Inventory updates using repository");
    Promise<Void> promise = Promise.promise();
    doDeleteRelationsItemsHoldings(okapiClient).onComplete(deletes -> {
      if (deletes.succeeded()) {
        doCreateRecordsWithDependants(okapiClient).onComplete(prerequisites ->
            doUpdateInstancesAndHoldings(okapiClient).onComplete(instanceAndHoldingsUpdates ->
                doCreateInstanceRelations(okapiClient).onComplete(relationsCreated ->
                    doUpdateItems(okapiClient).onComplete(itemUpdates ->
                        doCreateItems(okapiClient).onComplete(itemCreates -> {
                          if (prerequisites.succeeded()
                              && instanceAndHoldingsUpdates.succeeded()
                              && itemUpdates.succeeded()
                              && itemCreates.succeeded()) {
                            long updatesDone = System.currentTimeMillis() - startUpdates;
                            logger.debug("Updates performed in {} ms.", updatesDone);
                            promise.complete();
                          } else {
                            String error = "";
                            if (prerequisites.failed()) {
                              error = prerequisites.cause().getMessage();
                            } else if (instanceAndHoldingsUpdates.failed()) {
                              error = instanceAndHoldingsUpdates.cause().getMessage();
                            } else if (itemCreates.failed()) {
                              error = itemCreates.cause().getMessage();
                            }
                            promise.fail(
                                ErrorReport.makeErrorReportFromJsonString(error)
                                    .setShortMessage(
                                        "One or more errors occurred updating Inventory records")
                                    .asJsonString());
                          }
                        })))));
      } else {
        logger.error(deletes.cause().getMessage());
        if (deletes.cause().getMessage().startsWith("404")) {
          logger.error("One or more records to delete were not found: {}", deletes.cause().getMessage());
          promise.complete();
        } else {
          promise.fail(ErrorReport.makeErrorReportFromJsonString(deletes.cause().getMessage()).setShortMessage(
              "There was a problem processing deletes - all other updates skipped.").asJsonString());
        }
      }
    });
    return promise.future();
  }

  @Override
  public RequestValidation validateIncomingRecordSet(JsonObject inventoryRecordSet) {
    return new RequestValidation();
  }
}
