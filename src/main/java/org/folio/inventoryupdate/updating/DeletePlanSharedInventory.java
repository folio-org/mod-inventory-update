package org.folio.inventoryupdate.updating;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.updating.entities.*;
import org.folio.inventoryupdate.updating.instructions.ProcessingInstructionsDeletion;
import org.folio.okapi.common.OkapiClient;

import java.util.*;

import static org.folio.inventoryupdate.updating.entities.InventoryRecordSet.*;

public class DeletePlanSharedInventory extends DeletePlan {
  private RecordIdentifiers deletionIdentifiers;
  protected InventoryRecordSet updatingSet;
  protected static final Map<String,String> locationsToInstitutionsMap = new HashMap<>();

  private DeletePlanSharedInventory(InventoryQuery existingInstanceQuery) {
    super(existingInstanceQuery);
  }

  public boolean gotUpdatingRecordSet () {
    return updatingSet != null;
  }

  public boolean isInstanceUpdating () {
    return gotUpdatingRecordSet() && updatingSet.getInstance().getTransaction() == InventoryRecord.Transaction.UPDATE;
  }

  public Instance getUpdatingInstance() {
    return gotUpdatingRecordSet() ? updatingSet.getInstance() : null;
  }

  public static DeletePlanSharedInventory getDeletionPlan(RecordIdentifiers deletionIdentifiers) {
    InventoryQuery existingInstanceQuery = new QuerySharedInstanceByLocalIdentifier(
        deletionIdentifiers.localIdentifier(), deletionIdentifiers.identifierTypeId());
    DeletePlanSharedInventory deletePlan = new DeletePlanSharedInventory(existingInstanceQuery);
    deletePlan.deletionIdentifiers = deletionIdentifiers;
    return deletePlan;
  }

  @Override
  public Future<Void> planInventoryDelete(OkapiClient okapiClient, ProcessingInstructionsDeletion deleteInstructions) {
    return lookupExistingRecordSet(okapiClient, instanceQuery).compose(lookup -> {
      existingSet = lookup;
      if (!foundExistingRecordSet()) {
        return Future.failedFuture(new ErrorReport(ErrorReport.ErrorCategory.STORAGE, 404,
            "Record to be deleted was not found").asJsonString());
      }
      updatingSet = createUpdatingRecordSetFromExistingSet(existingSet, deletionIdentifiers);
      return mapLocationsToInstitutions(okapiClient, updatingSet, existingSet)
          .recover(cause -> Future.failedFuture(ErrorReport.makeErrorReportFromJsonString(cause.getMessage())
              .setShortMessage("There was a problem retrieving locations map, cannot perform updates.")
              .asJsonString()))
          .map(ignored -> {
            flagAndIdRecordsForInventoryUpdating(updatingSet, existingSet, deletionIdentifiers);
            return null;
          });
    }, cause -> Future.failedFuture(ErrorReport.makeErrorReportFromJsonString(cause.getMessage())
        .setShortMessage("Error looking up existing record set")
        .asJsonString()));
  }

  /**
   * In case of a delete request, build a 'updating record set' from the existing set from which records should be
   * updated and deleted
   * @param existingSet  The set of records that the delete request is targeting
   * @param recordIdentifiers identifiers for finding the pieces of data to remove from the targeted shared Instance
   */
  private static InventoryRecordSet createUpdatingRecordSetFromExistingSet (InventoryRecordSet existingSet, RecordIdentifiers recordIdentifiers) {
    JsonObject inventoryRecordSetForInstanceDeletion = new JsonObject();
    inventoryRecordSetForInstanceDeletion.put(INSTANCE, existingSet.getInstance().asJson());
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
      if ( recordIdentifiers.identifierTypeId().equals(identifierObject.getString(IDENTIFIER_TYPE_ID))
          && recordIdentifiers.localIdentifier().equals(identifierObject.getString("value"))) {
        identifiers.remove(i);
        break;
      }
    }
  }

  private static Future<Void> mapLocationsToInstitutions (OkapiClient okapiClient, InventoryRecordSet incomingSet, InventoryRecordSet existingSet) {
    boolean missMappings = false;
    List<HoldingsRecord> allHoldingsRecords = new ArrayList<>();
    if (incomingSet != null) allHoldingsRecords.addAll(incomingSet.getHoldingsRecords());
    if (existingSet != null) allHoldingsRecords.addAll(existingSet.getHoldingsRecords());
    if (incomingSet != null) {
      for (HoldingsRecord holdingsRecord : allHoldingsRecords) {
        if (! (locationsToInstitutionsMap.containsKey(holdingsRecord.getPermanentLocationId()))) {
          missMappings = true;
          break;
        }
      }
    }
    if (!missMappings) {
      return Future.succeededFuture();
    }
    logger.debug("Miss mappings for at least one location, retrieving locations from Inventory storage");
    return InventoryStorage.getLocations(okapiClient).compose(locationsJson -> {
      if (locationsJson == null || locationsJson.isEmpty()) {
        return Future.failedFuture("Retrieved a null or zero length array of locations from storage. Cannot map locations to institutions.");
      }
      for (Object entry : locationsJson) {
        JsonObject location = (JsonObject) entry;
        locationsToInstitutionsMap.put(location.getString("id"), location.getString(INSTITUTION_ID));
      }
      logger.debug("Updated a map of {} FOLIO locations to institutions.", locationsToInstitutionsMap.size());
      return Future.succeededFuture();
    });
  }

  protected static void flagAndIdRecordsForInventoryUpdating (
      InventoryRecordSet updatingSet,
      InventoryRecordSet existingSet,
      RecordIdentifiers deletionIdentifiers) {
    // Plan instance update/deletion
    if (existingSet != null) {
      updatingSet.getInstance().setTransition(InventoryRecord.Transaction.UPDATE);
      flagExistingHoldingsAndItemsForDeletion(existingSet,deletionIdentifiers);
    }
  }

  private static void flagExistingHoldingsAndItemsForDeletion (
      InventoryRecordSet existingSet,
      RecordIdentifiers deletionIdentifiers) {
    String institutionId = deletionIdentifiers.institutionId();
    List<HoldingsRecord> existingHoldingsRecords = new ArrayList<>();
    if (existingSet != null) {
      existingHoldingsRecords.addAll( existingSet.getHoldingsRecords() );
    }
    for (HoldingsRecord existingHoldingsRecord : existingHoldingsRecords) {
      if (existingHoldingsRecord.getInstitutionId(locationsToInstitutionsMap) != null
          && existingHoldingsRecord.getInstitutionId(locationsToInstitutionsMap)
          .equals(institutionId)) {
        existingHoldingsRecord.setTransition(InventoryRecord.Transaction.DELETE);
        for (Item item : existingHoldingsRecord.getItems()) {
          item.setTransition(InventoryRecord.Transaction.DELETE);
        }
      } else {
        existingHoldingsRecord.setTransition(InventoryRecord.Transaction.NONE);
        for (Item item : existingHoldingsRecord.getItems()) {
          item.setTransition(InventoryRecord.Transaction.NONE);
        }
      }
    }
  }

  @Override
  public Future<Void> doInventoryDelete(OkapiClient okapiClient) {
    logger.debug("Doing Inventory updates");
    return handleSingleSetDelete(okapiClient).compose(ignored -> handleSingleInstanceUpdate(okapiClient), cause -> {
      if (cause.getMessage().startsWith("404")) {
        logger.error("Records to delete not found: {}", cause.getMessage());
        return Future.succeededFuture();
      }
      return Future.failedFuture(ErrorReport.makeErrorReportFromJsonString(cause.getMessage())
          .setShortMessage("There was a problem processing deletes - all other updates skipped.").asJsonString());
    });
  }

  /**
   * Perform instance and holdings updates
   * @param okapiClient client for inventory storage requests
   */
  public Future<Void> handleSingleInstanceUpdate(OkapiClient okapiClient) {
    if (isInstanceUpdating()) {
      return InventoryStorage.putInventoryRecord(okapiClient, getUpdatingInstance()).mapEmpty();
    }
    return Future.succeededFuture();
  }

}
