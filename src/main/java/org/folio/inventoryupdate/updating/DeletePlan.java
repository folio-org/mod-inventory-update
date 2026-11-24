package org.folio.inventoryupdate.updating;

import io.vertx.core.Future;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.updating.entities.*;
import org.folio.inventoryupdate.updating.instructions.ProcessingInstructionsDeletion;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.folio.inventoryupdate.updating.ErrorReport.INTERNAL_SERVER_ERROR;
import static org.folio.inventoryupdate.updating.ErrorReport.NOT_FOUND;
import static org.folio.inventoryupdate.updating.InventoryUpdateOutcome.MULTI_STATUS;
import static org.folio.inventoryupdate.updating.InventoryUpdateOutcome.OK;

public abstract class DeletePlan {
  protected InventoryQuery instanceQuery;
  // Existing Inventory records matching either an incoming record set or a set of deletion identifiers
  protected InventoryRecordSet existingSet = null;
  protected static final Logger logger = LogManager.getLogger("inventory-update");

  protected DeletePlan(InventoryQuery existingInstanceQuery) {
    this.instanceQuery = existingInstanceQuery;
  }

  public abstract Future<Void> planInventoryDelete(OkapiClient client, ProcessingInstructionsDeletion deleteInstructions);

  public abstract Future<Void> doInventoryDelete(OkapiClient client);

  public boolean isInstanceDeleting() {
    return foundExistingRecordSet() && existingSet.getInstance().getTransaction() == InventoryRecord.Transaction.DELETE;
  }

  public boolean foundExistingRecordSet() {
    return existingSet != null;
  }

  public InventoryRecordSet getExistingRecordSet() {
    return existingSet;
  }

  public Instance getExistingInstance() {
    return foundExistingRecordSet() ? existingSet.getInstance() : null;
  }

  public List<Item> itemsToDelete() {
    return foundExistingRecordSet() ? existingSet.getItemsByTransactionType(InventoryRecord.Transaction.DELETE) : new ArrayList<>();
  }

  public List<Item> itemsToSilentlyUpdate() {
    return foundExistingRecordSet() ? existingSet.getItemsForSilentUpdate() : new ArrayList<>();
  }

  public List<HoldingsRecord> holdingsToDelete() {
    return foundExistingRecordSet() ? existingSet.getHoldingsRecordsByTransactionType(InventoryRecord.Transaction.DELETE) : new ArrayList<>();
  }

  public List<HoldingsRecord> holdingsRecordsToSilentlyUpdate() {
    return foundExistingRecordSet() ? existingSet.getHoldingsRecordsForSilentUpdate() : new ArrayList<>();
  }

  public List<InstanceToInstanceRelation> instanceRelationsToDelete() {
    return foundExistingRecordSet() ? existingSet.getInstanceRelationsByTransactionType(InventoryRecord.Transaction.DELETE) : new ArrayList<>();
  }

  protected Future<InventoryRecordSet> lookupExistingRecordSet(OkapiClient okapiClient, InventoryQuery instanceQuery) {
    return InventoryStorage.lookupSingleInventoryRecordSet(okapiClient, instanceQuery)
        .map(recordSet -> recordSet == null ? null : InventoryRecordSet.makeExistingRecordSet(recordSet));
  }

  /**
   * Perform deletions of any relations to other instances and
   * deletions of items, holdings records, instance (if any and in that order)
   */
  public Future<Void> handleSingleSetDelete(OkapiClient okapiClient) {
    return Future.join(deleteRelationsAndItems(okapiClient))
        .compose(ignored -> Future.join(deleteHoldingsRecords(okapiClient)))
        .compose(ignored -> {
          if (!isInstanceDeleting()) {
            return Future.succeededFuture();
          }
          if (getExistingInstance().skipped()) {
            return InventoryStorage.putInventoryRecordOutcomeLess(okapiClient, getExistingInstance()).mapEmpty();
          }
          return InventoryStorage.deleteInventoryRecord(okapiClient, getExistingRecordSet().getInstance()).mapEmpty();
        });
  }

  List<Future<JsonObject>> deleteRelationsAndItems(OkapiClient okapiClient) {
    List<Future<JsonObject>> deleteRelationsDeleteItems = new ArrayList<>();
    for (InstanceToInstanceRelation relation : instanceRelationsToDelete()) {
      deleteRelationsDeleteItems.add(InventoryStorage.deleteInventoryRecord(okapiClient, relation));
    }
    for (Item item : itemsToDelete()) {
      deleteRelationsDeleteItems.add(InventoryStorage.deleteInventoryRecord(okapiClient, item));
    }
    for (Item item : itemsToSilentlyUpdate()) {
      deleteRelationsDeleteItems.add(InventoryStorage.putInventoryRecordOutcomeLess(okapiClient, item));
    }
    return deleteRelationsDeleteItems;
  }

  List<Future<JsonObject>> deleteHoldingsRecords(OkapiClient okapiClient) {
    List<Future<JsonObject>> deleteHoldingsRecords = new ArrayList<>();
    for (HoldingsRecord holdingsRecord : holdingsToDelete()) {
      deleteHoldingsRecords.add(InventoryStorage.deleteInventoryRecord(okapiClient, holdingsRecord));
    }
    for (HoldingsRecord holdingsRecord : holdingsRecordsToSilentlyUpdate()) {
      deleteHoldingsRecords.add(InventoryStorage.putInventoryRecordOutcomeLess(okapiClient, holdingsRecord));
    }
    return deleteHoldingsRecords;
  }

  public JsonObject getUpdateStats() {
    UpdateMetrics metrics = new UpdateMetrics();

    if (foundExistingRecordSet()) {
      if (existingSet.getInstance().isDeleting()) {
        InventoryRecord inventoryRecord = existingSet.getInstance();
        metrics.entity(inventoryRecord.entityType()).transaction(inventoryRecord.getTransaction()).outcomes.increment(inventoryRecord.getOutcome());
      }
      List<InventoryRecord> holdingsRecordsAndItemsInExistingSet = Stream.of(
          existingSet.getHoldingsRecords(),
          existingSet.getItems()
      ).flatMap(Collection::stream).collect(Collectors.toList());

      for (InventoryRecord inventoryRecord : holdingsRecordsAndItemsInExistingSet) {
        if (inventoryRecord.isDeleting()) {
          metrics.entity(inventoryRecord.entityType()).transaction(inventoryRecord.getTransaction()).outcomes.increment(inventoryRecord.getOutcome());
        }
      }
      if (!existingSet.getInstanceToInstanceRelations().isEmpty()) {
        for (InstanceToInstanceRelation instanceToInstanceRelation : existingSet.getInstanceToInstanceRelations()) {
          if (!instanceToInstanceRelation.getTransaction().equals(InventoryRecord.Transaction.NONE)) {
            metrics.entity(instanceToInstanceRelation.entityType()).transaction(instanceToInstanceRelation.getTransaction()).outcomes.increment(
                instanceToInstanceRelation.getOutcome());
          }
        }
      }
    }

    return metrics.asJson();
  }

  public JsonArray getErrors() {
    return getExistingRecordSet().getErrors();
  }

  public Future<InventoryUpdateOutcome> runDeletionPlan(UpdateRequest request) {
    ProcessingInstructionsDeletion deleteInstructions =
        new ProcessingInstructionsDeletion(request.bodyAsJson().getJsonObject("processing"));

    return planInventoryDelete(request.getOkapiClient(), deleteInstructions)
        .compose(ignored -> doInventoryDelete(request.getOkapiClient()).transform(deletionsDone -> {
          InventoryUpdateOutcome outcome = new InventoryUpdateOutcome()
              .setMetrics(UpdateMetrics.makeMetricsFromJson(getUpdateStats())).setResponseStatusCode(OK);
          if (deletionsDone.failed()) {
            outcome.setErrors(new JsonArray().add(new ErrorReport(ErrorReport.ErrorCategory.STORAGE,
                    INTERNAL_SERVER_ERROR, deletionsDone.cause().getMessage()).asJson()))
                .setResponseStatusCode(MULTI_STATUS);
          }
          return Future.succeededFuture(outcome);
        }), cause -> {
          if (!foundExistingRecordSet()) {
            return Future.succeededFuture(new InventoryUpdateOutcome(new ErrorReport(ErrorReport.ErrorCategory.STORAGE,
                NOT_FOUND, "Error processing delete request: " + cause.getMessage())).setResponseStatusCode(404));
          }
          return Future.succeededFuture(new InventoryUpdateOutcome(new ErrorReport(ErrorReport.ErrorCategory.STORAGE,
              INTERNAL_SERVER_ERROR, cause.getMessage())).setResponseStatusCode(400));
        });
  }

}
