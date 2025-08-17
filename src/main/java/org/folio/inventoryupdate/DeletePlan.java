package org.folio.inventoryupdate;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.*;
import org.folio.inventoryupdate.instructions.ProcessingInstructionsDeletion;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    Promise<InventoryRecordSet> promise = Promise.promise();
    InventoryStorage.lookupSingleInventoryRecordSet(okapiClient, instanceQuery).onComplete(recordSet -> {
      if (recordSet.succeeded()) {
        JsonObject existingInventoryRecordSetJson = recordSet.result();
        if (existingInventoryRecordSetJson != null) {
          promise.complete(InventoryRecordSet.makeExistingRecordSet(existingInventoryRecordSetJson));
        } else {
          promise.complete(null);
        }
      } else {
        promise.fail(recordSet.cause().getMessage());
      }
    });
    return promise.future();
  }

  /**
   * Perform deletions of any relations to other instances and
   * deletions of items, holdings records, instance (if any and in that order)
   */
  public Future<Void> handleSingleSetDelete(OkapiClient okapiClient) {
    Promise<Void> promise = Promise.promise();
    Future.join(deleteRelationsAndItems(okapiClient)).onSuccess(
        relationsItemsDone -> Future.join(deleteHoldingsRecords(okapiClient)).onSuccess(
            holdingsDone -> {
              if (isInstanceDeleting()) {
                if (getExistingInstance().skipped()) {
                  InventoryStorage.putInventoryRecordOutcomeLess(okapiClient, getExistingInstance())
                      .onSuccess(res -> promise.complete()).onFailure(promise::fail);
                } else {
                  InventoryStorage.deleteInventoryRecord(okapiClient, getExistingRecordSet().getInstance())
                      .onSuccess(res -> promise.complete()).onFailure(promise::fail);
                }
              } else {
                promise.complete();
              }
            }
        ).onFailure(promise::fail)).onFailure(promise::fail);
    return promise.future();
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


}
