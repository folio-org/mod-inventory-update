package org.folio.inventoryupdate.updating.service;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.updating.DeletePlanAllHRIDs;
import org.folio.inventoryupdate.updating.DeletePlanSharedInventory;
import org.folio.inventoryupdate.updating.InventoryQuery;
import org.folio.inventoryupdate.updating.InventoryUpdateOutcome;
import org.folio.inventoryupdate.updating.QueryByHrid;
import org.folio.inventoryupdate.updating.UpdateMetrics;
import org.folio.inventoryupdate.updating.UpdatePlan;
import org.folio.inventoryupdate.updating.UpdatePlanAllHRIDs;
import org.folio.inventoryupdate.updating.UpdatePlanSharedInventory;
import org.folio.inventoryupdate.updating.UpdateRequest;
import org.folio.inventoryupdate.updating.entities.RecordIdentifiers;
import org.folio.okapi.common.ErrorType;

import static org.folio.inventoryupdate.updating.InventoryUpdateOutcome.MULTI_STATUS;
import static org.folio.inventoryupdate.updating.InventoryUpdateOutcome.OK;
import static org.folio.okapi.common.HttpResponse.responseError;
import static org.folio.okapi.common.HttpResponse.responseJson;


public class HandlersUpdating {

  public static final Logger logger = LogManager.getLogger("inventory-update");

  public void handleInventoryUpsertByHRID(UpdateRequest updateRequest) {
    UpdatePlan plan = new UpdatePlanAllHRIDs();
    doUpsert(updateRequest, plan);
  }

  public void handleInventoryUpsertByHRIDBatch(UpdateRequest request) {
    UpdatePlan plan = new UpdatePlanAllHRIDs();
    doBatchUpsert(request, plan)
        .onSuccess(outcome -> outcome.respond(request.routingContext()))
        .onFailure(e -> responseError(request.routingContext(), ErrorType.ANY, e.getCause()));
  }

  public void handleSharedInventoryUpsertByMatchKey(UpdateRequest request) {
    UpdatePlan plan = new UpdatePlanSharedInventory();
    doUpsert(request, plan);
  }

  public void handleSharedInventoryUpsertByMatchKeyBatch(UpdateRequest request) {
    UpdatePlan plan = new UpdatePlanSharedInventory();
    doBatchUpsert(request, plan)
        .onSuccess(outcome -> outcome.respond(request.routingContext()))
        .onFailure(e -> responseError(request.routingContext(), ErrorType.ANY, e.getCause()));
  }


  /**
   * Validates a single incoming record set and performs an upsert
   * @param plan a shared-inventory/matchKey, or an inventory/hrid upsert plan.
   */
  private void doUpsert(UpdateRequest request, UpdatePlan plan) {
    JsonArray inventoryRecordSets = new JsonArray();
    inventoryRecordSets.add(request.bodyAsJson());
    plan.upsertBatch(request, inventoryRecordSets).onComplete(update ->{
      if (update.succeeded()) {
        if (update.result().getStatusCode() == OK || update.result().getStatusCode() == MULTI_STATUS) {
          responseJson(request.routingContext(), update.result().getStatusCode()).end(update.result().getJson().encodePrettily());
        } else {
          update.result().getErrorResponse().respond(request.routingContext());
        }
      }
    });
  }

  /**
   * Validates a batch of incoming record sets and performs a batch-upsert
   * @param plan a shared-inventory/matchKey, or an inventory/hrid upsert plan.
   */
  public Future<InventoryUpdateOutcome> doBatchUpsert(UpdateRequest updateRequest, UpdatePlan plan) {
    Promise<InventoryUpdateOutcome> promise = Promise.promise();
    JsonArray inventoryRecordSets = updateRequest.bodyAsJson().getJsonArray("inventoryRecordSets");
    plan.upsertBatch(updateRequest, inventoryRecordSets).onComplete(update -> {
      // The upsert could succeed, but with an error report, if it was a batch of one
      // Only if a true batch upsert (of more than one) failed, will the promise fail.
      if (update.succeeded()) {
        promise.complete(update.result());
      } else {
        logger.error("A batch upsert failed, bringing down all records of the batch. Switching to record-by-record updates");
        UpdateMetrics accumulatedStats = new UpdateMetrics();
        JsonArray accumulatedErrorReport = new JsonArray();
        InventoryUpdateOutcome compositeOutcome = new InventoryUpdateOutcome();
        plan.multipleSingleRecordUpserts(updateRequest, inventoryRecordSets).onComplete(
            listOfOutcomes -> {
              for (InventoryUpdateOutcome outcome : listOfOutcomes.result()) {
                if (outcome.hasMetrics()) {
                  accumulatedStats.add(outcome.getMetrics());
                }
                if (outcome.hasError()) {
                  accumulatedErrorReport.add(outcome.getError().asJson());
                }
              }
              compositeOutcome.setMetrics(accumulatedStats);
              compositeOutcome.setErrors(accumulatedErrorReport);
              compositeOutcome.setResponseStatusCode(accumulatedErrorReport.isEmpty() ? OK : MULTI_STATUS);
              promise.complete(compositeOutcome);
            });
      }
    });
    return promise.future();
  }

  // DELETE REQUESTS
  public void handleInventoryRecordSetDeleteByHRID(UpdateRequest request) {

    InventoryQuery queryByInstanceHrid = new QueryByHrid(request.bodyAsJson().getString("hrid"));
    DeletePlanAllHRIDs.getDeletionPlan(queryByInstanceHrid).runDeletionPlan(request)
        .onSuccess(outcome -> outcome.respond(request.routingContext()))
        .onFailure(outcome -> responseError(request.routingContext(), ErrorType.ANY, outcome.getCause()));
  }

  public void handleSharedInventoryRecordSetDeleteByIdentifiers(UpdateRequest request) {

    RecordIdentifiers deletionIdentifiers = RecordIdentifiers.identifiersFromDeleteRequestJson(request.bodyAsJson());
    DeletePlanSharedInventory.getDeletionPlan(deletionIdentifiers).runDeletionPlan(request)
        .onSuccess(outcome -> outcome.respond(request.routingContext()))
        .onFailure(outcome -> responseError(request.routingContext(), ErrorType.ANY, outcome.getCause()));
  }

}
