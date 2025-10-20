package org.folio.inventoryupdate.updating.service;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.updating.DeletePlan;
import org.folio.inventoryupdate.updating.DeletePlanAllHRIDs;
import org.folio.inventoryupdate.updating.DeletePlanSharedInventory;
import org.folio.inventoryupdate.updating.ErrorReport;
import org.folio.inventoryupdate.updating.InventoryQuery;
import org.folio.inventoryupdate.updating.InventoryUpdateOutcome;
import org.folio.inventoryupdate.updating.QueryByHrid;
import org.folio.inventoryupdate.updating.UpdateMetrics;
import org.folio.inventoryupdate.updating.UpdatePlan;
import org.folio.inventoryupdate.updating.UpdatePlanAllHRIDs;
import org.folio.inventoryupdate.updating.UpdatePlanSharedInventory;
import org.folio.inventoryupdate.updating.UpdateRequest;
import org.folio.inventoryupdate.updating.entities.RecordIdentifiers;
import org.folio.inventoryupdate.updating.instructions.ProcessingInstructionsDeletion;
import org.folio.okapi.common.OkapiClient;

import static org.folio.inventoryupdate.updating.ErrorReport.INTERNAL_SERVER_ERROR;
import static org.folio.inventoryupdate.updating.ErrorReport.NOT_FOUND;
import static org.folio.inventoryupdate.updating.InventoryStorage.getOkapiClient;
import static org.folio.inventoryupdate.updating.InventoryUpdateOutcome.MULTI_STATUS;
import static org.folio.inventoryupdate.updating.InventoryUpdateOutcome.OK;

public class HandlersUpdating {

  public static final Logger logger = LogManager.getLogger("inventory-update");

  public void handleInventoryUpsertByHRID(UpdateRequest updateRequest) {
    UpdatePlan plan = new UpdatePlanAllHRIDs();
    doUpsert(updateRequest, plan)
        .onComplete(outcome -> outcome.result().respond(updateRequest.routingContext()));
  }

  public void handleInventoryUpsertByHRIDBatch(UpdateRequest request) {
    UpdatePlan plan = new UpdatePlanAllHRIDs();
    doBatchUpsert(request, plan)
        .onComplete(outcome -> outcome.result().respond(request.routingContext()));
  }

  public void handleSharedInventoryUpsertByMatchKey(UpdateRequest request) {
    UpdatePlan plan = new UpdatePlanSharedInventory();
    doUpsert(request, plan)
        .onComplete(outcome -> outcome.result().respond(request.routingContext()));
  }

  public void handleSharedInventoryUpsertByMatchKeyBatch(UpdateRequest request) {
    UpdatePlan plan = new UpdatePlanSharedInventory();
    doBatchUpsert(request, plan)
        .onComplete(outcome -> outcome.result().respond(request.routingContext()));
  }


  /**
   * Validates a single incoming record set and performs an upsert
   * @param plan a shared-inventory/matchKey, or an inventory/hrid upsert plan.
   */
  private Future<InventoryUpdateOutcome> doUpsert(UpdateRequest request, UpdatePlan plan) {
    JsonArray inventoryRecordSets = new JsonArray();
    inventoryRecordSets.add(request.bodyAsJson());
    return plan.upsertBatch(request.routingContext(), inventoryRecordSets)
        .onComplete(outcome -> outcome.result().respond(request.routingContext()));
  }

  /**
   * Validates a batch of incoming record sets and performs a batch-upsert
   * @param plan a shared-inventory/matchKey, or an inventory/hrid upsert plan.
   */
  private Future<InventoryUpdateOutcome> doBatchUpsert(UpdateRequest updateRequest, UpdatePlan plan) {
    JsonArray inventoryRecordSets = updateRequest.bodyAsJson().getJsonArray("inventoryRecordSets");
    Promise<InventoryUpdateOutcome> promise = Promise.promise();
    plan.upsertBatch(updateRequest.routingContext(), inventoryRecordSets).onComplete(update -> {
      // The upsert could succeed, but with an error report, if it was a batch of one
      // Only if a true batch upsert (of more than one) failed, will the promise fail.
      if (update.succeeded()) {
        promise.complete(update.result());
      } else {
        logger.error("A batch upsert failed, bringing down all records of the batch. Switching to record-by-record updates");
        UpdateMetrics accumulatedStats = new UpdateMetrics();
        JsonArray accumulatedErrorReport = new JsonArray();
        InventoryUpdateOutcome compositeOutcome = new InventoryUpdateOutcome();
        plan.multipleSingleRecordUpserts(updateRequest.routingContext(), inventoryRecordSets).andThen(
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
    ProcessingInstructionsDeletion deleteInstructions =  new ProcessingInstructionsDeletion(request.bodyAsJson().getJsonObject("processing"));

    InventoryQuery queryByInstanceHrid = new QueryByHrid(request.bodyAsJson().getString("hrid"));
    DeletePlan deletePlan = DeletePlanAllHRIDs.getDeletionPlan(queryByInstanceHrid);
    runDeletionPlan(deletePlan, deleteInstructions, request.routingContext())
        .onComplete(outcome -> outcome.result().respond(request.routingContext()));
  }

  public void handleSharedInventoryRecordSetDeleteByIdentifiers(UpdateRequest request) {

    JsonObject processing = request.bodyAsJson().getJsonObject( "processing" );
    ProcessingInstructionsDeletion deleteInstructions =  new ProcessingInstructionsDeletion(processing);

    RecordIdentifiers deletionIdentifiers = RecordIdentifiers.identifiersFromDeleteRequestJson(request.bodyAsJson());
    DeletePlan deletePlan = DeletePlanSharedInventory.getDeletionPlan(deletionIdentifiers);
    runDeletionPlan(deletePlan, deleteInstructions, request.routingContext())
        .onComplete(outcome -> outcome.result().respond(request.routingContext()));
  }

  private Future<InventoryUpdateOutcome> runDeletionPlan(DeletePlan deletePlan, ProcessingInstructionsDeletion deleteInstructions, RoutingContext routingCtx) {
    Promise<InventoryUpdateOutcome> promise = Promise.promise();
    OkapiClient okapiClient = getOkapiClient(routingCtx);
    deletePlan.planInventoryDelete(okapiClient, deleteInstructions).onComplete(planDone -> {
      if (planDone.succeeded()) {
        deletePlan.doInventoryDelete(okapiClient).onComplete(deletionsDone -> {
          JsonObject response = new JsonObject();
          response.put("metrics", deletePlan.getUpdateStats());
          InventoryUpdateOutcome outcome = new InventoryUpdateOutcome(response);
          if (deletionsDone.succeeded()) {
            outcome.setResponseStatusCode(OK).respond(routingCtx);
          } else {
            JsonArray errors = deletePlan.getErrors();
            errors.add(new ErrorReport(
                    ErrorReport.ErrorCategory.STORAGE,
                    INTERNAL_SERVER_ERROR,
                    deletionsDone.cause().getMessage())
                    .asJson());
            outcome.setErrors(errors)
                .setResponseStatusCode(MULTI_STATUS);
            promise.complete(outcome);
          }
        });
      } else {
        if (!deletePlan.foundExistingRecordSet()) {
          InventoryUpdateOutcome outcome = new InventoryUpdateOutcome(new ErrorReport(
              ErrorReport.ErrorCategory.STORAGE,
              NOT_FOUND,
              "Error processing delete request: "+ planDone.cause().getMessage()))
              .setResponseStatusCode(NOT_FOUND);
              promise.complete(outcome);
        } else {
          InventoryUpdateOutcome outcome = new InventoryUpdateOutcome(new ErrorReport(
              ErrorReport.ErrorCategory.STORAGE,
              INTERNAL_SERVER_ERROR,
              planDone.cause().getMessage()))
              .setResponseStatusCode(INTERNAL_SERVER_ERROR);
          promise.complete(outcome);
        }

      }
    });
    return promise.future();
  }

}
