package org.folio.inventoryupdate.updating.service;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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

import static org.folio.inventoryupdate.updating.ErrorReport.INTERNAL_SERVER_ERROR;
import static org.folio.inventoryupdate.updating.ErrorReport.NOT_FOUND;
import static org.folio.inventoryupdate.updating.InventoryUpdateOutcome.MULTI_STATUS;
import static org.folio.inventoryupdate.updating.InventoryUpdateOutcome.OK;
import static org.folio.okapi.common.HttpResponse.responseJson;


public class HandlersUpdating {

  public static final Logger logger = LogManager.getLogger("inventory-update");

  public void handleInventoryUpsertByHRID(UpdateRequest updateRequest) {
    UpdatePlan plan = new UpdatePlanAllHRIDs();
    doUpsert(updateRequest, plan);
  }

  public void handleInventoryUpsertByHRIDBatch(UpdateRequest request) {
    UpdatePlan plan = new UpdatePlanAllHRIDs();
    doBatchUpsert(request, plan);
  }

  public void handleSharedInventoryUpsertByMatchKey(UpdateRequest request) {
    UpdatePlan plan = new UpdatePlanSharedInventory();
    doUpsert(request, plan);
  }

  public void handleSharedInventoryUpsertByMatchKeyBatch(UpdateRequest request) {
    UpdatePlan plan = new UpdatePlanSharedInventory();
    doBatchUpsert(request, plan);
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
  private void doBatchUpsert(UpdateRequest updateRequest, UpdatePlan plan) {
    JsonArray inventoryRecordSets = updateRequest.bodyAsJson().getJsonArray("inventoryRecordSets");
    plan.upsertBatch(updateRequest, inventoryRecordSets).onComplete(update -> {
      // The upsert could succeed, but with an error report, if it was a batch of one
      // Only if a true batch upsert (of more than one) failed, will the promise fail.
      if (update.succeeded()) {
        update.result().respond(updateRequest.routingContext());
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
              compositeOutcome.respond(updateRequest.routingContext());
            });
      }
    });
  }

  // DELETE REQUESTS
  public void handleInventoryRecordSetDeleteByHRID(UpdateRequest request) {
    ProcessingInstructionsDeletion deleteInstructions =  new ProcessingInstructionsDeletion(request.bodyAsJson().getJsonObject("processing"));

    InventoryQuery queryByInstanceHrid = new QueryByHrid(request.bodyAsJson().getString("hrid"));
    DeletePlan deletePlan = DeletePlanAllHRIDs.getDeletionPlan(queryByInstanceHrid);
    runDeletionPlan(deletePlan, deleteInstructions, request);
  }

  public void handleSharedInventoryRecordSetDeleteByIdentifiers(UpdateRequest request) {

    JsonObject processing = request.bodyAsJson().getJsonObject( "processing" );
    ProcessingInstructionsDeletion deleteInstructions =  new ProcessingInstructionsDeletion(processing);

    RecordIdentifiers deletionIdentifiers = RecordIdentifiers.identifiersFromDeleteRequestJson(request.bodyAsJson());
    DeletePlan deletePlan = DeletePlanSharedInventory.getDeletionPlan(deletionIdentifiers);
    runDeletionPlan(deletePlan, deleteInstructions, request);
  }

  private void runDeletionPlan(DeletePlan deletePlan, ProcessingInstructionsDeletion deleteInstructions, UpdateRequest request) {


    deletePlan.planInventoryDelete(request.getOkapiClient(), deleteInstructions).onComplete(planDone -> {
      if (planDone.succeeded()) {
        deletePlan.doInventoryDelete(request.getOkapiClient()).onComplete(deletionsDone -> {
          JsonObject response = new JsonObject();
          response.put("metrics", deletePlan.getUpdateStats());
          if (deletionsDone.succeeded()) {
            respondWithOK(request,response);
          } else {
            response.put("errors", deletePlan.getErrors());
            response.getJsonArray("errors")
                .add(new ErrorReport(
                    ErrorReport.ErrorCategory.STORAGE,
                    INTERNAL_SERVER_ERROR,
                    deletionsDone.cause().getMessage())
                    .asJson());
            respondWithMultiStatus(request,response);
          }
        });
      } else {
        if (!deletePlan.foundExistingRecordSet()) {
          new ErrorReport(
              ErrorReport.ErrorCategory.STORAGE,
              NOT_FOUND,
              "Error processing delete request: "+ planDone.cause().getMessage())
              .respond(request.routingContext());
        } else {
          new ErrorReport(
              ErrorReport.ErrorCategory.STORAGE,
              INTERNAL_SERVER_ERROR,
              planDone.cause().getMessage())
              .respond(request.routingContext());
        }

      }
    });
  }
  public void respondWithOK(UpdateRequest request, JsonObject message) {
    responseJson(request.routingContext(), OK).end(message.encodePrettily());
  }

  public void respondWithMultiStatus(UpdateRequest request, JsonObject message) {
    responseJson(request.routingContext(), MULTI_STATUS).end(message.encodePrettily());
  }
}
