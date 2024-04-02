package org.folio.inventoryupdate;

import static org.folio.inventoryupdate.InventoryStorage.getOkapiClient;
import static org.folio.inventoryupdate.ErrorReport.*;
import static org.folio.inventoryupdate.InventoryUpdateOutcome.MULTI_STATUS;
import static org.folio.inventoryupdate.InventoryUpdateOutcome.OK;
import static org.folio.okapi.common.HttpResponse.responseError;
import static org.folio.okapi.common.HttpResponse.responseJson;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import org.folio.inventoryupdate.entities.RecordIdentifiers;
import org.folio.inventoryupdate.entities.InventoryRecordSet;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

/**
 * MatchService looks for an Instance in Inventory that matches an incoming
 * Instance and either updates or creates an Instance based on the results.
 *
 */
public class InventoryUpdateService {
  private static final Logger logger = LoggerFactory.getLogger("inventory-update");

  public void handleInventoryUpsertByHRID(RoutingContext routingContext) {
    UpdatePlan plan = new UpdatePlanAllHRIDs();
    doUpsert(routingContext, plan);
  }

  public void handleInventoryUpsertByHRIDBatch(RoutingContext routingContext) {
    UpdatePlan plan = new UpdatePlanAllHRIDs();
    doBatchUpsert(routingContext, plan);
  }

  public void handleSharedInventoryUpsertByMatchKey(RoutingContext routingContext) {
    UpdatePlan plan = new UpdatePlanSharedInventory();
    doUpsert(routingContext, plan);
  }

  public void handleSharedInventoryUpsertByMatchKeyBatch(RoutingContext routingContext) {
    UpdatePlan plan = new UpdatePlanSharedInventory();
    doBatchUpsert(routingContext, plan);
  }

  /**
   * Validates a single incoming record set and performs an upsert
   * @param plan a shared-inventory/matchKey, or an inventory/hrid upsert plan.
   */
  private void doUpsert(RoutingContext routingContext, UpdatePlan plan) {
    InventoryUpdateOutcome isJsonContentType = contentTypeIsJson(routingContext);
    if (isJsonContentType.failed()) {
      isJsonContentType.getErrorResponse().respond(routingContext);
      return;
    }
    InventoryUpdateOutcome incomingValidJson = getIncomingJsonBody(routingContext);
    if (incomingValidJson.failed()) {
      incomingValidJson.getErrorResponse().respond(routingContext);
      return;
    }
    InventoryUpdateOutcome validRecordSet = InventoryRecordSet.isValidInventoryRecordSet(incomingValidJson.result);
    if (validRecordSet.failed()) {
      validRecordSet.getErrorResponse().respond(routingContext);
      return;
    }
    JsonArray inventoryRecordSets = new JsonArray();
    inventoryRecordSets.add(new JsonObject(incomingValidJson.result.encodePrettily()));
    plan.upsertBatch(routingContext, inventoryRecordSets).onComplete(update ->{
      if (update.succeeded()) {
        if (update.result().statusCode == OK || update.result().statusCode == MULTI_STATUS) {
          responseJson(routingContext, update.result().statusCode).end(update.result().getJson().encodePrettily());
        } else {
          update.result().getErrorResponse().respond(routingContext);
        }
      }
    });
  }

  /**
   * Validates a batch of incoming record sets and performs a batch-upsert
   * @param plan a shared-inventory/matchKey, or an inventory/hrid upsert plan.
   */
  private void doBatchUpsert(RoutingContext routingContext, UpdatePlan plan) {
    InventoryUpdateOutcome incomingValidJson = getIncomingJsonBody(routingContext);

    if (incomingValidJson.failed()) {
      incomingValidJson.getErrorResponse().respond(routingContext);
      return;
    }
    if (incomingValidJson.getJson().containsKey("inventoryRecordSets")) {
      JsonArray inventoryRecordSets = incomingValidJson.getJson().getJsonArray("inventoryRecordSets");
      plan.upsertBatch(routingContext, inventoryRecordSets).onComplete(update -> {
        // The upsert could succeed, but with an error report, if it was a batch of one
        // Only if a true batch upsert (of more than one) failed, will the promise fail.
        if (update.succeeded()) {
          update.result().respond(routingContext);
        } else {
          logger.error("A batch upsert failed, bringing down all records of the batch. Switching to record-by-record updates");
          UpdateMetrics accumulatedStats = new UpdateMetrics();
          JsonArray accumulatedErrorReport = new JsonArray();
          InventoryUpdateOutcome compositeOutcome = new InventoryUpdateOutcome();
          plan.multipleSingleRecordUpserts(routingContext, inventoryRecordSets).onComplete(
                  listOfOutcomes -> {
                    for (InventoryUpdateOutcome outcome : listOfOutcomes.result()) {
                      if (outcome.hasMetrics()) {
                        accumulatedStats.add(outcome.metrics);
                      }
                      if (outcome.hasError()) {
                        accumulatedErrorReport.add(outcome.getError().asJson());
                      }
                    }
                    compositeOutcome.setMetrics(accumulatedStats);
                    compositeOutcome.setErrors(accumulatedErrorReport);
                    compositeOutcome.setResponseStatusCode(accumulatedErrorReport.isEmpty() ? OK : MULTI_STATUS);
                    compositeOutcome.respond(routingContext);
                  });
        }
      });
    } else {
      new ErrorReport(
              ErrorCategory.VALIDATION,
              BAD_REQUEST,
              "Did not recognize request body as a batch of Inventory record sets")
              .setShortMessage("Not a batch of Inventory record sets")
              .setRequestJson(incomingValidJson.getJson())
              .respond(routingContext);
    }
  }

  // DELETE REQUESTS
  public void handleInventoryRecordSetDeleteByHRID(RoutingContext routingCtx) {
    InventoryUpdateOutcome isJsonContentType = contentTypeIsJson(routingCtx);
    if (isJsonContentType.failed()) {
      isJsonContentType.getErrorResponse().respond(routingCtx);
      return;
    }

    InventoryUpdateOutcome incomingValidJson =  getIncomingJsonBody(routingCtx);
    JsonObject processing = incomingValidJson.getJson().getJsonObject( "processing" );
    ProcessingInstructionsDeletion deleteInstructions =  new ProcessingInstructionsDeletion(processing);

    if (incomingValidJson.failed()) {
      incomingValidJson.getErrorResponse().respond(routingCtx);
      return;
    }

    InventoryQuery queryByInstanceHrid = new QueryByHrid(incomingValidJson.getJson().getString("hrid"));
    UpdatePlan updatePlan = UpdatePlanAllHRIDs.getDeletionPlan(queryByInstanceHrid);
    runDeletionPlan(updatePlan, deleteInstructions, routingCtx);
  }

  public void handleSharedInventoryRecordSetDeleteByIdentifiers(RoutingContext routingContext) {
    logger.debug("Handling delete request for shared index " + routingContext.getBodyAsString());
    InventoryUpdateOutcome isJsonContentType = contentTypeIsJson(routingContext);
    if (isJsonContentType.failed()) {
      isJsonContentType.getErrorResponse().respond(routingContext);
      return;
    }

    InventoryUpdateOutcome incomingValidJson = getIncomingJsonBody(routingContext);
    if (incomingValidJson.failed()) {
      incomingValidJson.getErrorResponse().respond(routingContext);
      return;
    }
    JsonObject processing = incomingValidJson.getJson().getJsonObject( "processing" );
    ProcessingInstructionsDeletion deleteInstructions =  new ProcessingInstructionsDeletion(processing);

    RecordIdentifiers deletionIdentifiers = RecordIdentifiers.identifiersFromDeleteRequestJson(incomingValidJson.getJson());
    UpdatePlan updatePlan = UpdatePlanSharedInventory.getDeletionPlan(deletionIdentifiers);
    runDeletionPlan(updatePlan, deleteInstructions, routingContext);
  }

  private void runDeletionPlan(UpdatePlan updatePlan, ProcessingInstructionsDeletion deleteInstructions, RoutingContext routingCtx) {

    OkapiClient okapiClient = getOkapiClient(routingCtx);
    updatePlan.planInventoryDelete(okapiClient, deleteInstructions).onComplete(planDone -> {
      if (planDone.succeeded()) {
          updatePlan.doInventoryDelete(okapiClient).onComplete(deletionsDone -> {
          JsonObject response = new JsonObject();
          response.put("metrics", updatePlan.getUpdateStats());
          if (deletionsDone.succeeded()) {
            respondWithOK(routingCtx,response);
          } else {
            response.put("errors", updatePlan.getErrors());
            response.getJsonArray("errors")
                    .add(new ErrorReport(
                            ErrorCategory.STORAGE,
                            INTERNAL_SERVER_ERROR,
                            deletionsDone.cause().getMessage())
                            .asJson());
            respondWithMultiStatus(routingCtx,response);
          }
        });
      }  else {
        if (updatePlan.isDeletion && !updatePlan.foundExistingRecordSet()) {
          new ErrorReport(
                  ErrorReport.ErrorCategory.STORAGE,
                  NOT_FOUND,
                  "Error processing delete request: "+ planDone.cause().getMessage())
                  .respond(routingCtx);
        } else {
          new ErrorReport(
                  ErrorReport.ErrorCategory.STORAGE,
                  INTERNAL_SERVER_ERROR,
                  planDone.cause().getMessage())
                  .respond(routingCtx);
        }
      }
    });
  }

  // UTILS
  public void handleHealthCheck(RoutingContext routingContext) {
    responseJson(routingContext, OK).end("{ \"status\": \"UP\" }");
  }

  public void handleUnrecognizedPath(RoutingContext routingContext) {
    responseError(routingContext, NOT_FOUND, "No Service found for requested path " + routingContext.request().path());
  }

  private InventoryUpdateOutcome contentTypeIsJson (RoutingContext routingCtx) {
    String contentType = routingCtx.request().getHeader("Content-Type");
    if (contentType != null && !contentType.startsWith("application/json")) {
      logger.error("Only accepts Content-Type application/json, was: " + contentType);
      return new InventoryUpdateOutcome(new ErrorReport(
              ErrorReport.ErrorCategory.VALIDATION,
              BAD_REQUEST,
              "Only accepts Content-Type application/json, content type was: "+ contentType)
              .setShortMessage("Only accepts application/json")
              .setStatusCode(BAD_REQUEST));
    } else {
      return new InventoryUpdateOutcome(new JsonObject().put("succeeded", "yes"));
    }
  }

  private InventoryUpdateOutcome getIncomingJsonBody(RoutingContext routingCtx) {
    String bodyAsString;
    try {
      bodyAsString = routingCtx.getBodyAsString("UTF-8");
      if (bodyAsString == null) {
        return new InventoryUpdateOutcome(
                new ErrorReport(
                        ErrorReport.ErrorCategory.VALIDATION,
                        BAD_REQUEST,
                        "No request body provided."));
      }
      JsonObject bodyAsJson = new JsonObject(bodyAsString);
      logger.debug("Request body " + bodyAsJson.encodePrettily());
      return new InventoryUpdateOutcome(bodyAsJson);
    } catch (DecodeException de) {
      return new InventoryUpdateOutcome(new ErrorReport(ErrorReport.ErrorCategory.VALIDATION,
              BAD_REQUEST,
              "Could not parse JSON body of the request: " + de.getMessage())
              .setShortMessage("Could not parse request JSON"));
    }
  }

  public void respondWithOK(RoutingContext routingContext, JsonObject message) {
    responseJson(routingContext, OK).end(message.encodePrettily());
  }

  public void respondWithMultiStatus(RoutingContext routingContext, JsonObject message) {
    responseJson(routingContext, MULTI_STATUS).end(message.encodePrettily());
  }

}
