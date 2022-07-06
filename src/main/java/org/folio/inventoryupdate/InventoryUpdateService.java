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

  private static final String LF = System.lineSeparator();

  // INVENTORY UPSERT BY HRID
  public void handleInventoryUpsertByHRID(RoutingContext routingContext) {
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
    UpdatePlan plan = new UpdatePlanAllHRIDs();
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

  public void handleInventoryUpsertByHRIDBatch(RoutingContext routingContext) {
    InventoryUpdateOutcome incomingValidJson = getIncomingJsonBody(routingContext);

    if (incomingValidJson.failed()) {
      incomingValidJson.getErrorResponse().respond(routingContext);
      return;
    }
    if (incomingValidJson.getJson().containsKey("inventoryRecordSets")) {
      JsonArray inventoryRecordSets = incomingValidJson.getJson().getJsonArray("inventoryRecordSets");
      UpdatePlanAllHRIDs plan = new UpdatePlanAllHRIDs();
      plan.upsertBatch(routingContext, inventoryRecordSets).onComplete(update -> {
        if (update.succeeded()) {
          update.result().respond(routingContext);
        } else {
          if (inventoryRecordSets.size() > 1) {
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
                      compositeOutcome.setResponseStatusCode(MULTI_STATUS);
                      compositeOutcome.respond(routingContext);
                    });
          }
        }
      });
    } else {
      new ErrorReport(
              ErrorReport.ErrorCategory.VALIDATION,
              BAD_REQUEST,
              "Did not recognize request body as a batch of Inventory record sets")
              .setShortMessage("Not a batch of Inventory record sets")
              .setRequestJson(incomingValidJson.getJson())
              .respond(routingContext);
    }
  }

  // INVENTORY UPSERT BY MATCH KEY
  public void handleSharedInventoryUpsertByMatchKey(RoutingContext routingContext) {
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

      // Make a batch of one
      JsonArray inventoryRecordSets = new JsonArray();
      inventoryRecordSets.add(new JsonObject(incomingValidJson.getJson().encodePrettily()));
      //sharedInventoryUpsertByMatchKeyBatch(routingContext, inventoryRecordSets);
      UpdatePlan plan = new UpdatePlanSharedInventory();
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

  public void handleSharedInventoryUpsertByMatchKeyBatch(RoutingContext routingContext) {
    InventoryUpdateOutcome incomingValidJson = getIncomingJsonBody(routingContext);

    if (incomingValidJson.failed()) {
      incomingValidJson.getErrorResponse().respond(routingContext);
      return;
    }
    if (incomingValidJson.getJson().containsKey("inventoryRecordSets")) {
      JsonArray inventoryRecordSets = incomingValidJson.getJson().getJsonArray("inventoryRecordSets");
      UpdatePlan plan = new UpdatePlanSharedInventory();
      plan.upsertBatch(routingContext, inventoryRecordSets).onComplete(update -> {
        if (update.succeeded()) {
          update.result().respond(routingContext);
        } else {
          if (inventoryRecordSets.size() > 1) {
            logger.error("A batch update failed bringing down all records of the batch. Switching to record-by-record updates");
            UpdateMetrics accumulatedStats = new UpdateMetrics();
            JsonArray accumulatedErrorReport = new JsonArray();
            InventoryUpdateOutcome compositeOutcome = new InventoryUpdateOutcome();
            plan.multipleSingleRecordUpserts(routingContext, inventoryRecordSets).onComplete(
                    listOfOutcomes -> {
                      for (InventoryUpdateOutcome outcome : listOfOutcomes.result()) {
                        outcome.setMetrics(plan.getUpdateMetricsFromRepository());
                        if (outcome.hasErrors()) {
                          accumulatedErrorReport.addAll(outcome.getErrorsAsJsonArray());
                        }
                      }
                      compositeOutcome.setMetrics(accumulatedStats);
                      compositeOutcome.setErrors(accumulatedErrorReport);
                      compositeOutcome.setResponseStatusCode(MULTI_STATUS);
                      compositeOutcome.respond(routingContext);
                    });
          }
        }
      });
    } else {
      new ErrorReport(
              ErrorReport.ErrorCategory.VALIDATION,
              BAD_REQUEST,
              "Did not recognize request body as a batch of Inventory record sets")
              .setShortMessage("Not a batch of Inventory record sets")
              .setEntity(incomingValidJson.getJson())
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

    if (incomingValidJson.failed()) {
      incomingValidJson.getErrorResponse().respond(routingCtx);
      return;
    }

    InventoryQuery queryByInstanceHrid = new QueryByHrid(incomingValidJson.getJson().getString("hrid"));
    UpdatePlan updatePlan = UpdatePlanAllHRIDs.getDeletionPlan(queryByInstanceHrid);
    runDeletionPlan(updatePlan, routingCtx);
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

    RecordIdentifiers deletionIdentifiers = RecordIdentifiers.identifiersFromDeleteRequestJson(incomingValidJson.getJson());
    UpdatePlan updatePlan = UpdatePlanSharedInventory.getDeletionPlan(deletionIdentifiers);
    runDeletionPlan(updatePlan, routingContext);
  }

  private void runDeletionPlan(UpdatePlan updatePlan, RoutingContext routingCtx) {

    OkapiClient okapiClient = getOkapiClient(routingCtx);
    updatePlan.planInventoryDelete(okapiClient).onComplete(planDone -> {
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
           //   .respond(routingCtx);
    }
  }

  public void respondWithOK(RoutingContext routingContext, JsonObject message) {
    responseJson(routingContext, OK).end(message.encodePrettily());
  }

  public void respondWithMultiStatus(RoutingContext routingContext, JsonObject message) {
    responseJson(routingContext, MULTI_STATUS).end(message.encodePrettily());
  }

}
