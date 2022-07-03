package org.folio.inventoryupdate;

import static org.folio.inventoryupdate.InventoryStorage.getOkapiClient;
import static org.folio.inventoryupdate.ErrorReport.*;
import static org.folio.inventoryupdate.InventoryUpdateOutcome.MULTI_STATUS;
import static org.folio.inventoryupdate.InventoryUpdateOutcome.OK;
import static org.folio.okapi.common.HttpResponse.responseError;
import static org.folio.okapi.common.HttpResponse.responseJson;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import org.folio.inventoryupdate.entities.RecordIdentifiers;
import org.folio.inventoryupdate.entities.InventoryRecordSet;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * MatchService looks for an Instance in Inventory that matches an incoming
 * Instance and either updates or creates an Instance based on the results.
 *
 */
public class InventoryUpdateService {
  private final Logger logger = LoggerFactory.getLogger("inventory-update");

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
    inventoryUpsertByHRID(routingContext, inventoryRecordSets).onComplete(update ->{
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
      inventoryUpsertByHRID(routingContext, inventoryRecordSets).onComplete(update -> {
        if (update.succeeded()) {
          update.result().respond(routingContext);
        } else {
          if (inventoryRecordSets.size() > 1) {
            logger.error("A batch update failed bringing down all records of the batch. Switching to record-by-record updates");
            UpdateMetrics accumulatedStats = new UpdateMetrics();
            JsonArray accumulatedErrorReport = new JsonArray();
            InventoryUpdateOutcome compositeOutcome = new InventoryUpdateOutcome();
            multipleSingleRecordUpsertsByHrid(routingContext, inventoryRecordSets).onComplete(
                    listOfOutcomes -> {
                      int i = 1;
                      for (InventoryUpdateOutcome outcome : listOfOutcomes.result()) {
                        if (outcome.hasMetrics()) {
                          accumulatedStats.add(outcome.metrics);
                        } else {
                          logger.info("Processing InventoryUpdateOutcome without metrics (?)");
                        }
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

  private Future<List<InventoryUpdateOutcome>> multipleSingleRecordUpsertsByHrid(RoutingContext routingContext, JsonArray inventoryRecordSets) {
    List<JsonArray> arraysOfOneRecordSet = new ArrayList<>();
    for (Object o : inventoryRecordSets) {
      JsonArray batchOfOne = new JsonArray().add(o);
      arraysOfOneRecordSet.add(batchOfOne);
    }
    return chainSingleRecordUpserts(routingContext, arraysOfOneRecordSet, this::inventoryUpsertByHRID);
  }

  private Future<List<InventoryUpdateOutcome>> chainSingleRecordUpserts(RoutingContext routingContext, List<JsonArray> arraysOfOneRecordSet, BiFunction<RoutingContext, JsonArray, Future<InventoryUpdateOutcome>> upsertMethod) {
    Promise<List<InventoryUpdateOutcome>> promise = Promise.promise();
    List<InventoryUpdateOutcome> outcomes = new ArrayList<>();
    Future<InventoryUpdateOutcome> fut = Future.succeededFuture();
    for (JsonArray arrayOfOneRecordSet : arraysOfOneRecordSet) {
      fut = fut.compose(v -> {
        // First time around, a null outcome is passed in
        if (v != null) outcomes.add(v);
        return upsertMethod.apply(routingContext, arrayOfOneRecordSet);
      });
    }
    fut.onComplete( result -> {
      // capture the last outcome too
      outcomes.add(result.result());
      promise.complete(outcomes);
    });
   return promise.future();
  }

  /**
   * @param inventoryRecordSets List of one or more record sets to be processed
   * @return The outcome of the planning and updating
   */
  private Future<InventoryUpdateOutcome> inventoryUpsertByHRID(RoutingContext routingContext, JsonArray inventoryRecordSets) {
    Promise<InventoryUpdateOutcome> promise = Promise.promise();
    UpdatePlanAllHRIDs plan = UpdatePlanAllHRIDs.getUpsertPlan();
    RequestValidation validations = validateIncomingRecordSets (plan, inventoryRecordSets);
    UpdatePlanAllHRIDs.checkForUniqueHRIDsInBatch(validations, inventoryRecordSets);
    final boolean batchOfOne = (inventoryRecordSets.size() == 1);

    if (validations.passed()) {
      plan.setIncomingRecordSets(inventoryRecordSets)
              .buildRepositoryFromStorage(routingContext).onComplete(
                      result -> {
                        if (result.succeeded()) {
                          plan.planInventoryUpdates()
                                  .doInventoryUpdates(
                                          getOkapiClient(routingContext)).onComplete(inventoryUpdated -> {

                                    JsonObject response = (batchOfOne ?
                                            plan.getOneUpdatingRecordSetJsonFromRepository() : 
                                            new JsonObject());

                                    if (inventoryUpdated.succeeded()) {
                                      response.put("metrics", plan.getUpdateStatsFromRepository());
                                      InventoryUpdateOutcome outcome =
                                              new InventoryUpdateOutcome(
                                                      response)
                                                      .setResponseStatusCode(OK);
                                      promise.complete(outcome);
                                    } else {
                                      logger.error("Update failed " +
                                              inventoryUpdated.cause().getMessage());
                                      ErrorReport report = ErrorReport
                                              .makeErrorReportFromJsonString(
                                                      inventoryUpdated.cause().getMessage());
                                      // If the error affected an entire batch of records
                                      // then fail the request as a whole. This particular error
                                      // message will not go to the client but rather be picked
                                      // up and acted upon by the module.
                                      //
                                      // If the error only affected individual records, either
                                      // because the update was for a batch of one or because the
                                      // error affected entities that are not batch updated
                                      // (ie relationships) then return a (partial) success,
                                      // a multi-status (207) that is.
                                      if (report.isBatchStorageError() && !batchOfOne) {
                                        // This will cause the controller to switch to record-by-record
                                        promise.fail(report.asJsonString());
                                      } else {
                                        response.put("metrics", plan.getUpdateStatsFromRepository());
                                        response.put("errors", plan.getErrorsUsingRepository());
                                        InventoryUpdateOutcome outcome =
                                                new InventoryUpdateOutcome(response)
                                                        .setResponseStatusCode(MULTI_STATUS);
                                        promise.complete(outcome);
                                      }
                                    }
                                  });
                        } else {
                          InventoryUpdateOutcome outcome = new InventoryUpdateOutcome(
                                  ErrorReport.makeErrorReportFromJsonString(
                                          result.cause().getMessage())
                                          .setShortMessage("Fetching from storage before update failed."));
                          promise.complete(outcome);
                        }
                      });
    } else {
      ErrorReport report = new ErrorReport(
              ErrorReport.ErrorCategory.VALIDATION,
              UNPROCESSABLE_ENTITY,
              validations.firstMessage())
              .setEntityType(validations.firstEntityType())
              .setEntity(validations.firstEntity())
              .setShortMessage(validations.firstShortMessage())
              .setDetails(validations.asJson());
      if (batchOfOne) {
        promise.complete(new InventoryUpdateOutcome(report));
      } else {
        // Pre-validation of batch of record sets failed, switch to record-by-record upsert
        // to process the good record sets, if any.
        promise.fail(report.asJsonString());
      }
    }
    return promise.future();
  }

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
      sharedInventoryUpsertByMatchKeyBatch(routingContext, inventoryRecordSets);

  }

  public void handleSharedInventoryUpsertByMatchKeyBatch(RoutingContext routingContext) {
    long start = System.currentTimeMillis();
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
    long gotJsonMs = System.currentTimeMillis() - start;
    logger.debug("Got input JSON in " + gotJsonMs + " ms.");
    if (incomingValidJson.getJson().containsKey("inventoryRecordSets")) {
      JsonArray inventoryRecordSets = incomingValidJson.getJson().getJsonArray("inventoryRecordSets");
      sharedInventoryUpsertByMatchKeyBatch(routingContext, inventoryRecordSets);
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


  private void sharedInventoryUpsertByMatchKeyBatch(RoutingContext routingContext, JsonArray inventoryRecordSets) {
      long siUpsertBatchStart = System.currentTimeMillis();
      UpdatePlanSharedInventory plan = UpdatePlanSharedInventory.getUpsertPlan();
      RequestValidation validations = validateIncomingRecordSets(plan, inventoryRecordSets);
      final boolean batchOfOne = (inventoryRecordSets.size()==1);
      if (validations.passed()) {
        plan.setIncomingRecordSets(inventoryRecordSets)
                .buildRepositoryFromStorage(routingContext).onComplete(result -> {
                  if (result.succeeded()) {
                    plan.planInventoryUpdates()
                            .doInventoryUpdates(getOkapiClient(routingContext))
                            .onComplete(inventoryUpdated -> {
                              if (inventoryUpdated.succeeded()) {
                                JsonObject pushedRecordSetWithStats = plan.getOneUpdatingRecordSetJsonFromRepository();
                                pushedRecordSetWithStats.put("metrics", plan.getUpdateStatsFromRepository());
                                long siUpsertDone = System.currentTimeMillis()-siUpsertBatchStart;
                                logger.debug("SI batch upsert done in " + siUpsertDone + " ms.");
                                respondWithOK(routingContext, pushedRecordSetWithStats);
                              } else {
                                JsonObject pushedRecordSetWithStats = plan.getOneUpdatingRecordSetJsonFromRepository();
                                pushedRecordSetWithStats.put("metrics", plan.getUpdateStatsFromRepository());
                                pushedRecordSetWithStats.put("errors", plan.getErrorsUsingRepository());


                                pushedRecordSetWithStats.getJsonArray("errors")
                                        .add(new ErrorReport(
                                                ErrorCategory.STORAGE,
                                                INTERNAL_SERVER_ERROR,
                                                inventoryUpdated.cause().getMessage())
                                                .asJson());

                                respondWithMultiStatus(routingContext,pushedRecordSetWithStats);
                              }
                            });
                  } else {
                    new ErrorReport(
                            ErrorCategory.STORAGE,
                            INTERNAL_SERVER_ERROR,
                            result.cause().getMessage())
                            .setShortMessage("Failed to fetch from storage to upsert by match-key.")
                            .respond(routingContext);
                  }
                });
      } else {
        new ErrorReport(
                ErrorCategory.VALIDATION,
                UNPROCESSABLE_ENTITY,
                validations.asJson())
                .setShortMessage(
                        "The incoming record set(s) had validation errors and were not processed ")
                .respond(routingContext);
      }
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
        //updatePlan.writePlanToLog();
        updatePlan.doInventoryDelete(okapiClient).onComplete(deletionsDone -> {
          JsonObject pushedRecordSetWithStats = updatePlan.getUpdatingRecordSetJson();
          pushedRecordSetWithStats.put("metrics", updatePlan.getUpdateStats());
          if (deletionsDone.succeeded()) {
            respondWithOK(routingCtx,pushedRecordSetWithStats);
          } else {
            pushedRecordSetWithStats.put("errors", updatePlan.getErrors());
            pushedRecordSetWithStats.getJsonArray("errors")
                    .add(new ErrorReport(
                            ErrorCategory.STORAGE,
                            INTERNAL_SERVER_ERROR,
                            deletionsDone.cause().getMessage())
                            .asJson());
            respondWithMultiStatus(routingCtx,pushedRecordSetWithStats);
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

  // TODO: move to UpdatePlan
  public static RequestValidation validateIncomingRecordSets (UpdatePlan plan, JsonArray incomingRecordSets) {
    RequestValidation validations = new RequestValidation();
    for (Object recordSetObject : incomingRecordSets) {
      RequestValidation validation = plan.validateIncomingRecordSet((JsonObject) recordSetObject);
      if (validation.hasErrors()) {
        validations.addValidation(validation);
      }
    }
    return validations;
  }

  public void respondWithOK(RoutingContext routingContext, JsonObject message) {
    responseJson(routingContext, OK).end(message.encodePrettily());
  }

  public void respondWithMultiStatus(RoutingContext routingContext, JsonObject message) {
    responseJson(routingContext, MULTI_STATUS).end(message.encodePrettily());
  }

}
