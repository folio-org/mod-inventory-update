package org.folio.inventoryupdate;

import static org.folio.inventoryupdate.InventoryStorage.getOkapiClient;
import static org.folio.inventoryupdate.ErrorResponse.*;
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
  private final Logger logger = LoggerFactory.getLogger("inventory-update");

  private static final String LF = System.lineSeparator();
  private static final int OK = 200;

  private static final int MULTI_STATUS = 207;

  // INVENTORY UPSERT BY HRID
  public void handleInventoryUpsertByHRID(RoutingContext routingCtx) {
    if (! contentTypeIsJson(routingCtx)) {
      return;
    }
    JsonObject incomingJson = getIncomingJsonBody(routingCtx);
    if (incomingJson == null) {
      return;
    }
    if (! InventoryRecordSet.isValidInventoryRecordSet(incomingJson)) {
      new ErrorResponse(
              ErrorResponse.ErrorCategory.VALIDATION,
              BAD_REQUEST,
              "Did not recognize input as an Inventory record set")
              .setShortMessage("Not an Inventory record set.")
              .setEntity(incomingJson)
              .respond(routingCtx);
      return;
    }
    JsonArray inventoryRecordSets = new JsonArray();
    inventoryRecordSets.add(new JsonObject(incomingJson.encodePrettily()));
    inventoryUpsertByHRIDBatch(routingCtx, inventoryRecordSets);
  }

  public void handleInventoryUpsertByHRIDBatch(RoutingContext routingContext) {
    JsonObject requestJson = getIncomingJsonBody(routingContext);
    if (requestJson == null) {
      return;
    }
    if (requestJson.containsKey("inventoryRecordSets")) {
      JsonArray inventoryRecordSets = requestJson.getJsonArray("inventoryRecordSets");
      inventoryUpsertByHRIDBatch(routingContext, inventoryRecordSets);
    } else {
      new ErrorResponse(
              ErrorResponse.ErrorCategory.VALIDATION,
              BAD_REQUEST,
              "Did not recognize request body as a batch of Inventory record sets")
              .setShortMessage("Not a batch of Inventory record sets")
              .setEntity(requestJson)
              .respond(routingContext);
    }
  }

  private void inventoryUpsertByHRIDBatch(RoutingContext routingContext, JsonArray inventoryRecordSets) {
    UpdatePlanAllHRIDs plan = UpdatePlanAllHRIDs.getUpsertPlan();
    RequestValidation validations = validateIncomingRecordSets (plan, inventoryRecordSets);
    UpdatePlanAllHRIDs.checkForUniqueHRIDsInBatch(validations, inventoryRecordSets);

    if (validations.passed()) {
      plan
              .setIncomingRecordSets(inventoryRecordSets)
              .buildRepositoryFromStorage(routingContext).onComplete(
                      result -> {
                        if (result.succeeded()) {
                          plan.planInventoryUpdates()
                                  .doInventoryUpdates(
                                          getOkapiClient(routingContext)).onComplete(inventoryUpdated -> {
                                    if (inventoryUpdated.succeeded()) {
                                      JsonObject pushedRecordSetWithStats = plan.getUpdatingRecordSetJsonFromRepository();
                                      pushedRecordSetWithStats.put("metrics", plan.getUpdateStatsFromRepository());
                                      respondWithOK(routingContext, pushedRecordSetWithStats);
                                    } else {
                                      logger.error("Update failed " + plan.getErrorsUsingRepository());
                                      JsonObject pushedRecordSetWithStats = plan.getUpdatingRecordSetJsonFromRepository();
                                      pushedRecordSetWithStats.put("metrics", plan.getUpdateStatsFromRepository());
                                      pushedRecordSetWithStats.put("errors", plan.getErrorsUsingRepository());
                                      // TODO: switch to single record upserts

                                      // TODO: not this error response yet:
                                      respondWithMultiStatus(routingContext,pushedRecordSetWithStats);
                                    }
                                  });
                        } else {
                          new ErrorResponse(
                                  ErrorResponse.ErrorCategory.STORAGE,
                                  INTERNAL_SERVER_ERROR,
                                  result.cause().getMessage())
                                  .setShortMessage("Fetching from storage before update failed.")
                                  .respond(routingContext);
                        }
                      });
    } else {
      // TODO: switch to single record upserts

      // TODO: not this error response yet:
      new ErrorResponse(
              ErrorResponse.ErrorCategory.VALIDATION,
              UNPROCESSABLE_ENTITY,
              validations.firstMessage())
              .setEntityType(validations.firstEntityType())
              .setEntity(validations.firstEntity())
              .setShortMessage(validations.firstShortMessage())
              .setDetails(validations.asJson())
              .respond(routingContext);
    }
  }

  public void handleInventoryRecordSetDeleteByHRID(RoutingContext routingCtx) {
    if (contentTypeIsJson(routingCtx)) {
      JsonObject deletionJson = getIncomingJsonBody(routingCtx);
      InventoryQuery queryByInstanceHrid = new QueryByHrid(deletionJson.getString("hrid"));
      UpdatePlan updatePlan = UpdatePlanAllHRIDs.getDeletionPlan(queryByInstanceHrid);
      runDeletionPlan(updatePlan, routingCtx);
    }
  }


  // INVENTORY UPSERT BY MATCH KEY
  public void handleSharedInventoryUpsertByMatchKey(RoutingContext routingContext) {
    try {
      if (contentTypeIsJson(routingContext)) {
        JsonObject incomingJson = getIncomingJsonBody(routingContext);
        if (incomingJson == null) {
          return;
        }
        // Fake a batch of one for now
        JsonArray inventoryRecordSets = new JsonArray();
        inventoryRecordSets.add(new JsonObject(incomingJson.encodePrettily()));
        sharedInventoryUpsertByMatchKeyBatch(routingContext, inventoryRecordSets);
      }
    } catch (NullPointerException npe) {
      npe.printStackTrace();
      new ErrorResponse(
              ErrorResponse.ErrorCategory.INTERNAL,
              INTERNAL_SERVER_ERROR,
              "Application had a null pointer exception.")
              .respond(routingContext);
    } catch (Exception e) {
      e.printStackTrace();
      new ErrorResponse(
              ErrorResponse.ErrorCategory.INTERNAL,
              INTERNAL_SERVER_ERROR,
              "Application had an exception.")
              .respond(routingContext);
    }
  }

  public void handleSharedInventoryUpsertByMatchKeyBatch(RoutingContext routingContext) {
    long start = System.currentTimeMillis();
    JsonObject requestJson = getIncomingJsonBody(routingContext);
    if (requestJson == null) {
      return;
    }
    long gotJsonMs = System.currentTimeMillis() - start;
    logger.debug("Got input JSON in " + gotJsonMs + " ms.");
    if (requestJson.containsKey("inventoryRecordSets")) {
      JsonArray inventoryRecordSets = requestJson.getJsonArray("inventoryRecordSets");
      sharedInventoryUpsertByMatchKeyBatch(routingContext, inventoryRecordSets);
    } else {
      new ErrorResponse(
              ErrorResponse.ErrorCategory.VALIDATION,
              BAD_REQUEST,
              "Did not recognize request body as a batch of Inventory record sets")
              .setShortMessage("Not a batch of Inventory record sets")
              .setEntity(requestJson)
              .respond(routingContext);
    }
  }


  private void sharedInventoryUpsertByMatchKeyBatch(RoutingContext routingContext, JsonArray inventoryRecordSets) {
      long siUpsertBatchStart = System.currentTimeMillis();
      UpdatePlanSharedInventory plan = UpdatePlanSharedInventory.getUpsertPlan();
      RequestValidation validations = validateIncomingRecordSets(plan, inventoryRecordSets);
      if (validations.passed()) {
        plan.setIncomingRecordSets(inventoryRecordSets)
                .buildRepositoryFromStorage(routingContext).onComplete(result -> {
                  if (result.succeeded()) {
                    plan.planInventoryUpdates()
                            .doInventoryUpdates(getOkapiClient(routingContext))
                            .onComplete(inventoryUpdated -> {
                              if (inventoryUpdated.succeeded()) {
                                JsonObject pushedRecordSetWithStats = plan.getUpdatingRecordSetJsonFromRepository();
                                pushedRecordSetWithStats.put("metrics", plan.getUpdateStatsFromRepository());
                                long siUpsertDone = System.currentTimeMillis()-siUpsertBatchStart;
                                logger.debug("SI batch upsert done in " + siUpsertDone + " ms.");
                                respondWithOK(routingContext, pushedRecordSetWithStats);
                              } else {
                                JsonObject pushedRecordSetWithStats = plan.getUpdatingRecordSetJsonFromRepository();
                                pushedRecordSetWithStats.put("metrics", plan.getUpdateStatsFromRepository());
                                pushedRecordSetWithStats.put("errors", plan.getErrorsUsingRepository());
                                pushedRecordSetWithStats.getJsonArray("errors")
                                        .add(new ErrorResponse(
                                                ErrorCategory.STORAGE,
                                                INTERNAL_SERVER_ERROR,
                                                inventoryUpdated.cause().getMessage())
                                                .asJson());
                                respondWithMultiStatus(routingContext,pushedRecordSetWithStats);
                              }
                            });
                  } else {
                    new ErrorResponse(
                            ErrorCategory.STORAGE,
                            INTERNAL_SERVER_ERROR,
                            result.cause().getMessage())
                            .setShortMessage("Failed to fetch from storage to upsert by match-key.")
                            .respond(routingContext);
                  }
                });
      } else {
        new ErrorResponse(
                ErrorCategory.VALIDATION,
                UNPROCESSABLE_ENTITY,
                validations.asJson())
                .setShortMessage(
                        "The incoming record set(s) had validation errors and were not processed ")
                .respond(routingContext);
      }
  }

  public void handleSharedInventoryRecordSetDeleteByIdentifiers(RoutingContext routingCtx) {
    logger.debug("Handling delete request for shared index " + routingCtx.getBodyAsString());
    if (contentTypeIsJson(routingCtx)) {
      JsonObject deletionJson = getIncomingJsonBody(routingCtx);
      if (deletionJson == null) {
        return;
      }
      RecordIdentifiers deletionIdentifiers = RecordIdentifiers.identifiersFromDeleteRequestJson(deletionJson);
      UpdatePlan updatePlan = UpdatePlanSharedInventory.getDeletionPlan(deletionIdentifiers);
      runDeletionPlan(updatePlan, routingCtx);
    }
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
                    .add(new ErrorResponse(
                            ErrorCategory.STORAGE,
                            INTERNAL_SERVER_ERROR,
                            deletionsDone.cause().getMessage())
                            .asJson());
            respondWithMultiStatus(routingCtx,pushedRecordSetWithStats);
          }
        });
      }  else {
        if (updatePlan.isDeletion && !updatePlan.foundExistingRecordSet()) {
          new ErrorResponse(
                  ErrorResponse.ErrorCategory.STORAGE,
                  NOT_FOUND,
                  "Error processing delete request:: "+ planDone.cause().getMessage())
                  .respond(routingCtx);
        } else {
          new ErrorResponse(
                  ErrorResponse.ErrorCategory.STORAGE,
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

  private boolean contentTypeIsJson (RoutingContext routingCtx) {
    String contentType = routingCtx.request().getHeader("Content-Type");
    if (contentType != null && !contentType.startsWith("application/json")) {
      logger.error("Only accepts Content-Type application/json, was: " + contentType);
      new ErrorResponse(
              ErrorResponse.ErrorCategory.VALIDATION,
              BAD_REQUEST,
              "Only accepts Content-Type application/json, content type was: "+ contentType)
              .setShortMessage("Only accepts application/json")
              .setStatusCode(BAD_REQUEST)
              .respond(routingCtx);
      return false;
    } else {
      return true;
    }
  }

  private JsonObject getIncomingJsonBody(RoutingContext routingCtx) {
    String bodyAsString;
    try {
      bodyAsString = routingCtx.getBodyAsString("UTF-8");
      if (bodyAsString == null) {
        new ErrorResponse(
                ErrorResponse.ErrorCategory.VALIDATION,
                BAD_REQUEST,
                "No request body provided.")
                .respond(routingCtx);
        return null;
      }
      JsonObject bodyAsJson = new JsonObject(bodyAsString);
      logger.debug("Request body " + bodyAsJson.encodePrettily());
      return bodyAsJson;
    } catch (DecodeException de) {
      new ErrorResponse(ErrorResponse.ErrorCategory.VALIDATION,
              BAD_REQUEST,
              "Could not parse JSON body of the request: " + de.getMessage())
              .setShortMessage("Could not parse request JSON")
              .respond(routingCtx);
      return null;
    }
  }

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
