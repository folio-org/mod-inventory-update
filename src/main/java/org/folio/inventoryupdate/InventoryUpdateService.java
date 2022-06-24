package org.folio.inventoryupdate;

import static org.folio.inventoryupdate.InventoryStorage.getOkapiClient;
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
  private static final int BAD_REQUEST = 400;
  private static final int UNPROCESSABLE_ENTITY = 422;
  private static final int OK = 200;
  private static final int NOT_FOUND = 404;
  private static final int INTERNAL_SERVER_ERROR = 500;

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
      InventoryUpdateError error =
              new InventoryUpdateError(
                      InventoryUpdateError.ErrorCategory.VALIDATION,
                      "Did not recognize input as an Inventory record set")
                      .setShortMessage("Not an Inventory record set.")
                      .setEntity(incomingJson)
                      .setStatusCode(BAD_REQUEST);
      respondWithBadRequest(routingCtx,error);
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
      InventoryUpdateError error = new InventoryUpdateError(
              InventoryUpdateError.ErrorCategory.VALIDATION,
              "Did not recognize request body as a batch of Inventory record sets")
              .setShortMessage("Not a batch of Inventory record sets")
              .setEntity(requestJson)
              .setStatusCode(BAD_REQUEST);
      respondWithBadRequest(routingContext, error);
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
                                      respondWithUnprocessableEntity(routingContext,pushedRecordSetWithStats);
                                    }
                                  });
                        } else {
                          InventoryUpdateError error = new InventoryUpdateError(
                                  InventoryUpdateError.ErrorCategory.STORAGE,
                                  result.cause().getMessage())
                                  .setShortMessage("Upsert by HRIDs failed");
                          respondWithUnprocessableEntity(routingContext,error.asJson());
                        }
                      });
    } else {
      JsonObject jo = new JsonObject();
      jo.put("problem","The incoming record "
              + (inventoryRecordSets.size() == 1 ? "set" : "sets")
              + " had one or more errors."
              + (inventoryRecordSets.size() == 1 ? " The records were not processed: " : " The batch was not processed."));
      jo.mergeIn(validations.asJson());
      respondWithUnprocessableEntity(routingContext,jo);
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
      respondWithInternalServerError(routingContext,"Application had a null pointer exception.");

    } catch (Exception e) {
      e.printStackTrace();
      respondWithInternalServerError(routingContext,"Application had an exception: " + e.getMessage());
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
      InventoryUpdateError error = new InventoryUpdateError(
              InventoryUpdateError.ErrorCategory.VALIDATION,
              "Did not recognize request body as a batch of Inventory record sets")
              .setShortMessage("Not a batch of Inventory record sets")
              .setEntity(requestJson)
              .setStatusCode(BAD_REQUEST);
      respondWithBadRequest(routingContext, error);
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
                                respondWithUnprocessableEntity(routingContext,pushedRecordSetWithStats);
                              }
                            });
                  } else {
                    JsonObject jo = new JsonObject();
                    jo.put("problem", "Upsert by match-key failed");
                    jo.put("message", result.cause().getMessage());
                    respondWithUnprocessableEntity(routingContext,jo);
                  }
                });
      } else {
        JsonObject jo = new JsonObject();
        jo.put("problem", "The incoming record set(s) had errors and were not processed ");
        jo.put("message", validations.asJson());
        respondWithUnprocessableEntity(routingContext,jo);
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
        updatePlan.doInventoryDelete(okapiClient).onComplete(updatesDone -> {
          JsonObject pushedRecordSetWithStats = updatePlan.getUpdatingRecordSetJson();
          pushedRecordSetWithStats.put("metrics", updatePlan.getUpdateStats());
          if (updatesDone.succeeded()) {
            respondWithOK(routingCtx,pushedRecordSetWithStats);
          } else {
            pushedRecordSetWithStats.put("errors", updatePlan.getErrors());
            respondWithUnprocessableEntity(routingCtx,pushedRecordSetWithStats);
          }
        });
      }  else {
        if (updatePlan.isDeletion && !updatePlan.foundExistingRecordSet()) {
          respondWithNotFound(routingCtx,"Error processing delete request:: "+ planDone.cause().getMessage());
        } else {
          JsonObject jo = new JsonObject();
          jo.put("problem", "Error creating an inventory update plan");
          jo.put("message", planDone.cause().getMessage());
          respondWithUnprocessableEntity(routingCtx, jo);
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
      new InventoryUpdateError(
              InventoryUpdateError.ErrorCategory.VALIDATION,
              "Only accepts Content-Type application/json, content type was: "+ contentType)
              .setShortMessage("Only accepts application/json")
              .setStatusCode(BAD_REQUEST);
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
        respondWithBadRequest(routingCtx,
                new InventoryUpdateError(
                        InventoryUpdateError.ErrorCategory.VALIDATION,
                        "No request body provided."));
        return null;
      }
      JsonObject bodyAsJson = new JsonObject(bodyAsString);
      logger.debug("Request body " + bodyAsJson.encodePrettily());
      return bodyAsJson;
    } catch (DecodeException de) {
      respondWithBadRequest(routingCtx,
              new InventoryUpdateError(InventoryUpdateError.ErrorCategory.VALIDATION,
                      "Could not parse JSON body of the request: " + de.getMessage())
                      .setShortMessage("Could not parse request JSON")
                      .setStatusCode(BAD_REQUEST));
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

  public void respondWithUnprocessableEntity(RoutingContext routingContext, JsonObject message) {
    responseJson(routingContext, UNPROCESSABLE_ENTITY).end(message.encodePrettily());
  }

  public void respondWithBadRequest(RoutingContext routingContext, InventoryUpdateError error ) {
    responseJson(routingContext, BAD_REQUEST).end(error.asJsonString());
  }

  public void respondWithNotFound(RoutingContext routingContext, String message) {
    InventoryUpdateError error = new InventoryUpdateError(
            InventoryUpdateError.ErrorCategory.STORAGE,
            message)
            .setStatusCode(NOT_FOUND);
    responseJson(routingContext, NOT_FOUND).end(error.asJsonString());
  }

  public void respondWithInternalServerError(RoutingContext routingContext, String message) {
    InventoryUpdateError error = new InventoryUpdateError(
            InventoryUpdateError.ErrorCategory.STORAGE,
            message)
            .setStatusCode(INTERNAL_SERVER_ERROR);
    responseJson(routingContext,INTERNAL_SERVER_ERROR).end(error.asJsonString());
  }

}
