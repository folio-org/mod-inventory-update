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

  public void handleUnrecognizedPath(RoutingContext routingContext) {
    responseError(routingContext, 404, "No Service found for requested path " + routingContext.request().path());
  }

  public void handleHealthCheck(RoutingContext routingContext) {
    responseJson(routingContext, 200).end("{ \"status\": \"UP\" }");
  }

  public void handleSharedInventoryUpsertByMatchKeyBatch(RoutingContext routingContext) {
    long start = System.currentTimeMillis();
    JsonObject requestJson = routingContext.getBodyAsJson();
    if (requestJson.containsKey("inventoryRecordSets")) {
      JsonArray inventoryRecordSets = requestJson.getJsonArray("inventoryRecordSets");
      sharedInventoryUpsertByMatchKeyBatch(routingContext, inventoryRecordSets);
    } else {
      responseError(routingContext, 400,
              "InventoryBatchUpdateService expected but did not seem to receive " +
                      "a batch of Inventory record sets");
    }
  }



  public void handleSharedInventoryUpsertByMatchKey(RoutingContext routingContext) {
    try {
      if (contentTypeIsJson(routingContext)) {
        JsonObject incomingJson = getIncomingJsonBody(routingContext);

        // Fake a batch of one for now
        JsonArray inventoryRecordSets = new JsonArray();
        inventoryRecordSets.add(new JsonObject(incomingJson.encodePrettily()));
        sharedInventoryUpsertByMatchKeyBatch(routingContext, inventoryRecordSets);
      }
    } catch (NullPointerException npe) {
      npe.printStackTrace();
      responseJson(routingContext, 500).end(
              "Application had a null pointer exception.");

    } catch (Exception e) {
      e.printStackTrace();
      responseJson(routingContext, 500).end(
              "Application had an exception: " + e.getMessage());

    }
  }

  private void sharedInventoryUpsertByMatchKeyBatch(RoutingContext routingContext, JsonArray inventoryRecordSets) {
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
                                responseJson(routingContext, 200).end(pushedRecordSetWithStats.encodePrettily());
                              } else {
                                JsonObject pushedRecordSetWithStats = plan.getUpdatingRecordSetJsonFromRepository();
                                pushedRecordSetWithStats.put("metrics", plan.getUpdateStatsFromRepository());
                                pushedRecordSetWithStats.put("errors", plan.getErrorsUsingRepository());
                                responseJson(routingContext, 422).end(pushedRecordSetWithStats.encodePrettily());
                              }
                            });
                  } else {
                    responseJson(routingContext, 422).end("{\"failed\": \"upsertByMatchKey\"}");
                  }
                });
      } else {
        responseJson(routingContext, 422).end(
                "The incoming record set(s) had errors and were not processed " + validations);
      }
  }



  public void handleSharedInventoryRecordSetDeleteByIdentifiers(RoutingContext routingCtx) {
    logger.debug("Handling delete request for shared index " + routingCtx.getBodyAsString());
    if (contentTypeIsJson(routingCtx)) {
      JsonObject deletionJson = getIncomingJsonBody(routingCtx);
      RecordIdentifiers deletionIdentifiers = RecordIdentifiers.identifiersFromDeleteRequestJson(deletionJson);
      UpdatePlan updatePlan = UpdatePlanSharedInventory.getDeletionPlan(deletionIdentifiers);
      runDeletionPlan(updatePlan, routingCtx);
    }
  }

  public void handleInventoryUpsertByHRIDBatch(RoutingContext routingContext) {
    JsonObject requestJson = routingContext.getBodyAsJson();
    if (requestJson.containsKey("inventoryRecordSets")) {
      JsonArray inventoryRecordSets = requestJson.getJsonArray("inventoryRecordSets");
      inventoryUpsertByHRIDBatch(routingContext, inventoryRecordSets);
    } else {
      responseError(routingContext, 400,
              "InventoryBatchUpdateService expected but did not seem to receive " +
                      "a batch of Inventory record sets");
    }

  }

  public void handleInventoryUpsertByHRID(RoutingContext routingCtx) {
    if (! contentTypeIsJson(routingCtx)) {
      return;
    }
    JsonObject incomingJson = getIncomingJsonBody(routingCtx);
    if (incomingJson == null) {
      return;
    }
    if (! InventoryRecordSet.isValidInventoryRecordSet(incomingJson)) {
      responseError(routingCtx, 400, "Did not recognize input as an Inventory record set: " + incomingJson.encodePrettily());
      return;
    }
    // Fake a batch of one for now
    JsonArray inventoryRecordSets = new JsonArray();
    inventoryRecordSets.add(new JsonObject(incomingJson.encodePrettily()));
    inventoryUpsertByHRIDBatch(routingCtx, inventoryRecordSets);
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

  private void inventoryUpsertByHRIDBatch(RoutingContext routingContext, JsonArray inventoryRecordSets) {
    UpdatePlanAllHRIDs plan = UpdatePlanAllHRIDs.getUpsertPlan();
    RequestValidation validations = validateIncomingRecordSets (plan, inventoryRecordSets);

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
                                      responseJson(routingContext, 200).end(pushedRecordSetWithStats.encodePrettily());
                                    } else {
                                      JsonObject pushedRecordSetWithStats = plan.getUpdatingRecordSetJsonFromRepository();
                                      pushedRecordSetWithStats.put("metrics", plan.getUpdateStatsFromRepository());
                                      pushedRecordSetWithStats.put("errors", plan.getErrorsUsingRepository());
                                      responseJson(routingContext, 422).end(pushedRecordSetWithStats.encodePrettily());
                                    }
                                  });
                        } else {
                          responseJson(routingContext, 422).end("\"{\\\"failed\\\": \\\"upsertByHRIDs\\\"}\"");
                        }
                      });
    } else {
      responseJson(routingContext, 422).end("The incoming record set(s) had errors and were not processed " + validations);
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

  //TODO: invert not-found conditions in case of deletion?
  private void runDeletionPlan(UpdatePlan updatePlan, RoutingContext routingCtx) {

    OkapiClient okapiClient = getOkapiClient(routingCtx);
    updatePlan.planInventoryDelete(okapiClient).onComplete(planDone -> {
      if (planDone.succeeded()) {
        //updatePlan.writePlanToLog();
        updatePlan.doInventoryDelete(okapiClient).onComplete(updatesDone -> {
          JsonObject pushedRecordSetWithStats = updatePlan.getUpdatingRecordSetJson();
          pushedRecordSetWithStats.put("metrics", updatePlan.getUpdateStats());
          if (updatesDone.succeeded()) {
            responseJson(routingCtx, 200).end(pushedRecordSetWithStats.encodePrettily());
          } else {
            pushedRecordSetWithStats.put("errors", updatePlan.getErrors());
            responseJson(routingCtx, 422).end(pushedRecordSetWithStats.encodePrettily());
          }
        });
      }  else {
        if (updatePlan.isDeletion && !updatePlan.foundExistingRecordSet()) {
          responseJson(routingCtx, 404).end("Error processing delete request:: "+ planDone.cause().getMessage());
        } else {
          responseJson(routingCtx, 422).end("Error creating an inventory update plan:" + LF + "  " + planDone.cause().getMessage());
        }
      }
    });
  }

  private boolean contentTypeIsJson (RoutingContext routingCtx) {
    String contentType = routingCtx.request().getHeader("Content-Type");
    if (contentType != null && !contentType.startsWith("application/json")) {
      logger.error("Only accepts Content-Type application/json, was: " + contentType);
      responseError(routingCtx, 400, "Only accepts Content-Type application/json, content type was: "+ contentType);
      return false;
    } else {
      return true;
    }
  }

  private JsonObject getIncomingJsonBody(RoutingContext routingCtx) {
    String bodyAsString = "no body";
    try {
      bodyAsString = routingCtx.getBodyAsString("UTF-8");
      JsonObject bodyAsJson = new JsonObject(bodyAsString);
      logger.debug("Request body " + bodyAsJson.encodePrettily());
      return bodyAsJson;
    } catch (DecodeException de) {
      responseError(routingCtx, 400, "Only accepts json, content was: "+ bodyAsString);
      return null;
    }
  }

}
