package org.folio.inventoryupdate;

import static org.folio.okapi.common.HttpResponse.responseError;
import static org.folio.okapi.common.HttpResponse.responseJson;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.DecodeException;
import org.folio.inventoryupdate.entities.RecordIdentifiers;
import org.folio.inventoryupdate.entities.InventoryRecordSet;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

/**
 * MatchService looks for an Instance in Inventory that matches an incoming
 * Instance and either updates or creates an Instance based on the results.
 *
 */
public class InventoryUpdateService {
  private final Logger logger = LoggerFactory.getLogger("inventory-update");

  private static final String INSTANCE_STORAGE_PATH = "/instance-storage/instances";

  private static final String LF = System.lineSeparator();

  public void handleUnrecognizedPath(RoutingContext routingContext) {
    responseError(routingContext, 404, "No Service found for requested path " + routingContext.request().path());
  }

  public void handleHealthCheck(RoutingContext routingContext) {
    responseJson(routingContext, 200).end("{ \"status\": \"UP\" }");
  }

  public void handleSharedInventoryUpsertByMatchKey(RoutingContext routingCtx) {
    if (contentTypeIsJson(routingCtx)) {
      JsonObject incomingJson = getIncomingJsonBody(routingCtx);
      if (InventoryRecordSet.isValidInventoryRecordSet(incomingJson)) {
        InventoryRecordSet incomingSet = new InventoryRecordSet(incomingJson);
        UpdatePlan updatePlan = UpdatePlanSharedInventory.getUpsertPlan(incomingSet);
        runPlan(updatePlan, routingCtx);
      } else {
        responseError(routingCtx, 400, "Did not recognize input as an Inventory record set: "+
                (incomingJson != null ? incomingJson.encodePrettily() : "no JSON object"));
      }
    }
  }

  public void handleSharedInventoryRecordSetDeleteByIdentifiers(RoutingContext routingCtx) {
    logger.debug("Handling delete request for shared index " + routingCtx.getBodyAsString());
    if (contentTypeIsJson(routingCtx)) {
      JsonObject deletionJson = getIncomingJsonBody(routingCtx);
      RecordIdentifiers deletionIdentifiers = RecordIdentifiers.identifiersFromDeleteRequestJson(deletionJson);
      UpdatePlan updatePlan = UpdatePlanSharedInventory.getDeletionPlan(deletionIdentifiers);
      runPlan(updatePlan, routingCtx);
    }
  }

  public void handleInventoryUpsertByHRID(RoutingContext routingCtx) {
    if (contentTypeIsJson(routingCtx)) {
      JsonObject incomingJson = getIncomingJsonBody(routingCtx);
      if (InventoryRecordSet.isValidInventoryRecordSet(incomingJson)) {
        InventoryRecordSet incomingSet = new InventoryRecordSet(incomingJson);
        UpdatePlan updatePlan = UpdatePlanAllHRIDs.getUpsertPlan(incomingSet);
        runPlan(updatePlan, routingCtx);
      } else {
        responseError(routingCtx, 400, "Did not recognize input as an Inventory record set: "+ incomingJson.encodePrettily());
      }
    }
  }

  public void handleInventoryRecordSetDeleteByHRID(RoutingContext routingCtx) {
    if (contentTypeIsJson(routingCtx)) {
      JsonObject deletionJson = getIncomingJsonBody(routingCtx);
      InventoryQuery queryByInstanceHrid = new HridQuery(deletionJson.getString("hrid"));
      UpdatePlan updatePlan = UpdatePlanAllHRIDs.getDeletionPlan(queryByInstanceHrid);
      runPlan(updatePlan, routingCtx);
    }
  }

  //TODO: invert not-found conditions in case of deletion?
  private void runPlan(UpdatePlan updatePlan, RoutingContext routingCtx) {

    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingCtx);
    updatePlan.planInventoryUpdates(okapiClient).onComplete( planDone -> {
      if (planDone.succeeded()) {
        updatePlan.writePlanToLog();
        updatePlan.doInventoryUpdates(okapiClient).onComplete( updatesDone -> {
          JsonObject pushedRecordSetWithStats = updatePlan.getUpdatingRecordSetJson();
          pushedRecordSetWithStats.put("metrics", updatePlan.getUpdateStats());
          if (updatesDone.succeeded()) {
            responseJson(routingCtx, 200).end(pushedRecordSetWithStats.encodePrettily());
            okapiClient.close();
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
