/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventoryupdate;

import static org.folio.okapi.common.HttpResponse.responseError;
import static org.folio.okapi.common.HttpResponse.responseJson;

import java.util.HashMap;
import java.util.Map;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import org.folio.inventoryupdate.entities.DeletionIdentifiers;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.Future;
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

  public void handleSharedInventoryUpsertByMatchkey (RoutingContext routingCtx) {
    if (contentTypeIsJson(routingCtx)) {
      JsonObject inventoryRecordSet = getIncomingInventoryRecordSet(routingCtx);
      InventoryRecordSet incomingSet = new InventoryRecordSet(inventoryRecordSet);
      MatchKey matchKey = new MatchKey(incomingSet.getInstance().asJson());
      InventoryQuery instanceByMatchKeyQuery = new MatchQuery(matchKey.getKey());
      incomingSet.getInstance().asJson().put("matchKey", matchKey.getKey());
      incomingSet.getInstance().asJson().put("indexTitle", matchKey.getKey());

      UpdatePlan updatePlan = new UpdatePlanSharedInventory(incomingSet, instanceByMatchKeyQuery);
      runPlan(updatePlan, routingCtx);
    }
  }

  public void handleInventoryUpsertByHrid (RoutingContext routingCtx) {
    if (contentTypeIsJson(routingCtx)) {
      JsonObject incomingInventoryRecordSetJson = getIncomingInventoryRecordSet(routingCtx);
      InventoryRecordSet incomingSet = new InventoryRecordSet(incomingInventoryRecordSetJson);
      InventoryQuery queryByInstanceHrid = new HridQuery(incomingSet.getInstanceHRID());

      UpdatePlan updatePlan = new UpdatePlanAllHRIDs(incomingSet, queryByInstanceHrid);

      runPlan(updatePlan, routingCtx);
    }
  }

  public void handleInventoryRecordSetDeleteByHrid (RoutingContext routingCtx) {
    if (contentTypeIsJson(routingCtx)) {
      JsonObject deletionJson = routingCtx.getBodyAsJson();
      InventoryQuery queryByInstanceHrid = new HridQuery(deletionJson.getString("hrid"));
      UpdatePlan updatePlan = new UpdatePlanAllHRIDs(queryByInstanceHrid);
      runPlan(updatePlan, routingCtx);
    } else {
      //TODO: what if not?
    }
  }

  public void handleSharedInventoryRecordSetDeleteByMatchkey (RoutingContext routingCtx) {
    logger.debug("Handling delete request for shared index " + routingCtx.getBodyAsString());

    if (contentTypeIsJson(routingCtx)) {
      JsonObject deletionJson = routingCtx.getBodyAsJson();
      DeletionIdentifiers deletionIdentifiers = new DeletionIdentifiers(deletionJson);
      InventoryQuery instanceQuery = new SharedInstanceByLocalIdentifierQuery(deletionIdentifiers.localIdentifier(), deletionIdentifiers.identifierTypeId());
      UpdatePlan updatePlan = new UpdatePlanSharedInventory(deletionIdentifiers, instanceQuery);
      runPlan(updatePlan, routingCtx);
    } else {
      //TODO: what if not?
    }
  }


  //TODO: turn not-found conditions in case of deletion around?
  private void runPlan(UpdatePlan updatePlan, RoutingContext routingCtx) {

    OkapiClient okapiClient = getOkapiClient(routingCtx);

    Future<Void> planCreated = updatePlan.planInventoryUpdates(okapiClient);
    planCreated.onComplete( planDone -> {
      if (planDone.succeeded()) {
        updatePlan.writePlanToLog();
        Future<Void> planExecuted = updatePlan.doInventoryUpdates(okapiClient);
        planExecuted.onComplete( updatesDone -> {
          JsonObject pushedRecordSetWithStats = updatePlan.getUpdatingRecordSetJson();
          pushedRecordSetWithStats.put("metrics", updatePlan.getUpdateStats());
          if (updatesDone.succeeded()) {
            if (updatePlan.isDeletion && !updatePlan.foundExistingRecordSet()) {
              responseJson(routingCtx, 404).end(pushedRecordSetWithStats.encodePrettily());
            } else {
              responseJson(routingCtx, 200).end(pushedRecordSetWithStats.encodePrettily());
            }
            okapiClient.close();
          } else {
            pushedRecordSetWithStats.put("errors", updatePlan.getErrors());
            responseJson(routingCtx, 422).end(pushedRecordSetWithStats.encodePrettily());
          }
        });
      }  else {
        responseJson(routingCtx, 500).end("Error creating inventory update plan");
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

  private JsonObject getIncomingInventoryRecordSet(RoutingContext routingCtx) {
    String inventoryRecordSetAsString = routingCtx.getBodyAsString("UTF-8");
    JsonObject inventoryRecordSet = new JsonObject(inventoryRecordSetAsString);
    logger.debug("Incoming record set " + inventoryRecordSet.encodePrettily());
    return inventoryRecordSet;
  }

  private OkapiClient getOkapiClient (RoutingContext ctx) {
    OkapiClient client = new OkapiClient(ctx);
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-type", "application/json");
    if (ctx.request().getHeader("X-Okapi-Tenant") != null) headers.put("X-Okapi-Tenant", ctx.request().getHeader("X-Okapi-Tenant"));
    if (ctx.request().getHeader("X-Okapi-Token") != null) headers.put("X-Okapi-Token", ctx.request().getHeader("X-Okapi-Token"));
    headers.put("Accept", "application/json, text/plain");
    client.setHeaders(headers);
    return client;
  }


  /*
     =================
     Old API and supporting methods
     =================
  */

  /**
   * Main flow of Instance matching and creating/updating.
   * @param routingCtx
   */
  public void handleInstanceMatching(RoutingContext routingCtx) {
    String contentType = routingCtx.request().getHeader("Content-Type");
    if (contentType != null && !contentType.startsWith("application/json")) {
      responseError(routingCtx, 400, "Only accepts Content-Type application/json, was: "+ contentType);
    } else {
      OkapiClient okapiClient = getOkapiClient(routingCtx);

      String candidateInstanceAsString = routingCtx.getBodyAsString("UTF-8");
      JsonObject candidateInstance = new JsonObject(candidateInstanceAsString);

      logger.debug("Received a PUT of " + candidateInstance.toString());

      MatchKey matchKey = new MatchKey(candidateInstance);
      InventoryQuery matchQuery = new MatchQuery(matchKey.getKey());
      candidateInstance.put("matchKey", matchKey.getKey());
      candidateInstance.put("indexTitle", matchKey.getKey());
      logger.debug("Constructed match query: [" + matchQuery.getQueryString() + "]");

      okapiClient.get(INSTANCE_STORAGE_PATH+"?query="+matchQuery.getURLEncodedQueryString(), res-> {
        if ( res.succeeded()) {
          JsonObject matchingInstances = new JsonObject(res.result());
          updateSharedInventory(okapiClient, candidateInstance, matchingInstances, matchQuery, routingCtx);
        } else {
          String message = res.cause().getMessage();
          responseError(routingCtx, 500, "mod-inventory-storage failed with " + message);
        }
      });
    }
  }

  /**
   * Creates new Instance or updates existing Instance in Inventory, using the
   * incoming Instance candidate and depending on the result of the match query
   *
   * @param candidateInstance The new Instance to consider
   * @param matchingInstances Result of match query
   * @param matchQuery The match query (for log statements)
   * @param routingCtx
   */
  private void updateSharedInventory(OkapiClient okapiClient,
                               JsonObject candidateInstance,
                               JsonObject matchingInstances,
                               InventoryQuery matchQuery,
                               RoutingContext routingCtx) {

    int recordCount = matchingInstances.getInteger("totalRecords");
    if (recordCount == 0) {
      logger.debug("Match query [" + matchQuery.getQueryString() + "] did not find a matching instance. Will POST a new instance");
      postInstance(okapiClient, routingCtx, candidateInstance);
    }  else if (recordCount == 1) {
      logger.debug("Match query [" + matchQuery.getQueryString() + "] found a matching instance. Will PUT an instance update");
      JsonObject matchingInstance = matchingInstances.getJsonArray("instances").getJsonObject(0);
      JsonObject mergedInstance = mergeInstances(matchingInstance, candidateInstance);
      // Update existing instance
      putInstance(okapiClient, routingCtx, mergedInstance, matchingInstance.getString("id"));
    } else if (recordCount > 1) {
      logger.info("Multiple matches (" + recordCount + ") found by match query [" + matchQuery.getQueryString() + "], cannot determine which instance to update");
    } else {
      logger.info("Unexpected recordCount: ["+recordCount+"] cannot determine match");
    }
  }
  /**
   * Merges properties of candidate instance with select properties of existing instance
   * (without mutating the original JSON objects)
   * @param existingInstance Existing instance
   * @param newInstance Instance coming in on the request
   * @return merged Instance
   */
  private JsonObject mergeInstances (JsonObject existingInstance, JsonObject newInstance) {
    JsonObject mergedInstance = newInstance.copy();

    // Merge both identifier lists into list of distinct identifiers
    JsonArray uniqueIdentifiers = mergeUniquelyTwoArraysOfObjects(
            existingInstance.getJsonArray("identifiers"),
            newInstance.getJsonArray("identifiers"));
    mergedInstance.put("identifiers", uniqueIdentifiers);
    mergedInstance.put("hrid", existingInstance.getString("hrid"));
    return mergedInstance;
  }

  private JsonArray mergeUniquelyTwoArraysOfObjects (JsonArray array1, JsonArray array2) {
    JsonArray merged = new JsonArray();
    if (array1 != null) {
      merged = array1.copy();
    }
    if (array2 != null) {
      for (int i=0; i<array2.size(); i++) {
        if (arrayContainsValue(merged,array2.getJsonObject(i))) {
          continue;
        } else {
          merged.add(array2.getJsonObject(i).copy());
        }
      }
    }
    return merged;
  }

  private boolean arrayContainsValue(JsonArray array, JsonObject value) {
    for (int i=0; i<array.size(); i++) {
      if (array.getJsonObject(i).equals(value)) return true;
    }
    return false;
  }

  /**
   * Replaces an existing instance in Inventory with a new instance
   * @param routingCtx
   * @param newInstance
   * @param instanceId
   */
  private void putInstance (OkapiClient okapiClient, RoutingContext routingCtx, JsonObject newInstance, String instanceId) {
    okapiClient.request(HttpMethod.PUT, INSTANCE_STORAGE_PATH+"/"+instanceId, newInstance.toString(), putResult-> {
      if (putResult.succeeded()) {
        okapiClient.get(INSTANCE_STORAGE_PATH+"/"+instanceId, res-> {
          if ( res.succeeded()) {
            logger.debug("PUT of Instance succeeded");
            JsonObject instanceResponseJson = new JsonObject(res.result());
            String instancePrettyString = instanceResponseJson.encodePrettily();
            responseJson(routingCtx, 200).end(instancePrettyString);
            okapiClient.close();
          } else {
            String message = res.cause().getMessage();
            responseError(routingCtx, 500, "mod-inventory-storage GET failed with " + message);
            okapiClient.close();
          }
        });
      } else {
        String msg = putResult.cause().getMessage();
        responseError(routingCtx, 500, "mod-inventory-storage PUT failed with " + msg);
        okapiClient.close();
      }
    });
  }

  /**
   * Creates a new instance in Inventory
   * @param ctx
   * @param newInstance
   */
  private void postInstance (OkapiClient okapiClient, RoutingContext ctx, JsonObject newInstance) {
    okapiClient.post(INSTANCE_STORAGE_PATH, newInstance.toString(), postResult->{
      if (postResult.succeeded()) {
        logger.debug("POST of Instance succeeded");
        String instanceResult = postResult.result();
        JsonObject instanceResponseJson = new JsonObject(instanceResult);
        String instancePrettyString = instanceResponseJson.encodePrettily();
        responseJson(ctx, 200).end(instancePrettyString);
        okapiClient.close();
      } else {
        String msg = postResult.cause().getMessage();
        responseError(ctx, 500, "mod-inventory-storage POST failed with " + msg);
        okapiClient.close();
      }
    });
  }



}
