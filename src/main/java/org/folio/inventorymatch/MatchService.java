/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventorymatch;

import static org.folio.okapi.common.HttpResponse.responseError;
import static org.folio.okapi.common.HttpResponse.responseJson;

import java.util.HashMap;
import java.util.Map;

import org.folio.inventorymatch.InventoryRecordSet.HoldingsRecord;
import org.folio.inventorymatch.InventoryRecordSet.Item;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;

/**
 * MatchService looks for an Instance in Inventory that matches an incoming
 * Instance and either updates or creates an Instance based on the results.
 *
 */
public class MatchService {
  private final Logger logger = LoggerFactory.getLogger("inventory-matcher");
  private static final String INSTANCE_STORAGE_PATH = "/instance-storage/instances";

  public final static String INSTANCE_MATCH_PATH = "/instance-storage-match/instances"; // being deprecated
  public final static String INSTANCE_UPSERT_MATCHKEY_PATH = "/instance-storage-upsert-matchkey";
  public final static String INSTANCE_UPSERT_HRID_PATH = "/instance-storage-upsert-hrid";
  public final static String INVENTORY_UPSERT_HRID_PATH = "/inventory-upsert-hrid";
  public final static String INVENTORY_UPSERT_MATCHKEY_PATH = "/inventory-upsert-matchkey";

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

      logger.info("Received a PUT of " + candidateInstance.toString());

      MatchKey matchKey = new MatchKey(candidateInstance);
      InventoryQuery matchQuery = new MatchQuery(matchKey.getKey());
      candidateInstance.put("matchKey", matchKey.getKey());
      candidateInstance.put("indexTitle", matchKey.getKey());
      logger.info("Constructed match query: [" + matchQuery.getQueryString() + "]");

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

  public void handleInstanceUpsertByHrid (RoutingContext routingCtx) {
    String contentType = routingCtx.request().getHeader("Content-Type");
    if (contentType != null && !contentType.startsWith("application/json")) {
      responseError(routingCtx, 400, "Only accepts Content-Type application/json, was: "+ contentType);
    } else {
      OkapiClient okapiClient = getOkapiClient(routingCtx);

      String candidateInstanceAsString = routingCtx.getBodyAsString("UTF-8");
      JsonObject candidateInstance = new JsonObject(candidateInstanceAsString);

      logger.info("Received a PUT of " + candidateInstance.toString());

      String hrid = candidateInstance.getString("hrid");
      if (hrid != null) {
        HridQuery hridQuery = new HridQuery(hrid);
        logger.info("Constructed HRID query: [" + hridQuery.getQueryString() + "]");

        okapiClient.get(INSTANCE_STORAGE_PATH+"?query="+hridQuery.getURLEncodedQueryString(), res-> {
          if ( res.succeeded()) {
            JsonObject matchingInstances = new JsonObject(res.result());
            updateInventory(okapiClient, candidateInstance, matchingInstances, hridQuery.getQueryString(), routingCtx);
          } else {
            String message = res.cause().getMessage();
            responseError(routingCtx, 500, "mod-inventory-storage failed with " + message);
          }
        });
      } else {
        responseError(routingCtx, 400, "Cannot upsert by HRID, HRID missing in Instance");
      }
    }
  }

  public void handleInventoryUpsertByMatchkey (RoutingContext routingCtx) { // TODO: Pass in okapi client?
    if (contentTypeIsJson(routingCtx)) {
      OkapiClient okapiClient = getOkapiClient(routingCtx); // TODO: close it? when/where?
      JsonObject inventoryRecordSet = getIncomingInventoryRecordSet(routingCtx);

      JsonObject incomingInstance         = inventoryRecordSet.getJsonObject("instance");
      JsonArray  incomingHoldingsAndItems = inventoryRecordSet.getJsonArray("holdingsRecords") == null ?
                                                         new JsonArray()
                                                         :
                                                         inventoryRecordSet.getJsonArray("holdingsRecords");

      MatchKey matchKey = new MatchKey(incomingInstance);
      InventoryQuery instanceQuery = new MatchQuery(matchKey.getKey());
      incomingInstance.put("matchKey", matchKey.getKey());
      incomingInstance.put("indexTitle", matchKey.getKey());

      Future<JsonObject> promisedExistingInstance = InventoryStorage.lookupInstance(okapiClient, instanceQuery);
      promisedExistingInstance.onComplete( ar -> {
        if (ar.result()==null) {
          createInventoryRecords(routingCtx, okapiClient, incomingInstance, incomingHoldingsAndItems);
        } else {
          JsonObject existingInstance = ar.result();
          String instanceId = existingInstance.getString("id");
          incomingInstance.put("id", instanceId);
          JsonObject mergedInstance = mergeInstances(existingInstance, incomingInstance);
          updateInventoryRecords(routingCtx, okapiClient, mergedInstance, incomingHoldingsAndItems, instanceId);
        }
      });
    }
  }

  public void handleInventoryUpsertByHrid (RoutingContext routingCtx) {
    if (contentTypeIsJson(routingCtx)) {
      OkapiClient okapiClient = getOkapiClient(routingCtx);
      JsonObject incomingInventoryRecordSetJson = getIncomingInventoryRecordSet(routingCtx);
      InventoryRecordSet incomingSet = new InventoryRecordSet(incomingInventoryRecordSetJson);
      String instanceHrid = incomingSet.getInstanceHRID();
      Future<JsonObject> promisedExistingInventoryRecordSet = InventoryStorage.lookupInventoryRecordSetByInstanceHRID(okapiClient, instanceHrid);
      promisedExistingInventoryRecordSet.onComplete( recordSet -> {
        if (recordSet.succeeded()) {
          JsonObject existingInventoryRecordSetJson = recordSet.result();
          if (existingInventoryRecordSetJson != null) {
            logger.info("Found existing instance");
          }
          InventoryRecordSet existingSet = new InventoryRecordSet(existingInventoryRecordSetJson);
          logger.info("Instantiating an update plan");
          UpdatePlan updatePlan = new UpdatePlainInventoryByHRIDs(incomingSet, existingSet, okapiClient);
          logger.info("Planning updates");
          Future<Void> planDone = updatePlan.planInventoryUpdates(okapiClient);
          planDone.onComplete( handler -> {
            logger.info("Planning done: ");
            logger.info("Instance transition: " + updatePlan.getIncomingRecordSet().getInstance().getTransition());

            logger.info("Holdings to create: ");
            for (HoldingsRecord record : updatePlan.holdingsToCreate()) {
              logger.info(record.getJson().encodePrettily());
            }
            logger.info("Holdings to update: ");
            for (HoldingsRecord record : updatePlan.holdingsToUpdate()) {
              logger.info(record.getJson().encodePrettily());
            }
            logger.info("Items to create: ");
            for (Item record : updatePlan.itemsToCreate()) {
              logger.info(record.getJson().encodePrettily());
            }
            logger.info("Items to update: ");
            for (Item record : updatePlan.itemsToUpdate()) {
              logger.info(record.getJson().encodePrettily());
            }
            logger.info("Items to delete: ");
            for (Item record : updatePlan.itemsToDelete()) {
              logger.info(record.getJson().encodePrettily());
            }
            logger.info("Holdings to delete: ");
            for (HoldingsRecord record : updatePlan.holdingsToDelete()) {
              logger.info(record.getJson().encodePrettily());
            }

            Future<Void> promisedPlanDone = updatePlan.updateInventory(okapiClient);
            promisedPlanDone.onComplete( planExecuted -> {
              if (planExecuted.succeeded()) {
                responseJson(routingCtx, 200).end(
                  updatePlan.isInstanceUpdating() ?
                           "Updated this record set: " + updatePlan.getExistingRecordSet().getSourceJson().encodePrettily()
                            +  " with this record set: " + updatePlan.getIncomingRecordSet().getSourceJson().encodePrettily()
                            :
                            "Created this record set: " + updatePlan.getIncomingRecordSet().getSourceJson().encodePrettily());
                okapiClient.close();

              } else {
                responseJson(routingCtx, 500).end("Error executing inventory update plan");
                okapiClient.close();
              }
            });
          });
        } else {
          responseError(routingCtx, 422, "There was an retrieving existing record set: " + recordSet.cause().getMessage());
        }
      });
    }
  }

  public void handleInventoryUpsertByHridPreviousVersion (RoutingContext routingCtx) {
    if (contentTypeIsJson(routingCtx)) {
      OkapiClient okapiClient = getOkapiClient(routingCtx); // TODO: close it? when/where?
      JsonObject inventoryRecordSet = getIncomingInventoryRecordSet(routingCtx);

      JsonObject incomingInstance         = inventoryRecordSet.getJsonObject("instance");
      JsonArray  incomingHoldingsAndItems = inventoryRecordSet.getJsonArray("holdingsRecords") == null ?
                                                         new JsonArray()
                                                         :
                                                         inventoryRecordSet.getJsonArray("holdingsRecords");

      InventoryQuery instanceQuery = new HridQuery(incomingInstance.getString("hrid"));

      Future<JsonObject> promisedExistingInstance = InventoryStorage.lookupInstance(okapiClient, instanceQuery);
      promisedExistingInstance.onComplete( ar -> {
        if (ar.result()==null) {
          // check if any holdings / items already exists on another instance
          createInventoryRecords(routingCtx, okapiClient, incomingInstance, incomingHoldingsAndItems);

        } else {

          String instanceId = ar.result().getString("id");
          incomingInstance.put("id", instanceId);

          updateInventoryRecords(routingCtx, okapiClient, incomingInstance, incomingHoldingsAndItems, instanceId);
        }
      });
    }
  }

  private boolean contentTypeIsJson (RoutingContext routingCtx) {
    String contentType = routingCtx.request().getHeader("Content-Type");
    if (contentType != null && !contentType.startsWith("application/json")) {
      responseError(routingCtx, 400, "Only accepts Content-Type application/json, content type was: "+ contentType);
      return false;
    } else {
      return true;
    }
  }

  private JsonObject getIncomingInventoryRecordSet(RoutingContext routingCtx) {
    String inventoryRecordSetAsString = routingCtx.getBodyAsString("UTF-8");
    JsonObject inventoryRecordSet = new JsonObject(inventoryRecordSetAsString);
    logger.info("Incoming record set " + inventoryRecordSet.encodePrettily());
    return inventoryRecordSet;
  }

  private void createInventoryRecords(RoutingContext routingCtx,
                                      OkapiClient okapiClient,
                                      JsonObject incomingInstance,
                                      JsonArray incomingHoldingsAndItems) {
    Future<JsonObject> promisedCreatedInstance = InventoryStorage.postInstance(okapiClient, incomingInstance);

    promisedCreatedInstance.onComplete( hndl -> {
      if (hndl.succeeded()) {
        JsonObject persistedInstance = hndl.result();
        String instanceId = persistedInstance.getString("id");
        insertHoldingsAndItems(okapiClient, incomingHoldingsAndItems, instanceId);
        responseJson(routingCtx, 200).end(persistedInstance.encodePrettily());
        // TODO: okapiClient.close();
      } else {
        responseError(routingCtx, 422, hndl.cause().getMessage());
        // TODO: okapiClient.close();
      }
    });
  }

  private void updateInventoryRecords(RoutingContext routingCtx,
                                      OkapiClient okapiClient,
                                      JsonObject instance,
                                      JsonArray incomingHoldingsAndItems,
                                      String instanceId) {

    Future<JsonObject> promisedUpdatedInstance = InventoryStorage.putInstance(okapiClient, instance, instanceId);

    promisedUpdatedInstance.onComplete( hndl -> {
      if (hndl.succeeded()) {
        Future<JsonArray> promisedExistingHoldingsAndItems =
              InventoryStorage.lookupExistingHoldingsRecordsAndItemsByInstanceUUID(okapiClient, instanceId);
        promisedExistingHoldingsAndItems.onComplete( existingHoldingsResult -> {
          if (existingHoldingsResult != null) {
            upsertHoldingsAndItems(okapiClient, existingHoldingsResult.result(), incomingHoldingsAndItems, instanceId);
          } else {
            // No existing holdings and items - persist incoming records
            insertHoldingsAndItems(okapiClient, incomingHoldingsAndItems, instanceId);
          }
        });
        responseJson(routingCtx, 200).end(hndl.result().encodePrettily());
        // TODO: okapiClient.close()
      } else {
        responseError(routingCtx,422, hndl.cause().getMessage());
        // TODO: okapiClient.close()
      }
    });
  }


  private void upsertHoldingsAndItems(OkapiClient okapiClient,
                                      JsonArray existingHoldingsAndItems,
                                      JsonArray incomingHoldingsAndItems,
                                      String instanceId) {

    boolean existingHoldingsAndItemsHaveIds = checkForIdsInHoldingsAndItems(existingHoldingsAndItems);
    boolean incomingHoldingsAndItemsHaveIds = checkForIdsInHoldingsAndItems(incomingHoldingsAndItems);

    if (existingHoldingsAndItemsHaveIds && incomingHoldingsAndItemsHaveIds) {
      Map<String, JsonObject> existingHoldingsMap = new HashMap<String, JsonObject>();
      Map<String, JsonObject> existingItemsMap = new HashMap<String, JsonObject>();
      mapHoldingsAndItemsByIdentifiers (existingHoldingsAndItems, existingHoldingsMap, existingItemsMap);

      for (Object holdingsObject : incomingHoldingsAndItems) {
        JsonObject incomingHoldingsRecordWithItems = (JsonObject) holdingsObject;
        String incomingLocalIdentifier = incomingHoldingsRecordWithItems.getJsonArray("formerIds").getString(0);
        JsonObject existingHoldingsRecord = existingHoldingsMap.get(incomingLocalIdentifier);
        if (existingHoldingsRecord != null) {
          incomingHoldingsRecordWithItems.put("id", existingHoldingsRecord.getString("id"));
          incomingHoldingsRecordWithItems.put("hrid", existingHoldingsRecord.getString("hrid"));
          incomingHoldingsRecordWithItems.put("instanceId", existingHoldingsRecord.getString("instanceId"));
          JsonArray incomingItems = extractJsonArrayFromObject(incomingHoldingsRecordWithItems, "items");
          InventoryStorage.putHoldingsRecord(okapiClient, incomingHoldingsRecordWithItems, existingHoldingsRecord.getString("id")); // TODO: capture response
          for (Object itemObject : incomingItems) {
            JsonObject incomingItem = (JsonObject) itemObject;
            String incomingItemIdentifier = incomingItem.getString("itemIdentifier");
            JsonObject existingItem = existingItemsMap.get(incomingItemIdentifier);
            if (existingItem != null) {
              incomingItem.put("id", existingItem.getString("id"));
              incomingItem.put("hrid", existingItem.getString("hrid"));
              incomingItem.put("holdingsRecordId", existingItem.getString("holdingsRecordId"));
              InventoryStorage.putItem(okapiClient, incomingItem, existingItem.getString("id")); // TODO: capture response
            } else {
              insertItem(okapiClient, incomingItem, existingHoldingsRecord.getString("id"));
            }
          }
        } else {
          // Current holdings record with that identifier not found, create holdings and items
          // TODO: but check that items don't already exist on another holdings record
          insertHoldingsRecordWithItems(okapiClient, incomingHoldingsRecordWithItems, instanceId);
        }
      }
    } else {
      // One or more existings or incoming holdings or items did not have identifier(s) - fall back to delete and create
    }
  }

  private void insertHoldingsAndItems (OkapiClient okapiClient, JsonArray holdingsRecords, String instanceId) {
    for (Object element : holdingsRecords) {
      JsonObject holdingsRecord = (JsonObject) element;
      // TODO: holdings record already exists? (same holdings record identifier, different instance ID - update that instead of insert)
      insertHoldingsRecordWithItems(okapiClient, holdingsRecord, instanceId);
    }
  }

  private void insertHoldingsRecordWithItems (OkapiClient okapiClient, JsonObject holdingsRecord, String instanceId) {
    holdingsRecord.put("instanceId", instanceId);
    JsonArray items = extractJsonArrayFromObject(holdingsRecord, "items");
      // TODO: if holdings record already exists (same holdings identifier, different instanceId: update that instead of insert)
    Future<JsonObject> promisedCreatedHoldings = InventoryStorage.postHoldingsRecord(okapiClient, holdingsRecord);
    promisedCreatedHoldings.onComplete( hndl -> {
      JsonObject createdHoldings = hndl.result();
      if (createdHoldings == null) {
        logger.info("Something went wrong with the holdings record");
      } else {
        String callNumber = createdHoldings.getString("callNumber");
        logger.info("Holdings record created, call number " + callNumber + " - will insert items");
        String holdingsRecordId = createdHoldings.getString("id");
        insertItems(okapiClient, items, holdingsRecordId);
      }
    });
  }

  private void insertItems(OkapiClient okapiClient, JsonArray items, String holdingsRecordId) {
    for (Object element : items) {
      JsonObject item = (JsonObject) element;
      // TODO: if item already exists (same item id, different holdingsRecordId: update that instead of insert)
      insertItem(okapiClient, item, holdingsRecordId);
    }
  }

  private void insertItem (OkapiClient okapiClient, JsonObject item, String holdingsRecordId) {
    item.put("holdingsRecordId", holdingsRecordId);
    Future<JsonObject> promisedCreatedItem = InventoryStorage.postItem(okapiClient, item);
    promisedCreatedItem.onComplete( hndl -> {
      if (hndl.succeeded()) {
        // count created item
      } else {
        // add to list of errors
      }
    });
  }



  private boolean checkForIdsInHoldingsAndItems (JsonArray holdingsRecords) {
    for (Object holdingsObject : holdingsRecords) {
      JsonObject holdingsRecord = (JsonObject) holdingsObject;
      if (holdingsRecord.containsKey("formerIds") && holdingsRecord.getJsonArray("formerIds").size()==1) {
        logger.info("Looking for items in " + holdingsRecord.encodePrettily());
        JsonArray items = holdingsRecord.getJsonArray("items");
        for (Object itemObject : items) {
          JsonObject item = (JsonObject) itemObject;
          if (item.containsKey("itemIdentifier")) {
            continue;
          } else {
            logger.info("Found item without item identifier. Cannot update holdings and items by IDs");
            return false;
          }
        }
        continue;
      } else {
        logger.info("Found holdings record without one and only one former ID. Cannot update holdings and items by IDs");
        return false;
      }
    }
    return true;
  }

  private void mapHoldingsAndItemsByIdentifiers(JsonArray holdingsRecords, Map<String,JsonObject> holdingsMap, Map<String,JsonObject> itemsMap) {
    for (Object holdingsObject : holdingsRecords) {
      JsonObject holdingsRecord = (JsonObject) holdingsObject;
      holdingsMap.put(holdingsRecord.getJsonArray("formerIds").getString(0), holdingsRecord);
      if (holdingsRecord.containsKey("formerIds") && holdingsRecord.getJsonArray("formerIds").size()==1) {
        JsonArray items = holdingsRecord.getJsonArray("items");
        for (Object itemObject : items) {
          JsonObject item = (JsonObject) itemObject;
          itemsMap.put(item.getString("itemIdentifier"), item);
        }
      }
    }
  }

  /**
   * Creates a deep clone of a JSONArray from a JSONObject, removes the array from the source object and returns the clone
   * @param jsonObject Source object containing the array to extract
   * @param arrayName Property name of the array to extract
   * @return  The extracted JsonArray or an empty JsonArray if none found to extract.
   */
  private static JsonArray extractJsonArrayFromObject(JsonObject jsonObject, String arrayName)  {
    JsonArray array = new JsonArray();
    if (jsonObject.containsKey(arrayName)) {
      array = new JsonArray((jsonObject.getJsonArray(arrayName)).encode());
      jsonObject.remove(arrayName);
    }
    return array;
  }


  // Old method, only creates/updates instances
  private void updateInventory (OkapiClient okapiClient,
                                JsonObject candidateInstance,
                                JsonObject matchingInstances,
                                String queryString,
                                RoutingContext routingCtx) {
    int recordCount = matchingInstances.getInteger("totalRecords");
    if (recordCount == 0) {
      logger.info("Match query [" + queryString + "] did not find a matching instance. Will POST a new instance");
      postInstance(okapiClient, routingCtx, candidateInstance);
    }  else if (recordCount == 1) {
      logger.info("Match query [" + queryString + "] found a matching instance. Will PUT an instance update");
      JsonObject matchingInstance = matchingInstances.getJsonArray("instances").getJsonObject(0);
      // Update existing instance
      putInstance(okapiClient, routingCtx, candidateInstance, matchingInstance.getString("id"));
    } else if (recordCount > 1) {
      logger.info("Multiple matches (" + recordCount + ") found by match query [" + queryString + "], cannot determine which instance to update");
    } else {
      logger.info("Unexpected recordCount: ["+recordCount+"] cannot determine match");
    }
  }

  /**
   * Old method
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
      logger.info("Match query [" + matchQuery.getQueryString() + "] did not find a matching instance. Will POST a new instance");
      postInstance(okapiClient, routingCtx, candidateInstance);
    }  else if (recordCount == 1) {
      logger.info("Match query [" + matchQuery.getQueryString() + "] found a matching instance. Will PUT an instance update");
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
   * Old method (http methods generally moved to InventoryStorage class)
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
            logger.info("PUT of Instance succeeded");
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
   * Old method (http methods generally moved to InventoryStorage class)
   * Creates a new instance in Inventory
   * @param ctx
   * @param newInstance
   */
  private void postInstance (OkapiClient okapiClient, RoutingContext ctx, JsonObject newInstance) {
    okapiClient.post(INSTANCE_STORAGE_PATH, newInstance.toString(), postResult->{
      if (postResult.succeeded()) {
        logger.info("POST of Instance succeeded");
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


}
