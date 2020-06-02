/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventorymatch;

import static org.folio.okapi.common.HttpResponse.responseError;
import static org.folio.okapi.common.HttpResponse.responseJson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
  private static final String HOLDINGS_STORAGE_PATH = "/holdings-storage/holdings";
  private static final String ITEM_STORAGE_PATH = "/item-storage/items";

  public final static String INSTANCE_MATCH_PATH = "/instance-storage-match/instances"; // being deprecated
  public final static String INSTANCE_UPSERT_MATCHKEY_PATH = "/instance-storage-upsert-matchkey";
  public final static String INSTANCE_UPSERT_HRID_PATH = "/instance-storage-upsert-hrid";
  public final static String INVENTORY_UPSERT_HRID_PATH = "/inventory-upsert-hrid";


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
      InstanceQuery matchQuery = new MatchQuery(matchKey.getKey());
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

  // ===========================================
  // New inventory upsert by HRID

  public void handleInventoryUpsertByHrid (RoutingContext routingCtx) {
    String contentType = routingCtx.request().getHeader("Content-Type");
    if (contentType != null && !contentType.startsWith("application/json")) {
      responseError(routingCtx, 400, "Only accepts Content-Type application/json, was: "+ contentType);
    } else {
      OkapiClient okapiClient = getOkapiClient(routingCtx); // TODO: close it? when/where?
      String inventoryRecordSetAsString = routingCtx.getBodyAsString("UTF-8");
      JsonObject inventoryRecordSet = new JsonObject(inventoryRecordSetAsString);
      logger.info("Incoming record set " + inventoryRecordSet.encodePrettily());

      JsonObject incomingInstance         = inventoryRecordSet.getJsonObject("instance");
      JsonArray  incomingHoldingsAndItems = inventoryRecordSet.getJsonArray("holdingsRecords") == null ?
                                                         new JsonArray()
                                                         :
                                                         inventoryRecordSet.getJsonArray("holdingsRecords");

      InstanceQuery instanceQuery = new HridQuery(incomingInstance.getString("hrid"));

      Future<JsonObject> promisedExistingInstance = lookupExistingInstance(okapiClient, routingCtx, instanceQuery);
      promisedExistingInstance.onComplete( ar -> {
        if (ar.result()==null) {
          Future<JsonObject> promisedCreatedInstance = InventoryStorage.postInstance(okapiClient, incomingInstance);
          promisedCreatedInstance.onComplete( hndl -> {
            JsonObject persistedInstance = hndl.result();
            String instanceId = persistedInstance.getString("id");
            insertHoldingsAndItems(okapiClient, incomingHoldingsAndItems, instanceId);
            responseJson(routingCtx, 200).end(persistedInstance.encodePrettily());
          });
        } else {
          String instanceId = ar.result().getString("id");
          incomingInstance.put("id", instanceId);
          Future<JsonObject> promisedUpdatedInstance = InventoryStorage.putInstance(okapiClient, incomingInstance, instanceId);
          promisedUpdatedInstance.onComplete( hndl -> {
            Future<JsonArray> promisedExistingHoldingsAndItems = lookupExistingHoldingsRecordsAndItems(okapiClient, instanceId);
            promisedExistingHoldingsAndItems.onComplete( existingHoldingsResult -> {
              if (existingHoldingsResult != null) {
                upsertHoldingsAndItems(okapiClient, existingHoldingsResult.result(), incomingHoldingsAndItems, instanceId);
              } else {
                // No existing holdings and items - persist incoming records
                insertHoldingsAndItems(okapiClient, incomingHoldingsAndItems, instanceId);
              }
            });
            responseJson(routingCtx, 200).end(hndl.result().encodePrettily());
          });
        }
      });
    }
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
      insertHoldingsRecordWithItems(okapiClient, holdingsRecord, instanceId);
    }
  }

  private void insertHoldingsRecordWithItems (OkapiClient okapiClient, JsonObject holdingsRecord, String instanceId) {
    holdingsRecord.put("instanceId", instanceId);
    JsonArray items = extractJsonArrayFromObject(holdingsRecord, "items");
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
      insertItem(okapiClient, item, holdingsRecordId);
    }
  }

  private void insertItem (OkapiClient okapiClient, JsonObject item, String holdingsRecordId) {
    item.put("holdingsRecordId", holdingsRecordId);
    Future<JsonObject> promisedCreatedItem = InventoryStorage.postItem(okapiClient, item);
    promisedCreatedItem.onComplete( hndl -> {
      JsonObject createdItem = hndl.result();
      if (createdItem == null) {
        logger.info("Something went wrong with inserting the item");
      } else {
        String barcode = createdItem.getString("barcode");
        logger.info("Item created, barcode " + barcode);
      }
    });
  }

  private Future<JsonObject> lookupExistingInstance (OkapiClient okapiClient, RoutingContext routingCtx, InstanceQuery instanceQuery) {
    Promise<JsonObject> promise = Promise.promise();
    okapiClient.get(INSTANCE_STORAGE_PATH+"?query="+instanceQuery.getURLEncodedQueryString(), res-> {
      if ( res.succeeded()) {
        JsonObject matchingInstances = new JsonObject(res.result());
        int recordCount = matchingInstances.getInteger("totalRecords");
        if (recordCount > 0) {
          promise.complete(matchingInstances.getJsonArray("instances").getJsonObject(0));
        } else {
          promise.complete(null);
        }
      } else {
        String message = res.cause().getMessage();
        responseError(routingCtx, 500, "mod-inventory-storage failed with " + message);  // TODO: Create failed-record response
      }
    });
    return promise.future();
  }

  @SuppressWarnings("rawtypes")
  private Future<JsonArray> lookupExistingHoldingsRecordsAndItems (OkapiClient okapiClient, String instanceId) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(HOLDINGS_STORAGE_PATH+"?limit=1000&query=instanceId%3D%3D"+instanceId, res-> {
      if (res.succeeded()) {
        JsonObject holdingsRecordsResult = new JsonObject(res.result());
        JsonArray holdingsRecords = holdingsRecordsResult.getJsonArray("holdingsRecords");
        logger.info("Successfully looked up existing holdings records, found  " + holdingsRecords.size());
        List<Future> itemFutures = new ArrayList<Future>();
        for (Object holdingsObject : holdingsRecords) {
          JsonObject holdingsRecord = (JsonObject) holdingsObject;
          itemFutures.add(lookupAndEmbedExistingItems(okapiClient, holdingsRecord));
        }
        CompositeFuture.all(itemFutures).onComplete( result -> {
          if (result.succeeded()) {
            logger.info("Composite succeeded with " + result.result().size() + " result(s). First item: " + result.result().resultAt(0));
            promise.complete(holdingsRecords);
          }
        });
      } else {
        promise.complete(null);
        logger.info("Oops - holdings records lookup failed");
      }
    });
    return promise.future();
  }

  private Future<JsonArray> lookupAndEmbedExistingItems (OkapiClient okapiClient, JsonObject holdingsRecord) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(ITEM_STORAGE_PATH+"?limit=1000&query=holdingsRecordId%3D%3D"+holdingsRecord.getString("id"), res-> {
      if (res.succeeded()) {
        JsonObject itemsResult = new JsonObject(res.result());
        JsonArray items = itemsResult.getJsonArray("items");
        logger.info("Successfully looked up existing items, found  " + items.size());
        holdingsRecord.put("items",items);
        promise.complete(items);
      } else {
        promise.complete(null);
        logger.info("Oops - items lookup failed");
      }
    });
    return promise.future();
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

  // New Inventory upsert by HRID ends
  // ======================================================

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
                               InstanceQuery matchQuery,
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
