package org.folio.inventorymatch;

import java.util.ArrayList;
import java.util.List;

import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Static methods making low level HTTP request to create and update records in Inventory Storage.
 */
public class InventoryStorage {
    private static final Logger logger = LoggerFactory.getLogger("inventory-matcher");
    private static final String INSTANCE_STORAGE_PATH = "/instance-storage/instances";
    private static final String HOLDINGS_STORAGE_PATH = "/holdings-storage/holdings";
    private static final String ITEM_STORAGE_PATH = "/item-storage/items";

    public static Future<JsonObject> postInstance (OkapiClient okapiClient, JsonObject newInstance) {
        Promise<JsonObject> promise = Promise.promise();
        okapiClient.post(INSTANCE_STORAGE_PATH, newInstance.toString(), postResult->{
          if (postResult.succeeded()) {
            String instanceResult = postResult.result();
            JsonObject instanceResponseJson = new JsonObject(instanceResult);
            promise.complete(instanceResponseJson);
          } else {
            JsonObject errorMessage = new JsonObject(postResult.cause().getMessage());
            errorMessage.put("entity-type","instance");
            errorMessage.put("operation", "create");
            promise.fail(errorMessage.encodePrettily());
          }
        });
        return promise.future();
    }

    public static Future<JsonObject> putInstance (OkapiClient okapiClient, JsonObject newInstance, String instanceId) {
        Promise<JsonObject> promise = Promise.promise();
        logger.debug("Putting instance " + newInstance.encodePrettily());
        okapiClient.request(HttpMethod.PUT, INSTANCE_STORAGE_PATH+"/"+instanceId, newInstance.toString(), putResult-> {
          if (putResult.succeeded()) {
            JsonObject done = new JsonObject("{ \"message\": \"done\" }");
            promise.complete(done);
          } else {
            JsonObject errorMessage = new JsonObject(putResult.cause().getMessage());
            errorMessage.put("entity-type","instance");
            errorMessage.put("operation", "update");
            promise.fail(errorMessage.encodePrettily());
          }
        });
        return promise.future();
    }

    public static Future<JsonObject> postHoldingsRecord(OkapiClient okapiClient, JsonObject holdingsRecord) {
        Promise<JsonObject> promise = Promise.promise();
        okapiClient.post(HOLDINGS_STORAGE_PATH, holdingsRecord.toString(), postResult->{
          if (postResult.succeeded()) {
            logger.info("POST of holdings record succeeded");
            String holdingsResult = postResult.result();
            JsonObject holdingsResponseJson = new JsonObject(holdingsResult);
            promise.complete(holdingsResponseJson);
          } else {
            JsonObject errorMessage = new JsonObject(postResult.cause().getMessage());
            errorMessage.put("entity-type","holdingsRecord");
            errorMessage.put("operation", "create");
            promise.fail(errorMessage.encodePrettily());
          }
        });
        return promise.future();
      }

      public static Future<JsonObject> putHoldingsRecord(OkapiClient okapiClient, JsonObject holdingsRecord, String uuid) {
        Promise<JsonObject> promise = Promise.promise();
        okapiClient.request(HttpMethod.PUT, HOLDINGS_STORAGE_PATH + "/" + uuid, holdingsRecord.toString(), putResult->{
          if (putResult.succeeded()) {
            JsonObject done = new JsonObject("{ \"message\": \"done\" }");
            promise.complete(done);
          } else {
            JsonObject errorMessage = new JsonObject(putResult.cause().getMessage());
            errorMessage.put("entity-type","holdingsRecord");
            errorMessage.put("operation", "update");
            promise.fail(errorMessage.encodePrettily());
          }
        });
        return promise.future();
      }

      public static Future<JsonObject> postItem(OkapiClient okapiClient, JsonObject item) {
        Promise<JsonObject> promise = Promise.promise();
        okapiClient.post(ITEM_STORAGE_PATH, item.toString(), postResult->{
          if (postResult.succeeded()) {
            String itemResult = postResult.result();
            JsonObject itemResponseJson = new JsonObject(itemResult);
            promise.complete(itemResponseJson);
          } else {
            JsonObject errorMessage = new JsonObject(postResult.cause().getMessage());
            errorMessage.put("entity-type","item");
            errorMessage.put("operation", "create");
            promise.fail(errorMessage.encodePrettily());
          }
        });
        return promise.future();
      }

      public static Future<JsonObject> putItem(OkapiClient okapiClient, JsonObject item, String uuid) {
        Promise<JsonObject> promise = Promise.promise();
        okapiClient.request(HttpMethod.PUT, ITEM_STORAGE_PATH + "/" + uuid, item.toString(), putResult->{
          if (putResult.succeeded()) {
            JsonObject done = new JsonObject("{ \"message\": \"done\" }");
            promise.complete(done);
          } else {
            JsonObject errorMessage = new JsonObject(putResult.cause().getMessage());
            errorMessage.put("entity-type","item");
            errorMessage.put("operation", "update");
            promise.fail(errorMessage.encodePrettily());
          }
        });
        return promise.future();
    }

    public static Future<JsonObject> lookupInstance (OkapiClient okapiClient, InventoryQuery inventoryQuery) {
      Promise<JsonObject> promise = Promise.promise();
      okapiClient.get(INSTANCE_STORAGE_PATH+"?query="+inventoryQuery.getURLEncodedQueryString(), res-> {
        if ( res.succeeded()) {
          JsonObject matchingInstances = new JsonObject(res.result());
          int recordCount = matchingInstances.getInteger("totalRecords");
          if (recordCount == 1) {
            promise.complete(matchingInstances.getJsonArray("instances").getJsonObject(0));
          } else {
            promise.complete(null);
          }
        } else {
          JsonObject errorMessage = new JsonObject(res.cause().getMessage());
          errorMessage.put("entity-type","instance");
          errorMessage.put("operation", "get-single");
          promise.fail(errorMessage.encodePrettily());
      }
      });
      return promise.future();
    }

    public static Future<JsonObject> lookupHoldingsRecordByHRID (OkapiClient okapiClient, InventoryQuery hridQuery) {
      Promise<JsonObject> promise = Promise.promise();
      okapiClient.get(HOLDINGS_STORAGE_PATH+"?query="+hridQuery.getURLEncodedQueryString(), res-> {
        if ( res.succeeded()) {
          JsonObject records = new JsonObject(res.result());
          int recordCount = records.getInteger("totalRecords");
          if (recordCount == 1) {
            promise.complete(records.getJsonArray("holdingsRecords").getJsonObject(0));
          } else {
            promise.complete(null);
          }
        } else {
          JsonObject errorMessage = new JsonObject(res.cause().getMessage());
          errorMessage.put("entity-type","holdingsRecord");
          errorMessage.put("operation", "get-single");
          promise.fail(errorMessage.encodePrettily());
      }
      });
      return promise.future();
    }

    public static Future<JsonObject> lookupItemByHRID (OkapiClient okapiClient, InventoryQuery hridQuery) {
      Promise<JsonObject> promise = Promise.promise();
      okapiClient.get(ITEM_STORAGE_PATH+"?query="+hridQuery.getURLEncodedQueryString(), res-> {
        if ( res.succeeded()) {
          JsonObject records = new JsonObject(res.result());
          int recordCount = records.getInteger("totalRecords");
          if (recordCount == 1) {
            promise.complete(records.getJsonArray("items").getJsonObject(0));
          } else {
            promise.complete(null);
          }
        } else {
          JsonObject errorMessage = new JsonObject(res.cause().getMessage());
          errorMessage.put("entity-type","item");
          errorMessage.put("operation", "get-single");
          promise.fail(errorMessage.encodePrettily());
      }
      });
      return promise.future();
    }

    public static Future<JsonObject> lookupInventoryRecordSetByInstanceHRID (OkapiClient okapiClient, String instanceHrid) {
      Promise<JsonObject> promise = Promise.promise();
      InventoryQuery hridQuery = new HridQuery(instanceHrid);
      Future<JsonObject> promisedExistingInstance = lookupInstance(okapiClient, hridQuery);
      promisedExistingInstance.onComplete( ar -> {
        if (ar.result()==null) {
          promise.complete(null);
        } else {
          JsonObject instance = ar.result();
          String instanceUUID = instance.getString("id");
          JsonObject inventoryRecordSet = new JsonObject();
          inventoryRecordSet.put("instance",instance);
          Future<JsonArray> promisedExistingHoldingsAndItems =
                      lookupExistingHoldingsRecordsAndItemsByInstanceUUID(okapiClient, instanceUUID);
          promisedExistingHoldingsAndItems.onComplete( existingHoldingsResult -> {
              if (existingHoldingsResult.succeeded()) {
                  if (existingHoldingsResult.result() != null) {
                      inventoryRecordSet.put("holdingsRecords",existingHoldingsResult.result());
                  } else {
                      inventoryRecordSet.put("holdingsRecords", new JsonArray());
                  }
                  promise.complete(inventoryRecordSet);
              } else {
                  promise.fail(existingHoldingsResult.cause());
              }
          });
        }
      });
      return promise.future();
    }

    public static Future<JsonArray> lookupExistingHoldingsRecordsAndItemsByInstanceUUID (OkapiClient okapiClient, String instanceId) {
      Promise<JsonArray> promise = Promise.promise();
      okapiClient.get(HOLDINGS_STORAGE_PATH+"?limit=1000&query=instanceId%3D%3D"+instanceId, res-> {
        if (res.succeeded()) {
          JsonObject holdingsRecordsResult = new JsonObject(res.result());
          JsonArray holdingsRecords = holdingsRecordsResult.getJsonArray("holdingsRecords");
          logger.info("Successfully looked up existing holdings records, found  " + holdingsRecords.size());
          @SuppressWarnings("rawtypes")
          List<Future> itemFutures = new ArrayList<Future>();
          for (Object holdingsObject : holdingsRecords) {
            JsonObject holdingsRecord = (JsonObject) holdingsObject;
            itemFutures.add(lookupAndEmbedExistingItems(okapiClient, holdingsRecord));
          }
          CompositeFuture.all(itemFutures).onComplete( result -> {
            if (result.succeeded()) {
              logger.info("Composite succeeded with " + result.result().size() + " result(s). First item: " + ((JsonObject) result.result().resultAt(0)).encodePrettily());
              promise.complete(holdingsRecords);
            }
          });
        } else {
          // TODO: fail it instead
          promise.complete(null);
          logger.info("Oops - holdings records lookup failed");
        }
      });
      return promise.future();
    }

    private static Future<JsonObject> lookupAndEmbedExistingItems (OkapiClient okapiClient, JsonObject holdingsRecord) {
      Promise<JsonObject> promise = Promise.promise();
      okapiClient.get(ITEM_STORAGE_PATH+"?limit=1000&query=holdingsRecordId%3D%3D"+holdingsRecord.getString("id"), res-> {
        if (res.succeeded()) {
          JsonObject itemsResult = new JsonObject(res.result());
          JsonArray items = itemsResult.getJsonArray("items");
          logger.info("Successfully looked up existing items, found  " + items.size());
          holdingsRecord.put("items",items);
          promise.complete(holdingsRecord);
        } else {
          // TODO: fail
          promise.complete(null);
          logger.info("Oops - items lookup failed");
        }
      });
      return promise.future();
    }


}