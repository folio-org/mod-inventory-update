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
    private static final String LOCATIONS_STORAGE_PATH = "/locations";

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

    public static Future<Void> deleteInstance (OkapiClient okapiClient, String instanceId) {
      Promise<Void> promise = Promise.promise();
      okapiClient.delete(INSTANCE_STORAGE_PATH+"/"+instanceId, deleteResult-> {
        if (deleteResult.succeeded()) {
          promise.complete();
        } else {
          JsonObject errorMessage = new JsonObject(deleteResult.cause().getMessage());
          errorMessage.put("entity-type","instance");
          errorMessage.put("operation", "delete");
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
            logger.debug("Done putting holdings record " + holdingsRecord.encodePrettily());
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

      public static Future<Void> deleteHoldingsRecord(OkapiClient okapiClient, String holdingsRecordId) {
        Promise<Void> promise = Promise.promise();
        okapiClient.delete(HOLDINGS_STORAGE_PATH + "/" + holdingsRecordId, deleteResult->{
          if (deleteResult.succeeded()) {
            promise.complete();
          } else {
            JsonObject errorMessage = new JsonObject(deleteResult.cause().getMessage());
            errorMessage.put("entity-type","holdingsRecord");
            errorMessage.put("operation", "delete");
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
        logger.debug("Putting item " + uuid+ ": " + item.encodePrettily());
        okapiClient.request(HttpMethod.PUT, ITEM_STORAGE_PATH + "/" + uuid, item.toString(), putResult->{
          if (putResult.succeeded()) {
            okapiClient.get(ITEM_STORAGE_PATH + "/" + uuid, getResult -> {
              if (getResult.succeeded()) {
                JsonObject done = new JsonObject(getResult.result());
                logger.debug("Done putting item " + done.encodePrettily());
                promise.complete(done);
              } else {
                promise.fail("Error attempting to get confirmation for PUT of item: " + getResult.cause().getMessage());
              }
            });
          } else {
            JsonObject errorMessage = new JsonObject(putResult.cause().getMessage());
            errorMessage.put("entity-type","item");
            errorMessage.put("operation", "update");
            promise.fail(errorMessage.encodePrettily());
          }
        });
        return promise.future();
    }

    public static Future<Void> deleteItem(OkapiClient okapiClient, String itemId) {
      Promise<Void> promise = Promise.promise();
      okapiClient.delete(ITEM_STORAGE_PATH + "/" + itemId, deleteResult->{
        if (deleteResult.succeeded()) {
          promise.complete();
        } else {
          JsonObject errorMessage = new JsonObject(deleteResult.cause().getMessage());
          errorMessage.put("entity-type","item");
          errorMessage.put("operation", "delete");
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

    public static Future<JsonObject> lookupSingleInventoryRecordSet (OkapiClient okapiClient, InventoryQuery uniqueQuery) {
      Promise<JsonObject> promise = Promise.promise();
      Future<JsonObject> promisedExistingInstance = lookupInstance(okapiClient, uniqueQuery);
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
          if (holdingsRecords.size()>0) {
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
            promise.complete(null);
          }
        } else {
          promise.fail("There was an error looking up existing holdings and items");
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
          promise.fail("Error occurred when attempting to look up existing items");
        }
      });
      return promise.future();
    }

    public static Future<JsonArray> getLocations(OkapiClient okapiClient)  {
      Promise<JsonArray> promise = Promise.promise();
      okapiClient.get(LOCATIONS_STORAGE_PATH + "?limit=9999", locs -> {
        if (locs.succeeded()) {
          JsonObject response = new JsonObject(locs.result());
          JsonArray locationsJson = response.getJsonArray("locations");
          promise.complete(locationsJson);
        }  else {
          promise.fail("An error occurred when attempting to retrieve locations from Inventory storage");
        }
      });
      return promise.future();
    }


}