package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import org.folio.inventoryupdate.entities.InventoryRecord;
import org.folio.inventoryupdate.entities.InventoryRecord.Entity;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Static methods making low level HTTP request to create and update records in Inventory Storage.
 */
public class InventoryStorage {

  private static final Logger logger = LoggerFactory.getLogger("inventory-update");
  private static final String INSTANCE_STORAGE_PATH = "/instance-storage/instances";
  private static final String INSTANCE_RELATIONSHIPS_STORAGE_PATH = "/instance-storage/instance-relationships";
  private static final String HOLDINGS_STORAGE_PATH = "/holdings-storage/holdings";
  private static final String ITEM_STORAGE_PATH = "/item-storage/items";
  private static final String LOCATIONS_STORAGE_PATH = "/locations";


  public static Future<JsonObject> postInventoryRecord (OkapiClient okapiClient, InventoryRecord record) {
    Promise<JsonObject> promise = Promise.promise();
    okapiClient.post(getApi(record.entityType()), record.asJsonString(), postResult -> {
      if (postResult.succeeded()) {
        String result = postResult.result();
        JsonObject responseJson = new JsonObject(result);
        record.complete();
        promise.complete(responseJson);
      } else {
        record.fail();
        record.skipDependants();
        record.logError(okapiClient.getResponsebody(), okapiClient.getStatusCode());
        promise.fail(record.getError().encodePrettily());
      }
    });
    return promise.future();
  }

  public static Future<JsonObject> putInventoryRecord (OkapiClient okapiClient, InventoryRecord record) {
    Promise<JsonObject> promise = Promise.promise();
    logger.debug("Putting " + record.entityType() + ": " + record.asJson().encodePrettily());
    okapiClient.request(HttpMethod.PUT, getApi(record.entityType())+"/"+record.getUUID(), record.asJsonString(), putResult -> {
      if (putResult.succeeded()) {
        record.complete();
        promise.complete(record.asJson());
      } else {
        record.fail();
        record.logError(okapiClient.getResponsebody(), okapiClient.getStatusCode());
        promise.fail(record.getError().encodePrettily());
      }
    });
    return promise.future();
  }

  public static Future<JsonObject> deleteInventoryRecord (OkapiClient okapiClient, InventoryRecord record) {
    Promise<JsonObject> promise = Promise.promise();
    okapiClient.delete(getApi(record.entityType())+"/"+record.getUUID(), deleteResult -> {
      if (deleteResult.succeeded()) {
        record.complete();
        promise.complete();
      } else {
        record.fail();
        record.logError(deleteResult.cause().getMessage(), okapiClient.getStatusCode());
        promise.fail(record.getError().encodePrettily());
      }
    });
    return promise.future();

  }

  public static Future<JsonObject> lookupInstance (OkapiClient okapiClient, InventoryQuery inventoryQuery) {
    Promise<JsonObject> promise = Promise.promise();
    okapiClient.get(INSTANCE_STORAGE_PATH+"?query="+inventoryQuery.getURLEncodedQueryString(), res -> {
      if ( res.succeeded()) {
        JsonObject matchingInstances = new JsonObject(res.result());
        int recordCount = matchingInstances.getInteger("totalRecords");
        if (recordCount == 1) {
          promise.complete(matchingInstances.getJsonArray("instances").getJsonObject(0));
        } else {
          promise.complete(null);
        }
      } else {
        failure(res.cause(), Entity.INSTANCE, Transaction.GET, okapiClient.getStatusCode(), promise);
    }
    });
    return promise.future();
  }

  public static Future<JsonObject> lookupHoldingsRecordByHRID (OkapiClient okapiClient, InventoryQuery hridQuery) {
    Promise<JsonObject> promise = Promise.promise();
    okapiClient.get(HOLDINGS_STORAGE_PATH+"?query="+hridQuery.getURLEncodedQueryString(), res -> {
      if ( res.succeeded()) {
        JsonObject records = new JsonObject(res.result());
        int recordCount = records.getInteger("totalRecords");
        if (recordCount == 1) {
          promise.complete(records.getJsonArray("holdingsRecords").getJsonObject(0));
        } else {
          promise.complete(null);
        }
      } else {
        failure(res.cause(), Entity.HOLDINGS_RECORD, Transaction.GET, okapiClient.getStatusCode(), promise);
    }
    });
    return promise.future();
  }

  public static Future<JsonObject> lookupItemByHRID (OkapiClient okapiClient, InventoryQuery hridQuery) {
    Promise<JsonObject> promise = Promise.promise();
    okapiClient.get(ITEM_STORAGE_PATH+"?query="+hridQuery.getURLEncodedQueryString(), res -> {
      if ( res.succeeded()) {
        JsonObject records = new JsonObject(res.result());
        int recordCount = records.getInteger("totalRecords");
        if (recordCount == 1) {
          promise.complete(records.getJsonArray("items").getJsonObject(0));
        } else {
          promise.complete(null);
        }
      } else {
        failure(res.cause(), Entity.ITEM, Transaction.GET, okapiClient.getStatusCode(), promise);
      }
    });
    return promise.future();
  }

  public static Future<JsonObject> lookupSingleInventoryRecordSet (OkapiClient okapiClient, InventoryQuery uniqueQuery) {
    Promise<JsonObject> promise = Promise.promise();
    Future<JsonObject> promisedExistingInstance = lookupInstance(okapiClient, uniqueQuery);
    promisedExistingInstance.onComplete( instanceResult -> {
      if (instanceResult.succeeded()) {
        if (instanceResult.result()==null) {
          promise.complete(null);
        } else {
          JsonObject instance = instanceResult.result();
          String instanceUUID = instance.getString("id");
          JsonObject inventoryRecordSet = new JsonObject();
          inventoryRecordSet.put("instance",instance);
          // GBV-106 have method for lookup and put in inventory record set?
          Future<JsonArray> promisedInstanceRelationships =
                      lookupExistingInstanceRelationshipsByInstanceUUID(okapiClient, instanceUUID);
          // GBV-106 composite join? No, JsonObject not thread safe
          // GBV-106 extract this one too to lookup and embed method?
          Future<JsonArray> promisedExistingHoldingsAndItems =
                  lookupExistingHoldingsRecordsAndItemsByInstanceUUID(okapiClient, instanceUUID);
          promisedExistingHoldingsAndItems.onComplete(existingHoldingsResult -> {
              if (existingHoldingsResult.succeeded()) {
                  if (existingHoldingsResult.result() != null) {
                      inventoryRecordSet.put("holdingsRecords",existingHoldingsResult.result());
                  } else {
                      inventoryRecordSet.put("holdingsRecords", new JsonArray());
                  }
                  promise.complete(inventoryRecordSet);
              } else {
                failure(existingHoldingsResult.cause(), Entity.HOLDINGS_RECORD, Transaction.GET, okapiClient.getStatusCode(), promise, "While looking up Inventory record set");
              }
          });
        }
      } else {
        failure(instanceResult.cause(), Entity.INSTANCE, Transaction.GET, okapiClient.getStatusCode(), promise);
      }
    });
    return promise.future();
  }

  public static Future<JsonArray> lookupExistingHoldingsRecordsAndItemsByInstanceUUID (OkapiClient okapiClient, String instanceId) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(HOLDINGS_STORAGE_PATH+"?limit=1000&query=instanceId%3D%3D"+instanceId, res -> {
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
            } else {
              failure(result.cause(), Entity.ITEM, Transaction.GET, okapiClient.getStatusCode(), promise, "While looking up and embedding items for holdings records");
            }
          });
        } else {
          promise.complete(null);
        }
      } else {
        failure(res.cause(), Entity.HOLDINGS_RECORD, Transaction.GET, okapiClient.getStatusCode(), promise, "While looking up holdings by instance ID");
        promise.fail("There was an error looking up existing holdings and items");
      }
    });
    return promise.future();
  }

  private static Future<JsonObject> lookupAndEmbedExistingItems (OkapiClient okapiClient, JsonObject holdingsRecord) {
    Promise<JsonObject> promise = Promise.promise();
    okapiClient.get(ITEM_STORAGE_PATH+"?limit=1000&query=holdingsRecordId%3D%3D"+holdingsRecord.getString("id"), res -> {
      if (res.succeeded()) {
        JsonObject itemsResult = new JsonObject(res.result());
        JsonArray items = itemsResult.getJsonArray("items");
        logger.info("Successfully looked up existing items, found  " + items.size());
        holdingsRecord.put("items",items);
        promise.complete(holdingsRecord);
      } else {
        failure(res.cause(), Entity.ITEM, Transaction.GET, okapiClient.getStatusCode(), promise, "While looking up items by holdingsRecordId");
      }
    });
    return promise.future();
  }

  // GBV-106?  Lookup the instances -- to get hrid, matchkey, etc?. Not here but in lookupAndEmbed?
  public static Future<JsonArray> lookupExistingInstanceRelationshipsByInstanceUUID (OkapiClient okapiClient, String instanceId) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(INSTANCE_RELATIONSHIPS_STORAGE_PATH+"?limit=1000&query=(subInstanceId%3D%3D"+instanceId+"%20or%20superInstanceId%3D%3D"+instanceId+")", res -> {
      if (res.succeeded()) {
        JsonObject relationshipsResult = new JsonObject(res.result());
        JsonArray instanceRelationships = relationshipsResult.getJsonArray("instanceRelationships");
        logger.info("Successfully looked up existing items, found  " + instanceRelationships.size());
        promise.complete(instanceRelationships);
      } else {
        failure(res.cause(), Entity.ITEM, Transaction.GET, okapiClient.getStatusCode(), promise, "While looking instance relationships");
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
        failure(locs.cause(), Entity.LOCATION, Transaction.GET, okapiClient.getStatusCode(), promise);
      }
    });
    return promise.future();
  }

  private static String getApi(Entity entityType) {
    String api = "";
    switch (entityType) {
      case INSTANCE:
        api = INSTANCE_STORAGE_PATH;
        break;
      case HOLDINGS_RECORD:
        api = HOLDINGS_STORAGE_PATH;
        break;
      case ITEM:
        api = ITEM_STORAGE_PATH;
        break;
      case LOCATION:
        break;
      default:
        break;
    }
    return api;
  }

  private static <T> void failure(Throwable cause, Entity entityType, Transaction transaction, int httpStatusCode, Promise<T> promise) {
    failure(cause, entityType, transaction, httpStatusCode, promise, null);
  }

  private static <T> void failure(Throwable cause, Entity entityType, Transaction transaction, int httpStatusCode, Promise<T> promise, String contextNote) {
    JsonObject errorMessage = new JsonObject();
    errorMessage.put("message", cause.getMessage());
    errorMessage.put("entity-type",entityType);
    errorMessage.put("operation", transaction);
    errorMessage.put("http-status-code", httpStatusCode);
    if (contextNote != null) {
      errorMessage.put("note-of-context", contextNote);
    }
    promise.fail(errorMessage.encodePrettily());
  }

}