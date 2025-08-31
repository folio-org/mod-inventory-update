package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.HoldingsRecord;
import org.folio.inventoryupdate.entities.Instance;
import org.folio.inventoryupdate.entities.InstanceReferences;
import org.folio.inventoryupdate.entities.InventoryRecord;
import org.folio.inventoryupdate.entities.InventoryRecord.Entity;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.inventoryupdate.entities.InventoryRecordSet;
import org.folio.inventoryupdate.entities.Item;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.okapi.common.WebClientFactory;

/**
 * Static methods making low level HTTP requests to create and update records in Inventory Storage.
 */
public class InventoryStorage {

  private static final Logger logger = LogManager.getLogger("inventory-update");
  @SuppressWarnings("java:S1075")  // suppress "URIs should not be hardcoded"
  private static final String INSTANCE_STORAGE_PATH = "/instance-storage/instances";
  @SuppressWarnings("java:S1075")  // suppress "URIs should not be hardcoded"
  private static final String INSTANCE_SET_PATH = "/inventory-view/instance-set";
  @SuppressWarnings("java:S1075")  // suppress "URIs should not be hardcoded"
  public static final String INSTANCE_STORAGE_BATCH_PATH = "/instance-storage/batch/synchronous";
  @SuppressWarnings("java:S1075")  // suppress "URIs should not be hardcoded"
  private static final String INSTANCE_RELATIONSHIP_STORAGE_PATH = "/instance-storage/instance-relationships";
  @SuppressWarnings("java:S1075")  // suppress "URIs should not be hardcoded"
  private static final String PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH = "/preceding-succeeding-titles";
  @SuppressWarnings("java:S1075")  // suppress "URIs should not be hardcoded"
  private static final String HOLDINGS_STORAGE_PATH = "/holdings-storage/holdings";
  @SuppressWarnings("java:S1075")  // suppress "URIs should not be hardcoded"
  public static final String HOLDINGS_STORAGE_BATCH_PATH = "/holdings-storage/batch/synchronous";
  @SuppressWarnings("java:S1075")  // suppress "URIs should not be hardcoded"
  private static final String ITEM_STORAGE_PATH = "/item-storage/items";
  @SuppressWarnings("java:S1075")  // suppress "URIs should not be hardcoded"
  public static final String ITEM_STORAGE_BATCH_PATH = "/item-storage/batch/synchronous";
  @SuppressWarnings("java:S1075")  // suppress "URIs should not be hardcoded"
  private static final String LOCATION_STORAGE_PATH = "/locations";

  // Property keys, JSON responses
  public static final String ID = "id";
  public static final String INSTANCES = "instances";
  public static final String TOTAL_RECORDS = "totalRecords";
  public static final String HOLDINGS_RECORDS = "holdingsRecords";
  public static final String ITEMS = "items";
  public static final String LOCATIONS = "locations";

  private static final String X_OKAPI_TOKEN = "X-Okapi-Token";
  private static final String X_OKAPI_TENANT = "X-Okapi-Tenant";
  private static final String X_OKAPI_REQUEST_ID = "X-Okapi-Request-Id";

  public static Future<JsonObject> postInventoryRecord (OkapiClient okapiClient, InventoryRecord inventoryRecord) {
    Promise<JsonObject> promise = Promise.promise();
    okapiClient.post(getApi(inventoryRecord.entityType()), inventoryRecord.asJsonString(), postResult -> {
      if (postResult.succeeded()) {
        String result = postResult.result();
        JsonObject responseJson = new JsonObject(result);
        inventoryRecord.complete();
        promise.complete(responseJson);
      } else {
        inventoryRecord.fail();
        inventoryRecord.skipDependants();
        inventoryRecord.logError(okapiClient.getResponsebody(), okapiClient.getStatusCode(), ErrorReport.ErrorCategory.STORAGE, inventoryRecord.getOriginJson());
        promise.fail(inventoryRecord.getErrorAsJson().encodePrettily());
      }
    });
    return promise.future();
  }

  public static Future<Void> postInstances(OkapiClient okapiClient, List<Instance> records) {
    return postInventoryRecords(okapiClient, new ArrayList<>(records), INSTANCES);
  }

  public static Future<Void> postHoldingsRecords (OkapiClient okapiClient, List<HoldingsRecord> records) {
    return postInventoryRecords(okapiClient, new ArrayList<>(records), HOLDINGS_RECORDS);
  }

  public static Future<Void> postItems (OkapiClient okapiClient, List<Item> records) {
    return postInventoryRecords(okapiClient, new ArrayList<>(records), ITEMS);
  }

  private static Future<Void> postInventoryRecords (OkapiClient okapiClient, List<InventoryRecord> records, String arrayName) {
    Promise<Void> promise = Promise.promise();
    if (!records.isEmpty()) {
      JsonObject request = new JsonObject();
      request.put(arrayName, jsonArrayFromInventoryRecordList(records));
      logger.debug("Posting request {}: to {}", request.encodePrettily(), getBatchApi(arrayName));
      okapiClient.post(getBatchApi(arrayName) + "?upsert=true", request.encode(), postResult -> {
        if (postResult.succeeded()) {
          for (InventoryRecord inventoryRecord : records) {
            inventoryRecord.complete();
          }
          promise.complete();
        } else {
          for (InventoryRecord inventoryRecord : records) {
            inventoryRecord.fail();
            inventoryRecord.skipDependants();
            inventoryRecord.logError(
                    okapiClient.getResponsebody(),
                    okapiClient.getStatusCode(),
                    (records.size()>1 ? ErrorReport.ErrorCategory.BATCH_STORAGE : ErrorReport.ErrorCategory.STORAGE),
                    inventoryRecord.getOriginJson()
            );
          }
          promise.fail(records.get(0).getErrorAsJson().encodePrettily());
        }
      });
    } else {
      promise.complete();
    }
    return promise.future();
  }

  private static JsonArray jsonArrayFromInventoryRecordList (List<InventoryRecord> records) {
    JsonArray array = new JsonArray();
    for (InventoryRecord inventoryRecord : records) {
      array.add(inventoryRecord.asJson());
    }
    return array;
  }

  public static Future<JsonObject> putInventoryRecord (OkapiClient okapiClient, InventoryRecord inventoryRecord) {
    Promise<JsonObject> promise = Promise.promise();
    logger.debug("Putting {}: {}", inventoryRecord.entityType(), inventoryRecord.asJson().encodePrettily());
    okapiClient.request(HttpMethod.PUT, getApi(inventoryRecord.entityType())+"/"+inventoryRecord.getUUID(), inventoryRecord.asJsonString(), putResult -> {
      if (putResult.succeeded()) {
        inventoryRecord.complete();
        promise.complete(inventoryRecord.asJson());
      } else {
        inventoryRecord.fail();
        inventoryRecord.logError(okapiClient.getResponsebody(), okapiClient.getStatusCode(), ErrorReport.ErrorCategory.STORAGE, inventoryRecord.getOriginJson());
        promise.fail(inventoryRecord.getErrorAsJson().encodePrettily());
      }
    });
    return promise.future();
  }

  /**
   * Loops back to storage with a record update, beneath the ordinary update logic.
   * Currently used for setting statistical codes when a delete request is skipped for the record.
   * Will not count as an update, and will not throw an error if the PUT fails (it can for example fail for attempting
   * to set non-compliant UUIDs for statistical codes).
   */
  public static Future<JsonObject> putInventoryRecordOutcomeLess (OkapiClient okapiClient, InventoryRecord inventoryRecord) {
    Promise<JsonObject> promise = Promise.promise();
    logger.debug("Putting {}: {}", inventoryRecord.entityType(), inventoryRecord.asJson().encodePrettily());
    okapiClient.request(HttpMethod.PUT, getApi(inventoryRecord.entityType())+"/"+inventoryRecord.getUUID(), inventoryRecord.asJsonString(), putResult -> {
      if (putResult.failed()) {
        inventoryRecord.logError(okapiClient.getResponsebody(), okapiClient.getStatusCode(), ErrorReport.ErrorCategory.STORAGE, inventoryRecord.getOriginJson());
      }
      promise.complete(inventoryRecord.asJson());
    });
    return promise.future();
  }

  public static Future<JsonObject> deleteInventoryRecord (OkapiClient okapiClient, InventoryRecord inventoryRecord) {
    Promise<JsonObject> promise = Promise.promise();
    okapiClient.delete(getApi(inventoryRecord.entityType())+"/"+inventoryRecord.getUUID(), deleteResult -> {
      if (deleteResult.succeeded()) {
        inventoryRecord.complete();
        promise.complete();
      } else {
        inventoryRecord.fail();
        inventoryRecord.logError(deleteResult.cause().getMessage(), okapiClient.getStatusCode(), ErrorReport.ErrorCategory.STORAGE, inventoryRecord.getOriginJson());
        promise.fail(inventoryRecord.getErrorAsJson().encodePrettily());
      }
    });
    return promise.future();

  }

  public static Future<JsonObject> lookupInstance(OkapiClient okapiClient, InventoryQuery inventoryQuery) {
    if (inventoryQuery instanceof QueryByUUID) {
      // this reduces the lookup response time from 22 ms to 18 ms.
      return lookupInstance(okapiClient, (QueryByUUID) inventoryQuery);
    }
    return okapiClient.get(queryUri(INSTANCE_STORAGE_PATH, inventoryQuery))
        .map(json -> {
          JsonArray matchingInstances = new JsonObject(json).getJsonArray(INSTANCES);
          if (matchingInstances.isEmpty()) {
            return null;
          }
          return matchingInstances.getJsonObject(0);
        })
        .recover(e -> failureFuture(e, Entity.INSTANCE, Transaction.GET, okapiClient.getStatusCode(), null));
  }

  public static Future<JsonObject> lookupInstance(OkapiClient okapiClient, QueryByUUID queryByUuid) {
    return okapiClient.get(INSTANCE_STORAGE_PATH + "/" + queryByUuid.getUuid())
        .map(JsonObject::new)
        .recover(e -> {
          if (okapiClient.getStatusCode() == 404) {
            return Future.succeededFuture(null);
          }
          return failureFuture(e, Entity.INSTANCE, Transaction.GET, okapiClient.getStatusCode(), null);
        });
  }


  public static Future<JsonArray> lookupInstances (OkapiClient okapiClient, QueryByListOfIds inventoryQuery) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(INSTANCE_STORAGE_PATH+"?limit=100000&query="+inventoryQuery.getURLEncodedQueryString(), res -> {
      if ( res.succeeded()) {
        JsonObject matchingInstances = new JsonObject(res.result());
        int recordCount = matchingInstances.getInteger(TOTAL_RECORDS);
        if (recordCount > 0) {
          promise.complete(matchingInstances.getJsonArray(INSTANCES));
        } else {
          promise.complete(null);
        }
      } else {
        failure(res.cause(), Entity.INSTANCE, Transaction.GET, okapiClient.getStatusCode(), promise);
      }
    });
    return promise.future();
  }

  public static Future<JsonArray> lookupInstanceSets(OkapiClient okapiClient, QueryByListOfIds inventoryQuery) {
    if (inventoryQuery.ids.size() > 10) {
      throw new IllegalArgumentException(
          INSTANCE_SET_PATH + " can process at most 10 ids, but got " + inventoryQuery.ids.size());
    }
    String path = INSTANCE_SET_PATH + "?instance=true&holdingsRecords=true&items=true"
        + "&precedingTitles=true&succeedingTitles=true"
        + "&superInstanceRelationships=true&subInstanceRelationships=true"
        + "&limit=10&query=" + inventoryQuery.getURLEncodedQueryString();
    return okapiClient.get(path)
        .map(result -> emptyToNull(new JsonObject(result).getJsonArray("instanceSets")))
        .recover(e -> failureFuture(e, Entity.INSTANCE, Transaction.GET, okapiClient.getStatusCode(), null));
  }

  private static JsonArray emptyToNull(JsonArray jsonArray) {
    if (jsonArray.isEmpty()) {
      return null;
    }
    return jsonArray;
  }

  public static Future<JsonArray> lookupHoldingsRecords (OkapiClient okapiClient, QueryByListOfIds inventoryQuery) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(HOLDINGS_STORAGE_PATH+"?limit=100000&query="+inventoryQuery.getURLEncodedQueryString(), res -> {
      if ( res.succeeded()) {
        JsonObject holdingsRecords = new JsonObject(res.result());
        int recordCount = holdingsRecords.getInteger(TOTAL_RECORDS);
        if (recordCount > 0) {
          promise.complete(holdingsRecords.getJsonArray(HOLDINGS_RECORDS));
        } else {
          promise.complete(null);
        }
      } else {
        failure(res.cause(),
                Entity.HOLDINGS_RECORD,
                Transaction.GET,
                okapiClient.getStatusCode(),
                promise,
                HOLDINGS_STORAGE_PATH+"?limit=100000&query="+inventoryQuery.getURLEncodedQueryString());
      }
    });
    return promise.future();
  }

  public static Future<JsonArray> lookupItems (OkapiClient okapiClient, QueryByListOfIds inventoryQuery) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(ITEM_STORAGE_PATH+"?limit=100000&query="+inventoryQuery.getURLEncodedQueryString(), res -> {
      if ( res.succeeded()) {
        JsonObject items = new JsonObject(res.result());
        int recordCount = items.getInteger(TOTAL_RECORDS);
        if (recordCount > 0) {
          promise.complete(items.getJsonArray(ITEMS));
        } else {
          promise.complete(null);
        }
      } else {
        failure(res.cause(), Entity.ITEM, Transaction.GET, okapiClient.getStatusCode(), promise,
                ITEM_STORAGE_PATH+"?limit=100000&query="+inventoryQuery.getURLEncodedQueryString());
      }
    });
    return promise.future();
  }



  public static Future<JsonObject> lookupSingleInventoryRecordSet(OkapiClient okapiClient, InventoryQuery uniqueQuery) {
    return okapiClient.get(INSTANCE_SET_PATH
        + "?instance=true&holdingsRecords=true&items=true"
        + "&precedingTitles=true&succeedingTitles=true"
        + "&superInstanceRelationships=true&subInstanceRelationships=true"
        + "&limit=1&query=" + uniqueQuery.getURLEncodedQueryString())
        .map(result -> {
          var sets = new JsonObject(result).getJsonArray("instanceSets");
          if (sets.isEmpty()) {
            return null;
          }
          var input = sets.getJsonObject(0);
          var holdings = input.getJsonArray(HOLDINGS_RECORDS);
          mergeItemsIntoHoldings(holdings, input.getJsonArray(ITEMS));
          var precedingSucceeding = new JsonArray()
              .addAll(input.getJsonArray("precedingTitles"))
              .addAll(input.getJsonArray("succeedingTitles"));
          var superSub = new JsonArray()
              .addAll(input.getJsonArray("superInstanceRelationships"))
              .addAll(input.getJsonArray("subInstanceRelationships"));
          var instanceRelations = new JsonObject()
              .put(InstanceReferences.EXISTING_PRECEDING_SUCCEEDING_TITLES, precedingSucceeding)
              .put(InstanceReferences.EXISTING_PARENT_CHILD_RELATIONS, superSub);
          return new JsonObject()
              .put(InventoryRecordSet.INSTANCE, input.getJsonObject("instance"))
              .put(InventoryRecordSet.HOLDINGS_RECORDS, holdings)
              .put(InstanceReferences.INSTANCE_RELATIONS, instanceRelations);
        });
  }

  private static void mergeItemsIntoHoldings(JsonArray holdingsArray, JsonArray items) {
    if (holdingsArray.isEmpty() || items.isEmpty()) {
      return;
    }
    Map<String, JsonObject> holdings = new HashMap<>();
    holdingsArray.forEach(h -> {
      var holding = (JsonObject) h;
      holdings.put(holding.getString("id"), holding);
    });
    items.forEach(i -> {
      var item = (JsonObject) i;
      var holding = holdings.get(item.getString("holdingsRecordId"));
      var itemList = holding.getJsonArray(ITEMS);
      if (itemList == null) {
        itemList = new JsonArray();
        holding.put(ITEMS, itemList);
      }
      itemList.add(item);
    });
  }

  public static Future<JsonArray> getLocations(OkapiClient okapiClient)  {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(LOCATION_STORAGE_PATH + "?limit=9999", locs -> {
      if (locs.succeeded()) {
        JsonObject response = new JsonObject(locs.result());
        JsonArray locationsJson = response.getJsonArray(LOCATIONS);
        promise.complete(locationsJson);
      }  else {
        failure(locs.cause(), Entity.LOCATION, Transaction.GET, okapiClient.getStatusCode(), promise,
                LOCATION_STORAGE_PATH + "?limit=9999");
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
      case INSTANCE_RELATIONSHIP:
        api = INSTANCE_RELATIONSHIP_STORAGE_PATH;
        break;
      case INSTANCE_TITLE_SUCCESSION:
        api = PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH;
        break;
      default:
        break;
    }
    return api;
  }
  private static String getBatchApi(String entityArrayName) {
    String api = "";
    switch (entityArrayName) {
      case INSTANCES:
        api = INSTANCE_STORAGE_BATCH_PATH;
        break;
      case HOLDINGS_RECORDS:
        api = HOLDINGS_STORAGE_BATCH_PATH;
        break;
      case ITEMS:
        api = ITEM_STORAGE_BATCH_PATH;
        break;
      default:
        break;
    }
    return api;
  }


  public static String queryUri(String path, InventoryQuery inventoryQuery) {
    return path + "?query=" + inventoryQuery.getURLEncodedQueryString();
  }

  public static <T> void failure(Throwable cause, Entity entityType, Transaction transaction, int httpStatusCode, Promise<T> promise) {
    failure(cause, entityType, transaction, httpStatusCode, promise, null);
  }

  public static <T> void failure(Throwable cause, Entity entityType, Transaction transaction, int httpStatusCode, Promise<T> promise, String contextNote) {
    promise.handle(failureFuture(cause, entityType, transaction, httpStatusCode, contextNote));
  }

  public static <T> Future<T> failureFuture(Throwable cause, Entity entityType, Transaction transaction,
      int httpStatusCode, String contextNote) {
    return Future.failedFuture(
            new ErrorReport(ErrorReport.ErrorCategory.STORAGE,
                    httpStatusCode,
                    cause.getMessage())
                    .setEntityType(entityType)
                    .setTransaction(transaction != null ? transaction.toString() : "")
                    .addDetail("context", contextNote)
                    .asJsonPrettily());
  }

  public static OkapiClient getOkapiClient ( RoutingContext ctx) {
    OkapiClient client = new OkapiClient(WebClientFactory.getWebClient(ctx.vertx()), ctx);
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-type", "application/json");
    if (ctx.request().getHeader(X_OKAPI_TENANT) != null) headers.put(X_OKAPI_TENANT, ctx.request().getHeader(X_OKAPI_TENANT));
    if (ctx.request().getHeader(X_OKAPI_TOKEN) != null) headers.put(X_OKAPI_TOKEN, ctx.request().getHeader(X_OKAPI_TOKEN));
    if (ctx.request().getHeader(X_OKAPI_REQUEST_ID) != null) headers.put(X_OKAPI_REQUEST_ID, ctx.request().getHeader(X_OKAPI_REQUEST_ID));
    headers.put("Accept", "application/json, text/plain");
    client.setHeaders(headers);
    return client;
  }

}
