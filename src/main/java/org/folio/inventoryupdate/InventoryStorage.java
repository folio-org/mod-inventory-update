package org.folio.inventoryupdate;

import static org.folio.inventoryupdate.entities.InstanceRelationsManager.EXISTING_PARENT_CHILD_RELATIONS;
import static org.folio.inventoryupdate.entities.InstanceRelationsManager.EXISTING_PRECEDING_SUCCEEDING_TITLES;
import static org.folio.inventoryupdate.entities.InstanceRelationsManager.INSTANCE_RELATIONS;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import org.folio.inventoryupdate.entities.InventoryRecord;
import org.folio.inventoryupdate.entities.InventoryRecord.Entity;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.inventoryupdate.entities.InventoryRecordSet;
import org.folio.okapi.common.ExtendedAsyncResult;
import org.folio.okapi.common.OkapiClient;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.okapi.common.WebClientFactory;

/**
 * Static methods making low level HTTP requests to create and update records in Inventory Storage.
 */
public class InventoryStorage {

  private static final Logger logger = LoggerFactory.getLogger("inventory-update");
  private static final String INSTANCE_STORAGE_PATH = "/instance-storage/instances";
  private static final String INSTANCE_RELATIONSHIP_STORAGE_PATH = "/instance-storage/instance-relationships";
  private static final String PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH = "/preceding-succeeding-titles";
  private static final String HOLDINGS_STORAGE_PATH = "/holdings-storage/holdings";
  private static final String ITEM_STORAGE_PATH = "/item-storage/items";
  private static final String LOCATION_STORAGE_PATH = "/locations";

  // Property keys, JSON responses
  public static final String ID = "id";
  public static final String INSTANCES = "instances";
  public static final String TOTAL_RECORDS = "totalRecords";
  public static final String HOLDINGS_RECORDS = "holdingsRecords";
  public static final String ITEMS = "items";
  public static final String INSTANCE_RELATIONSHIPS = "instanceRelationships";
  public static final String PRECEDING_SUCCEEDING_TITLES = "precedingSucceedingTitles";
  public static final String LOCATIONS = "locations";
  public static final String LF = System.lineSeparator();

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
        int recordCount = matchingInstances.getInteger(TOTAL_RECORDS);
        if (recordCount > 0) {
          promise.complete(matchingInstances.getJsonArray(INSTANCES).getJsonObject(0));
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
        int recordCount = records.getInteger(TOTAL_RECORDS);
        if (recordCount == 1) {
          promise.complete(records.getJsonArray(HOLDINGS_RECORDS).getJsonObject(0));
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
        int recordCount = records.getInteger(TOTAL_RECORDS);
        if (recordCount == 1) {
          promise.complete(records.getJsonArray(ITEMS).getJsonObject(0));
        } else {
          promise.complete(null);
        }
      } else {
        failure(res.cause(), Entity.ITEM, Transaction.GET, okapiClient.getStatusCode(), promise);
      }
    });
    return promise.future();
  }

  public static Future<JsonObject> lookupSingleInventoryRecordSet(OkapiClient okapiClient, InventoryQuery uniqueQuery) {
    return lookupInstanceHoldingsItems(okapiClient, uniqueQuery)
        .compose(inventoryRecordSet -> {
          if (inventoryRecordSet == null) {
            return Future.succeededFuture();
          }
          var instanceId = inventoryRecordSet.getJsonObject(InventoryRecordSet.INSTANCE).getString("id");
          var instanceRelations = new JsonObject();
          inventoryRecordSet.put(INSTANCE_RELATIONS, instanceRelations);
          var future1 = lookupExistingParentChildRelationshipsByInstanceUUID(okapiClient, instanceId)
              .onSuccess(result -> instanceRelations.put(EXISTING_PARENT_CHILD_RELATIONS, result));
          var future2 = lookupExistingPrecedingOrSucceedingTitlesByInstanceUUID(okapiClient, instanceId)
              .onSuccess(result -> instanceRelations.put(EXISTING_PRECEDING_SUCCEEDING_TITLES, result));
          return CompositeFuture.all(future1, future2)
              .map(inventoryRecordSet);
        })
        .recover(e -> failureFuture(e, Entity.INSTANCE, Transaction.GET, okapiClient.getStatusCode(),
            "lookupSingleInventoryRecordSet"));
  }

  private static Future<JsonObject> lookupInstanceHoldingsItems(
      OkapiClient okapiClient, InventoryQuery uniqueQuery) {

    // prepend first word: id -> instance.id, hrid -> instance.hrid
    String query = uniqueQuery.queryString.replaceFirst("\\w", "instance.$0");
    query = URLEncoder.encode(query, StandardCharsets.UTF_8);
    return okapiClient.get("/inventory-view/instances?query=" + query)
        .map(body -> {
          /* {
           *   "instances": [
           *     {
           *       "instanceId": "7fbd5d84-62d1-44c6-9c45-6cb173998bbd",
           *       "isBoundWith": false,
           *       "instance": {
           *         "id": "7fbd5d84-62d1-44c6-9c45-6cb173998bbd",
           *         "hrid": "inst000000000006",
           *         ...
           *       },
           *       "holdingsRecords": [
           *         {
           *           "id": "fb7b70f1-b898-4924-a991-0e4b6312bb5f",
           *           "hrid": "hold000000000005",
           *           ...
           *         },{
           *           ...
           *         }
           *       ],
           *       "items": [
           *         {
           *           "id": "d6f7c1ba-a237-465e-94ed-f37e91bc64bd",
           *           "hrid": "item000000000010",
           *           ...
           *         },{
           *           ...
           *         }
           *       ]
           *     }
           *   ]
           * }
           */
          JsonObject view = new JsonObject(body);
          if (view.getJsonArray("instances").size() == 0) {
            return null;
          }
          JsonObject input = view.getJsonArray("instances").getJsonObject(0);
          JsonObject instance = input.getJsonObject(InventoryRecordSet.INSTANCE);
          JsonObject output = new JsonObject().put(InventoryRecordSet.INSTANCE, instance);
          JsonArray holdingsRecords = input.getJsonArray(InventoryRecordSet.HOLDINGS_RECORDS);
          if (holdingsRecords == null) {
            output.put(InventoryRecordSet.HOLDINGS_RECORDS, new JsonArray());
            return output;
          }
          output.put(InventoryRecordSet.HOLDINGS_RECORDS, holdingsRecords);
          /** map holding id to the items array of the holding */
          Map<String,JsonArray> map = new HashMap<>();
          for (int i = 0; i < holdingsRecords.size(); i++) {
            JsonObject holding = holdingsRecords.getJsonObject(i);
            JsonArray items = new JsonArray();
            holding.put(InventoryRecordSet.ITEMS, items);
            map.put(holding.getString("id"), items);
          }
          JsonArray items = input.getJsonArray(InventoryRecordSet.ITEMS);
          for (int i = 0; i < items.size(); i++) {
            JsonObject item = items.getJsonObject(i);
            JsonArray itemArray = map.get(item.getString("holdingsRecordId"));
            if (itemArray == null) {
              continue;
            }
            itemArray.add(item);
          }
          return output;
        });
  }

  public static Future<JsonArray> lookupExistingParentChildRelationshipsByInstanceUUID(OkapiClient okapiClient, String instanceId) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(INSTANCE_RELATIONSHIP_STORAGE_PATH +"?limit=1000&query=(subInstanceId%3D%3D"+instanceId+"%20or%20superInstanceId%3D%3D"+instanceId+")", res -> {
      if (res.succeeded()) {
        JsonObject relationshipsResult = new JsonObject(res.result());
        JsonArray parentChildRelations = relationshipsResult.getJsonArray(INSTANCE_RELATIONSHIPS);
        logger.debug("Successfully looked up existing instance relationships, found  " + parentChildRelations.size());
        promise.complete(parentChildRelations);
      } else {
        failure(res.cause(), Entity.ITEM, Transaction.GET, okapiClient.getStatusCode(), promise, "While looking instance relationships");
      }
    });
    return promise.future();
  }

  public static Future<JsonArray> lookupExistingPrecedingOrSucceedingTitlesByInstanceUUID (OkapiClient okapiClient, String instanceId) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH +"?limit=1000&query=(precedingInstanceId%3D%3D"+instanceId+"%20or%20succeedingInstanceId%3D%3D"+instanceId+")", res -> {
      if (res.succeeded()) {
        JsonObject relationsResult = new JsonObject(res.result());
        JsonArray instanceTitleSuccessions = relationsResult.getJsonArray(PRECEDING_SUCCEEDING_TITLES);
        logger.debug("Successfully looked up existing preceding/succeeding titles, found  " + instanceTitleSuccessions.size());
        promise.complete(instanceTitleSuccessions);
      } else {
        failure(res.cause(), Entity.ITEM, Transaction.GET, okapiClient.getStatusCode(), promise, "While looking up preceding/succeeding titles for the Instance");
      }
    });
    return promise.future();

  }

  public static Future<JsonArray> getLocations(OkapiClient okapiClient)  {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(LOCATION_STORAGE_PATH + "?limit=9999", locs -> {
      if (locs.succeeded()) {
        JsonObject response = new JsonObject(locs.result());
        JsonArray locationsJson = response.getJsonArray(LOCATIONS);
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

  private static <T> void failure(Throwable cause, Entity entityType, Transaction transaction, int httpStatusCode, Promise<T> promise) {
    failure(cause, entityType, transaction, httpStatusCode, promise, null);
  }

  private static <T> void failure(Throwable cause, Entity entityType, Transaction transaction, int httpStatusCode, Promise<T> promise, String contextNote) {
    promise.handle(failureFuture(cause, entityType, transaction, httpStatusCode, contextNote));
  }

  private static <T> Future<T> failureFuture(Throwable cause, Entity entityType, Transaction transaction,
      int httpStatusCode, String contextNote) {

    JsonObject errorMessage = new JsonObject();
    errorMessage.put("message", cause.getMessage());
    errorMessage.put("entity-type",entityType);
    errorMessage.put("operation", transaction);
    errorMessage.put("http-status-code", httpStatusCode);
    if (contextNote != null) {
      errorMessage.put("note-of-context", contextNote);
    }
    return Future.failedFuture(errorMessage.encodePrettily());
  }

  /** Logs execution times of OkapiClient HTTP requests. */
  static class TimingOkapiClient extends OkapiClient {
    TimingOkapiClient(WebClient webClient, RoutingContext ctx) {
      super(webClient, ctx);
    }

    private void log(long start, long end, HttpMethod method, String uri) {
      logger.debug(String.format("%4d ms (...%03d - ...%03d) %s %s",
          (end-start), (start%1000), (end%1000), method, uri));
    }

    @Override
    public Future<String> get(String uri) {
      long start = System.currentTimeMillis();
      return super.get(uri)
          .onComplete(x -> log(start, System.currentTimeMillis(), HttpMethod.GET, uri));
    }

    @Override
    public void request(HttpMethod method, String uri, String body, Handler<ExtendedAsyncResult<String>> fut) {
      long start = System.currentTimeMillis();
      super.request(method, uri, body, event -> {
        log(start, System.currentTimeMillis(), method, uri);
        fut.handle(event);
      });
    }

    @Override
    public Future<String> request(HttpMethod method, String uri, String body) {
      long start = System.currentTimeMillis();
      return super.request(method, uri, body)
          .onComplete(x -> log(start, System.currentTimeMillis(), method, uri));
    }
  }

  public static OkapiClient getOkapiClient ( RoutingContext ctx) {
    OkapiClient client = logger.isDebugEnabled()
        ? new TimingOkapiClient(WebClientFactory.getWebClient(ctx.vertx()), ctx)
        : new OkapiClient(WebClientFactory.getWebClient(ctx.vertx()), ctx);
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-type", "application/json");
    if (ctx.request().getHeader("X-Okapi-Tenant") != null) headers.put("X-Okapi-Tenant", ctx.request().getHeader("X-Okapi-Tenant"));
    if (ctx.request().getHeader("X-Okapi-Token") != null) headers.put("X-Okapi-Token", ctx.request().getHeader("X-Okapi-Token"));
    headers.put("Accept", "application/json, text/plain");
    client.setHeaders(headers);
    return client;
  }

}