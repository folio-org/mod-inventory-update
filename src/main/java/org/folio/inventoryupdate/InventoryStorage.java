package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.HoldingsRecord;
import org.folio.inventoryupdate.entities.Instance;
import org.folio.inventoryupdate.entities.InstanceRelations;
import org.folio.inventoryupdate.entities.InventoryRecord;
import org.folio.inventoryupdate.entities.InventoryRecord.Entity;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.inventoryupdate.entities.InventoryRecordSet;
import org.folio.inventoryupdate.entities.Item;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
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

  private static final Logger logger = LoggerFactory.getLogger("inventory-update");
  private static final String INSTANCE_STORAGE_PATH = "/instance-storage/instances";
  public static final String INSTANCE_STORAGE_BATCH_PATH = "/instance-storage/batch/synchronous";
  private static final String INSTANCE_RELATIONSHIP_STORAGE_PATH = "/instance-storage/instance-relationships";
  private static final String PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH = "/preceding-succeeding-titles";
  private static final String HOLDINGS_STORAGE_PATH = "/holdings-storage/holdings";
  public static final String HOLDINGS_STORAGE_BATCH_PATH = "/holdings-storage/batch/synchronous";
  private static final String ITEM_STORAGE_PATH = "/item-storage/items";
  public static final String ITEM_STORAGE_BATCH_PATH = "/item-storage/batch/synchronous";
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
        record.logError(okapiClient.getResponsebody(), okapiClient.getStatusCode(), ErrorReport.ErrorCategory.STORAGE);
        promise.fail(record.getErrorAsJson().encodePrettily());
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
      logger.debug("Posting request: " + request.encodePrettily() + " to " + getBatchApi(arrayName));
      okapiClient.post(getBatchApi(arrayName) + "?upsert=true", request.encode(), postResult -> {
        if (postResult.succeeded()) {
          for (InventoryRecord record : records) {
            record.complete();
          }
          promise.complete();
        } else {
          for (InventoryRecord record : records) {
            record.fail();
            record.skipDependants();
            record.logError(
                    okapiClient.getResponsebody(),
                    okapiClient.getStatusCode(),
                    (records.size()>1 ? ErrorReport.ErrorCategory.BATCH_STORAGE : ErrorReport.ErrorCategory.STORAGE)
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
    for (InventoryRecord record : records) {
      array.add(record.asJson());
    }
    return array;
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
        record.logError(okapiClient.getResponsebody(), okapiClient.getStatusCode(), ErrorReport.ErrorCategory.STORAGE);
        promise.fail(record.getErrorAsJson().encodePrettily());
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
        record.logError(deleteResult.cause().getMessage(), okapiClient.getStatusCode(), ErrorReport.ErrorCategory.STORAGE);
        promise.fail(record.getErrorAsJson().encodePrettily());
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
          if (matchingInstances.size() == 0) {
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
  public static Future<JsonArray> lookupParentChildRelationships(OkapiClient okapiClient, QueryByListOfIds inventoryQuery) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(INSTANCE_RELATIONSHIP_STORAGE_PATH +"?limit=10000&query=" + inventoryQuery.getURLEncodedQueryString(), res -> {
      if (res.succeeded()) {
        JsonObject relationshipsResult = new JsonObject(res.result());
        JsonArray parentChildRelations = relationshipsResult.getJsonArray(INSTANCE_RELATIONSHIPS);
        logger.debug("Successfully looked up existing instance relationships, found  " + parentChildRelations.size());
        promise.complete(parentChildRelations);
      } else {
        logger.info("Could not look up existing instance relationships");
        failure(res.cause(), Entity.INSTANCE_RELATIONSHIP, Transaction.GET, okapiClient.getStatusCode(), promise,
                INSTANCE_RELATIONSHIP_STORAGE_PATH +"?limit=10000&query=" + inventoryQuery.getURLEncodedQueryString());
      }
    });
    return promise.future();
  }

  public static Future<JsonArray> lookupTitleSuccessions (OkapiClient okapiClient, QueryByListOfIds inventoryQuery) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH +"?limit=10000&query=" + inventoryQuery.getURLEncodedQueryString(), res -> {
      if (res.succeeded()) {
        JsonObject relationshipsResult = new JsonObject(res.result());
        JsonArray titleSuccessions = relationshipsResult.getJsonArray(PRECEDING_SUCCEEDING_TITLES);
        logger.debug("Successfully looked up existing title succession relationships, found  " + titleSuccessions.size());
        promise.complete(titleSuccessions);
      } else {
        failure(res.cause(), Entity.INSTANCE_TITLE_SUCCESSION, Transaction.GET, okapiClient.getStatusCode(), promise,
                PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH +"?limit=10000&query=" + inventoryQuery.getURLEncodedQueryString());
      }
    });
    return promise.future();

  }

  public static Future<JsonObject> lookupSingleInventoryRecordSet (OkapiClient okapiClient, InventoryQuery uniqueQuery) {
    Promise<JsonObject> promise = Promise.promise();
    JsonObject inventoryRecordSet = new JsonObject();

    lookupInstance(okapiClient, uniqueQuery).onComplete( instanceResult -> {
      if (instanceResult.succeeded()) {
        JsonObject instance = instanceResult.result();
        if (instance==null) {
          promise.complete(null);
        } else {
          inventoryRecordSet.put(InventoryRecordSet.INSTANCE,instance);
          String instanceUUID = instance.getString(ID);
          lookupExistingHoldingsRecordsAndItemsByInstanceUUID(okapiClient, instanceUUID).onComplete(existingHoldingsResult -> {
              StringBuilder errorMessages = new StringBuilder();
              if (existingHoldingsResult.succeeded()) {
                  if (existingHoldingsResult.result() != null) {
                      inventoryRecordSet.put(InventoryRecordSet.HOLDINGS_RECORDS,existingHoldingsResult.result());
                  } else {
                      inventoryRecordSet.put(InventoryRecordSet.HOLDINGS_RECORDS, new JsonArray());
                  }
              } else {
                errorMessages.append(LF).append(existingHoldingsResult.cause());
                logger.error("Lookup of existing holdings/items failed " + existingHoldingsResult);
              }
              lookupExistingParentChildRelationshipsByInstanceUUID(okapiClient, instanceUUID).onComplete(existingParentChildRelations -> {
                lookupExistingPrecedingOrSucceedingTitlesByInstanceUUID(okapiClient, instanceUUID).onComplete( existingInstanceTitleSuccessions -> {
                  JsonObject instanceRelations = new JsonObject();
                  inventoryRecordSet.put( InstanceRelations.INSTANCE_RELATIONS, instanceRelations);
                  if(existingInstanceTitleSuccessions.succeeded()) {
                    if (existingInstanceTitleSuccessions.result() != null)  {
                      instanceRelations.put( InstanceRelations.EXISTING_PRECEDING_SUCCEEDING_TITLES, existingInstanceTitleSuccessions.result());
                      logger.debug("InventoryRecordSet JSON populated with " +
                              inventoryRecordSet.getJsonObject( InstanceRelations.INSTANCE_RELATIONS)
                                      .getJsonArray( InstanceRelations.EXISTING_PRECEDING_SUCCEEDING_TITLES).size() + " preceding/succeeding titles");
                    }
                  } else {
                    errorMessages.append(LF).append(existingInstanceTitleSuccessions.cause());
                    logger.error("Lookup of existing preceding/succeeding titles failed " + existingInstanceTitleSuccessions.cause());
                  }
                  if (existingParentChildRelations.succeeded()) {
                    if (existingParentChildRelations.result() != null) {
                      instanceRelations.put( InstanceRelations.EXISTING_PARENT_CHILD_RELATIONS, existingParentChildRelations.result());
                      logger.debug("InventoryRecordSet JSON populated with " +
                              inventoryRecordSet.getJsonObject( InstanceRelations.INSTANCE_RELATIONS)
                                      .getJsonArray( InstanceRelations.EXISTING_PARENT_CHILD_RELATIONS).size() + " parent/child relations");
                    }
                  } else {
                    errorMessages.append(LF).append(existingParentChildRelations.cause());
                    logger.error("Lookup of existing Instance relationships failed " + existingParentChildRelations.cause());
                  }
                  if (existingHoldingsResult.succeeded() && existingInstanceTitleSuccessions.succeeded() && existingParentChildRelations.succeeded()) {
                    promise.complete(inventoryRecordSet);
                  } else {
                    promise.fail(new ErrorReport(
                            ErrorReport.ErrorCategory.STORAGE,
                            ErrorReport.UNPROCESSABLE_ENTITY,
                            errorMessages.toString())
                            .setShortMessage("Error looking up existing record set")
                            .asJsonString());
                  }
                });
              });
          });
        }
      } else {
        failure(instanceResult.cause(), Entity.INSTANCE, Transaction.GET, okapiClient.getStatusCode(), promise,
                "Looking up single inventory record set");
      }
    });
    return promise.future();
  }

  public static Future<JsonArray> lookupExistingHoldingsRecordsAndItemsByInstanceUUID (OkapiClient okapiClient, String instanceId) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(HOLDINGS_STORAGE_PATH+"?limit=1000&query=instanceId%3D%3D"+instanceId, res -> {
      if (res.succeeded()) {
        JsonObject holdingsRecordsResult = new JsonObject(res.result());
        JsonArray holdingsRecords = holdingsRecordsResult.getJsonArray(HOLDINGS_RECORDS);
        logger.debug("Successfully looked up existing holdings records, found  " + holdingsRecords.size());
        if (holdingsRecords.size()>0) {
          @SuppressWarnings("rawtypes")
          List<Future> itemFutures = new ArrayList<>();
          for (Object holdingsObject : holdingsRecords) {
            JsonObject holdingsRecord = (JsonObject) holdingsObject;
            itemFutures.add(lookupAndEmbedExistingItems(okapiClient, holdingsRecord));
          }
          CompositeFuture.all(itemFutures).onComplete( result -> {
            if (result.succeeded()) {
              logger.debug("Composite succeeded with " + result.result().size() + " result(s). First item: " + ((JsonObject) result.result().resultAt(0)).encodePrettily());
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
        promise.fail(new ErrorReport(ErrorReport.ErrorCategory.STORAGE,
                okapiClient.getStatusCode(),
                res.cause().getMessage())
                .setShortMessage("Error looking up holdings by Instance ID")
                .setEntityType(Entity.HOLDINGS_RECORD)
                .setTransaction(Transaction.GET.name())
                .asJsonPrettily());
      }
    });
    return promise.future();
  }

  private static Future<JsonObject> lookupAndEmbedExistingItems (OkapiClient okapiClient, JsonObject holdingsRecord) {
    Promise<JsonObject> promise = Promise.promise();
    okapiClient.get(ITEM_STORAGE_PATH+"?limit=1000&query=holdingsRecordId%3D%3D"+holdingsRecord.getString(ID), res -> {
      if (res.succeeded()) {
        JsonObject itemsResult = new JsonObject(res.result());
        JsonArray items = itemsResult.getJsonArray(ITEMS);
        logger.debug("Successfully looked up existing items, found  " + items.size());
        holdingsRecord.put(ITEMS,items);
        promise.complete(holdingsRecord);
      } else {
        failure(res.cause(), Entity.ITEM, Transaction.GET, okapiClient.getStatusCode(), promise, "While looking up items by holdingsRecordId");
      }
    });
    return promise.future();
  }


  public static Future<JsonArray> lookupExistingParentChildRelationshipsByInstanceUUID(OkapiClient okapiClient, String instanceId) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(INSTANCE_RELATIONSHIP_STORAGE_PATH +"?limit=10000&query=(subInstanceId%3D%3D"+instanceId+"%20or%20superInstanceId%3D%3D"+instanceId+")", res -> {
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


  private static String queryUri(String path, InventoryQuery inventoryQuery) {
    return path + "?query=" + inventoryQuery.getURLEncodedQueryString();
  }

  private static <T> void failure(Throwable cause, Entity entityType, Transaction transaction, int httpStatusCode, Promise<T> promise) {
    failure(cause, entityType, transaction, httpStatusCode, promise, null);
  }

  private static <T> void failure(Throwable cause, Entity entityType, Transaction transaction, int httpStatusCode, Promise<T> promise, String contextNote) {
    promise.handle(failureFuture(cause, entityType, transaction, httpStatusCode, contextNote));
  }

  private static <T> Future<T> failureFuture(Throwable cause, Entity entityType, Transaction transaction,
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
    if (ctx.request().getHeader("X-Okapi-Tenant") != null) headers.put("X-Okapi-Tenant", ctx.request().getHeader("X-Okapi-Tenant"));
    if (ctx.request().getHeader("X-Okapi-Token") != null) headers.put("X-Okapi-Token", ctx.request().getHeader("X-Okapi-Token"));
    headers.put("Accept", "application/json, text/plain");
    client.setHeaders(headers);
    return client;
  }

}