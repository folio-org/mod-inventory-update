package org.folio.inventorymatch;

import org.folio.okapi.common.OkapiClient;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
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

}