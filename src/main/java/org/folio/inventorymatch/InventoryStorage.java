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
            logger.info("POST of Instance succeeded");
            String instanceResult = postResult.result();
            JsonObject instanceResponseJson = new JsonObject(instanceResult);
            promise.complete(instanceResponseJson);
          } else {
            promise.complete(null);
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
            JsonObject fail = new JsonObject("{ \"message\": \"failed\" }");
            promise.complete(fail);
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
            logger.info("POST of holdings record did not succeed");
            promise.complete(null);
          }
        });
        return promise.future();
      }

      public static Future<JsonObject> putHoldingsRecord(OkapiClient okapiClient, JsonObject holdingsRecord, String uuid) {
        Promise<JsonObject> promise = Promise.promise();
        okapiClient.request(HttpMethod.PUT, HOLDINGS_STORAGE_PATH + "/" + uuid, holdingsRecord.toString(), putResult->{
          if (putResult.succeeded()) {
            logger.info("PUT of holdings record succeeded");
            String putResultString = putResult.result();
            logger.info("Holdings PUT result string: " + putResultString);
            JsonObject putResponseJson;
            if (putResultString == null || putResultString.length()==0) {
              putResponseJson = new JsonObject("{ \"message\": \"no content\"}");
            } else {
              putResponseJson = new JsonObject(putResultString);
            }
            promise.complete(putResponseJson);
          } else {
            logger.info("PUT of holdings record did not succeed");
            promise.complete(null);
          }
        });
        return promise.future();
      }

      public static Future<JsonObject> postItem(OkapiClient okapiClient, JsonObject item) {
        Promise<JsonObject> promise = Promise.promise();
        okapiClient.post(ITEM_STORAGE_PATH, item.toString(), postResult->{
          if (postResult.succeeded()) {
            logger.info("POST of item succeeded");
            String itemResult = postResult.result();
            JsonObject itemResponseJson = new JsonObject(itemResult);
            promise.complete(itemResponseJson);
          } else {
            logger.info("POST of item did not succeed");
            promise.complete(null);
          }
        });
        return promise.future();
      }

      public static Future<JsonObject> putItem(OkapiClient okapiClient, JsonObject item, String uuid) {
        Promise<JsonObject> promise = Promise.promise();
        okapiClient.request(HttpMethod.PUT, ITEM_STORAGE_PATH + "/" + uuid, item.toString(), putResult->{
          if (putResult.succeeded()) {
            logger.info("PUT of item succeeded");
            String putResultString = putResult.result();
            JsonObject putResponseJson;
            if (putResultString == null || putResultString.length()==0) {
              putResponseJson = new JsonObject("{ \"message\": \"no content\"}");
            } else {
              putResponseJson = new JsonObject(putResultString);
            }
            promise.complete(putResponseJson);
          } else {
            logger.info("PUT of item did not succeed");
            promise.complete(null);
          }
        });
        return promise.future();
    }

}