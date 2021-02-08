package org.folio.inventoryupdate.test.fakestorage;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import io.vertx.core.json.JsonObject;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

public class FakeInventoryStorage {
  public final static int PORT_INVENTORY_STORAGE = 9030;
  public final static String INSTANCE_STORAGE_PATH = "/instance-storage/instances";
  public static final String INSTANCE_RELATIONSHIP_STORAGE_PATH = "/instance-storage/instance-relationships";
  public static final String PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH = "/preceding-succeeding-titles";
  public static final String HOLDINGS_STORAGE_PATH = "/holdings-storage/holdings";
  public static final String ITEM_STORAGE_PATH = "/item-storage/items";
  public static final String LOCATION_STORAGE_PATH = "/locations";

  public static final String RESULT_SET_INSTANCES = "instances";

  public InstanceStorage instanceStorage = new InstanceStorage();
  public HoldingsStorage holdingsStorage = new HoldingsStorage();
  public InstanceRelationshipStorage instanceRelationshipStorage = new InstanceRelationshipStorage();
  public PrecedingSucceedingStorage precedingSucceedingStorage = new PrecedingSucceedingStorage();

  public FakeInventoryStorage (Vertx vertx, TestContext testContext, Async async) {
    instanceStorage.setFakeStorage(this);
    holdingsStorage.setFakeStorage(this);
    precedingSucceedingStorage.setFakeStorage(this);

    Router router = Router.router(vertx);
    router.get(INSTANCE_STORAGE_PATH).handler(instanceStorage::getRecordsByQuery);
    router.get(INSTANCE_STORAGE_PATH +"/:id").handler(instanceStorage::getRecordById);
    router.get(HOLDINGS_STORAGE_PATH).handler(holdingsStorage::getRecordsByQuery);
    router.get(HOLDINGS_STORAGE_PATH +"/:id").handler(holdingsStorage::getRecordById);
    router.get(INSTANCE_RELATIONSHIP_STORAGE_PATH).handler(instanceRelationshipStorage::getRecordsByQuery);
    router.get(INSTANCE_RELATIONSHIP_STORAGE_PATH + "/:id").handler(instanceRelationshipStorage::getRecordById);
    router.get(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH).handler(precedingSucceedingStorage::getRecordsByQuery);
    router.get(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH + "/:id").handler(precedingSucceedingStorage::getRecordById);
    router.post("/*").handler(BodyHandler.create());
    router.post(INSTANCE_STORAGE_PATH).handler(instanceStorage::createRecord);
    router.post(HOLDINGS_STORAGE_PATH).handler(holdingsStorage::createRecord);
    router.post(INSTANCE_RELATIONSHIP_STORAGE_PATH).handler(instanceRelationshipStorage::createRecord);
    router.post(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH).handler(precedingSucceedingStorage::createRecord);
    router.put("/*").handler(BodyHandler.create());
    router.put(INSTANCE_STORAGE_PATH +"/:id").handler(instanceStorage::updateRecord);
    router.put(HOLDINGS_STORAGE_PATH + "/:id").handler(holdingsStorage::updateRecord);
    HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
    vertx.createHttpServer(so)
      .requestHandler(router::accept)
      .listen(
        PORT_INVENTORY_STORAGE,
        result -> {
          if (result.failed()) {
            testContext.fail(result.cause());
          }
          async.complete();
        }
      );
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
  }

  public static JsonObject getRecordsByQuery(String storagePath, String query) {
    Response response = RestAssured.given()
            .get(storagePath + "?" + query)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();
    return new JsonObject(response.getBody().asString());
  }

  public static JsonObject getRecordById(String storagePath, String id) {
    Response response = RestAssured.given()
            .get(storagePath + "/" + id)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();
    return new JsonObject(response.getBody().asString());
  }

  public static JsonObject post (String storagePath, JsonObject recordToPOST) {
    Response response = RestAssured.given()
            .body(recordToPOST.toString())
            .post(storagePath)
            .then()
            .log().ifValidationFails()
            .statusCode(201).extract().response();
    return new JsonObject(response.getBody().asString());
  }

  public static void put(String storagePath, JsonObject recordToPUT) {
    RestAssured.given()
            .body(recordToPUT.toString())
            .put(storagePath +"/"+ recordToPUT.getString("id"))
            .then()
            .log().ifValidationFails()
            .statusCode(204).extract().response();
  }

}
