package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.core.json.JsonObject;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
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
    public static final String RESULT_SET_HOLDINGS_RECORDS = "holdingsRecords";
    public static final String RESULT_SET_ITEMS = "items";

    public LocationStorage locationStorage = new LocationStorage();
    public InstanceStorage instanceStorage = new InstanceStorage();
    public HoldingsStorage holdingsStorage = new HoldingsStorage();
    public ItemStorage itemStorage = new ItemStorage();
    public InstanceRelationshipStorage instanceRelationshipStorage = new InstanceRelationshipStorage();
    public PrecedingSucceedingStorage precedingSucceedingStorage = new PrecedingSucceedingStorage();

    public FakeInventoryStorage(Vertx vertx, TestContext testContext) {
        locationStorage.attachToFakeStorage(this);
        instanceStorage.attachToFakeStorage(this);
        holdingsStorage.attachToFakeStorage(this);
        itemStorage.attachToFakeStorage(this);
        instanceRelationshipStorage.attachToFakeStorage(this);
        precedingSucceedingStorage.attachToFakeStorage(this);

        Router router = Router.router(vertx);
        router.get(LOCATION_STORAGE_PATH).handler(locationStorage::getRecords);
        router.get(LOCATION_STORAGE_PATH + "/:id").handler(locationStorage::getRecordById);
        router.get(INSTANCE_STORAGE_PATH).handler(instanceStorage::getRecords);
        router.get(INSTANCE_STORAGE_PATH + "/:id").handler(instanceStorage::getRecordById);
        router.get(HOLDINGS_STORAGE_PATH).handler(holdingsStorage::getRecords);
        router.get(HOLDINGS_STORAGE_PATH + "/:id").handler(holdingsStorage::getRecordById);
        router.get(ITEM_STORAGE_PATH).handler(itemStorage::getRecords);
        router.get(ITEM_STORAGE_PATH + "/:id").handler(itemStorage::getRecordById);
        router.get(INSTANCE_RELATIONSHIP_STORAGE_PATH).handler(instanceRelationshipStorage::getRecords);
        router.get(INSTANCE_RELATIONSHIP_STORAGE_PATH + "/:id").handler(instanceRelationshipStorage::getRecordById);
        router.get(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH).handler(precedingSucceedingStorage::getRecords);
        router.get(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH + "/:id").handler(precedingSucceedingStorage::getRecordById);
        router.post("/*").handler(BodyHandler.create());
        router.post(LOCATION_STORAGE_PATH).handler(locationStorage::createRecord);
        router.post(INSTANCE_STORAGE_PATH).handler(instanceStorage::createRecord);
        router.post(HOLDINGS_STORAGE_PATH).handler(holdingsStorage::createRecord);
        router.post(ITEM_STORAGE_PATH).handler(itemStorage::createRecord);
        router.post(INSTANCE_RELATIONSHIP_STORAGE_PATH).handler(instanceRelationshipStorage::createRecord);
        router.post(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH).handler(precedingSucceedingStorage::createRecord);
        router.put("/*").handler(BodyHandler.create());
        router.put(LOCATION_STORAGE_PATH + "/:id").handler(locationStorage::updateRecord);
        router.put(INSTANCE_STORAGE_PATH + "/:id").handler(instanceStorage::updateRecord);
        router.put(HOLDINGS_STORAGE_PATH + "/:id").handler(holdingsStorage::updateRecord);
        router.put(ITEM_STORAGE_PATH + "/:id").handler(itemStorage::updateRecord);
        router.delete(LOCATION_STORAGE_PATH + "/:id").handler(locationStorage::deleteRecord);
        router.delete(INSTANCE_STORAGE_PATH + "/:id").handler(instanceStorage::deleteRecord);
        router.delete(HOLDINGS_STORAGE_PATH + "/:id").handler(holdingsStorage::deleteRecord);
        router.delete(ITEM_STORAGE_PATH + "/:id").handler(itemStorage::deleteRecord);
        router.delete(INSTANCE_RELATIONSHIP_STORAGE_PATH + "/:id").handler(instanceRelationshipStorage::deleteRecord);
        router.delete(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH + "/:id").handler(precedingSucceedingStorage::deleteRecord);
        router.delete(LOCATION_STORAGE_PATH).handler(locationStorage::deleteAll);
        HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
        vertx.createHttpServer(so)
                .requestHandler(router)
                .listen(PORT_INVENTORY_STORAGE)
                .onComplete(testContext.asyncAssertSuccess());
        RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    }

    public static JsonObject getRecordsByQuery(String storagePath, String query) {
        return getRecordsByQuery(storagePath, query, 200);
    }

    public static JsonObject getRecordsByQuery(String storagePath, String query, int expectedResponseCode) {
        Response response = RestAssured.given()
                .get(storagePath + "?" + query)
                .then()
                .log().ifValidationFails()
                .statusCode(expectedResponseCode).extract().response();
        return new JsonObject(response.getBody().asString());
    }

    public static JsonObject getRecordById(String storagePath, String id) {
        return getRecordById(storagePath, id, 200);
    }

    public static JsonObject getRecordById(String storagePath, String id, int expectedResponseCode) {
        Response response = RestAssured.given()
                .get(storagePath + "/" + id)
                .then()
                .log().ifValidationFails()
                .statusCode(expectedResponseCode).extract().response();
        return new JsonObject(response.getBody().asString());
    }

    public static JsonObject post(String storagePath, JsonObject recordToPOST) {
        return post(storagePath, recordToPOST, 201);
    }

    public static JsonObject post(String storagePath, JsonObject recordToPOST, int expectedResponseCode) {
        Response response = RestAssured.given()
                .body(recordToPOST.toString())
                .post(storagePath)
                .then()
                .log().ifValidationFails()
                .statusCode(expectedResponseCode).extract().response();
        if (response.getContentType().equals("application/json")) {
            return new JsonObject(response.getBody().asString());
        } else {
            return new JsonObject().put("response", response.asString());
        }
    }

    public static void put(String storagePath, JsonObject recordToPUT) {
        put(storagePath, recordToPUT, 204);
    }

    public static void put(String storagePath, JsonObject recordToPUT, int expectedResponseCode) {
        RestAssured.given()
                .body(recordToPUT.toString())
                .put(storagePath + "/" + recordToPUT.getString("id"))
                .then()
                .log().ifValidationFails()
                .statusCode(expectedResponseCode).extract().response();
    }

    public static void delete(String storagePath, String id) {
        delete(storagePath, id, 200);
    }

    public static void delete(String storagePath, String id, int expectedResponseCode) {
        RestAssured.given()
                .delete(storagePath + "/" + id)
                .then()
                .log().ifValidationFails()
                .statusCode(expectedResponseCode);

    }

}
