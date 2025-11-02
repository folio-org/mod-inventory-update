package org.folio.inventoryupdate.importing.test.fakestorage;

import io.vertx.core.json.JsonObject;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.folio.inventoryupdate.importing.foliodata.InventoryUpdateOverOkapiClient;
import org.folio.inventoryupdate.importing.test.fixtures.Service;
import org.folio.inventoryupdate.importing.foliodata.ConfigurationsClient;
import org.folio.inventoryupdate.importing.foliodata.SettingsClient;

import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApisForUpserts.HOLDINGS_STORAGE_BATCH_PATH;
import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApisForUpserts.HOLDINGS_STORAGE_PATH;
import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApisForUpserts.INSTANCE_RELATIONSHIP_STORAGE_PATH;
import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApisForUpserts.INSTANCE_SET_PATH;
import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApisForUpserts.INSTANCE_STORAGE_BATCH_PATH;
import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApisForUpserts.INSTANCE_STORAGE_PATH;
import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApisForUpserts.ITEM_STORAGE_BATCH_PATH;
import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApisForUpserts.ITEM_STORAGE_PATH;
import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApisForUpserts.LOCATION_STORAGE_PATH;
import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApisForUpserts.ORDER_LINES_STORAGE_PATH;
import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApisForUpserts.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH;


public class FakeFolioApisForImporting {

    public final ConfigurationStorage configurationStorage = new ConfigurationStorage();
    public final SettingsStorage settingsStorage = new SettingsStorage();
    public LocationStorage locationStorage = new LocationStorage();
    public InstanceStorage instanceStorage = new InstanceStorage();
    public InstanceSetView instanceSetview = new InstanceSetView();
    public HoldingsStorage holdingsStorage = new HoldingsStorage();
    public ItemStorage itemStorage = new ItemStorage();
    public InstanceRelationshipStorage instanceRelationshipStorage = new InstanceRelationshipStorage();
    public PrecedingSucceedingStorage precedingSucceedingStorage = new PrecedingSucceedingStorage();
    public InventoryUpsertStorage upsertStorage = new InventoryUpsertStorage();

    public OrdersStorage ordersStorage = new OrdersStorage();


    public FakeFolioApisForImporting(Vertx vertx, TestContext testContext) {
        configurationStorage.attachToFakeStorage(this);
        locationStorage.attachToFakeStorage(this);
        instanceStorage.attachToFakeStorage(this);
        instanceSetview.attachToFakeStorage(this);
        holdingsStorage.attachToFakeStorage(this);
        itemStorage.attachToFakeStorage(this);
        instanceRelationshipStorage.attachToFakeStorage(this);
        precedingSucceedingStorage.attachToFakeStorage(this);
        ordersStorage.attachToFakeStorage(this);
        upsertStorage.attachToFakeStorage(this);



      Router router = Router.router(vertx);
        router.get(ConfigurationsClient.CONFIGURATIONS_PATH).handler(configurationStorage::getRecords);
        router.get(ConfigurationsClient.CONFIGURATIONS_PATH + "/:id").handler(configurationStorage::getRecordById);
        router.get(SettingsClient.SETTINGS_PATH).handler(settingsStorage::getRecords);
        router.get(SettingsClient.SETTINGS_PATH + "/:id").handler(settingsStorage::getRecordById);
        router.post("/*").handler(BodyHandler.create());
        router.post(ConfigurationsClient.CONFIGURATIONS_PATH).handler(configurationStorage::createRecord);
        router.post(SettingsClient.SETTINGS_PATH).handler(settingsStorage::createRecord);
        router.put("/*").handler(BodyHandler.create());
        router.put(ConfigurationsClient.CONFIGURATIONS_PATH + "/:id").handler(configurationStorage::updateRecord);
        router.delete(ConfigurationsClient.CONFIGURATIONS_PATH + "/:id").handler(configurationStorage::deleteRecord);
        router.put(SettingsClient.SETTINGS_PATH + "/:id").handler(settingsStorage::updateRecord);
        router.delete(SettingsClient.SETTINGS_PATH + "/:id").handler(settingsStorage::deleteRecord);
        router.put(InventoryUpdateOverOkapiClient.INVENTORY_UPSERT_PATH).handler(upsertStorage::inventoryBatchUpsertHrid);
        router.delete("/*").handler(BodyHandler.create());
        router.delete(InventoryUpdateOverOkapiClient.INVENTORY_DELETION_PATH).handler(upsertStorage::deleteRecord);
      router.get(LOCATION_STORAGE_PATH).handler(locationStorage::getRecords);
      router.get(LOCATION_STORAGE_PATH + "/:id").handler(locationStorage::getRecordById);
      router.post(INSTANCE_STORAGE_PATH).handler(instanceStorage::createRecord);
      router.get(INSTANCE_STORAGE_PATH).handler(instanceStorage::getRecords);
      router.get(INSTANCE_STORAGE_PATH + "/:id").handler(instanceStorage::getRecordById);
      router.get(INSTANCE_SET_PATH).handler(instanceSetview::getRecords);
      router.get(HOLDINGS_STORAGE_PATH).handler(holdingsStorage::getRecords);
      router.get(HOLDINGS_STORAGE_PATH + "/:id").handler(holdingsStorage::getRecordById);
      router.get(ITEM_STORAGE_PATH).handler(itemStorage::getRecords);
      router.get(ITEM_STORAGE_PATH + "/:id").handler(itemStorage::getRecordById);
      router.get(INSTANCE_RELATIONSHIP_STORAGE_PATH).handler(instanceRelationshipStorage::getRecords);
      router.get(INSTANCE_RELATIONSHIP_STORAGE_PATH + "/:id").handler(instanceRelationshipStorage::getRecordById);
      router.get(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH).handler(precedingSucceedingStorage::getRecords);
      router.get(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH + "/:id").handler(precedingSucceedingStorage::getRecordById);
      router.get(ORDER_LINES_STORAGE_PATH).handler(ordersStorage::getRecords);
      router.get(ORDER_LINES_STORAGE_PATH + "/:id").handler(ordersStorage::getRecordById);
      router.post("/*").handler(BodyHandler.create());
      router.post(LOCATION_STORAGE_PATH).handler(locationStorage::createRecord);
      router.post(INSTANCE_STORAGE_PATH).handler(instanceStorage::createRecord);
      router.post(HOLDINGS_STORAGE_PATH).handler(holdingsStorage::createRecord);
      router.post(HOLDINGS_STORAGE_BATCH_PATH).handler(holdingsStorage::upsertRecords);
      router.post(ITEM_STORAGE_PATH).handler(itemStorage::createRecord);
      router.post(ITEM_STORAGE_BATCH_PATH).handler(itemStorage::upsertRecords);
      router.post(INSTANCE_RELATIONSHIP_STORAGE_PATH).handler(instanceRelationshipStorage::createRecord);
      router.post(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH).handler(precedingSucceedingStorage::createRecord);
      router.post(INSTANCE_STORAGE_BATCH_PATH).handler(instanceStorage::upsertRecords);
      router.post(ORDER_LINES_STORAGE_PATH).handler(ordersStorage::createRecord);
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
                .listen(Service.PORT_OKAPI)
                .onComplete(testContext.asyncAssertSuccess());
        RestAssured.port = Service.PORT_OKAPI;
    }

    public static JsonObject getRecordsByQuery(String storagePath, String query) {
        return getRecordsByQuery(storagePath, query, 200);
    }

    public static JsonObject getRecordsByQuery(String storagePath, String query, int expectedResponseCode) {
        Response response = RestAssured.given()
                .baseUri(Service.BASE_URI_OKAPI)
                .get(storagePath + "?" + query)
                .then()
                .log().ifValidationFails()
                .statusCode(expectedResponseCode).extract().response();
        return new JsonObject(response.getBody().asString());
    }


    public static JsonObject getRecordById(String storagePath, String id, int expectedResponseCode) {
        Response response =  RestAssured.given()
                .baseUri(Service.BASE_URI_OKAPI)
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
                .baseUri(Service.BASE_URI_OKAPI)
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
                .baseUri(Service.BASE_URI_OKAPI)
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
                .baseUri(Service.BASE_URI_OKAPI)
                .delete(storagePath + "/" + id)
                .then()
                .log().ifValidationFails()
                .statusCode(expectedResponseCode);

    }

}
