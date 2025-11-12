package org.folio.inventoryupdate.unittests.fakestorage;

import io.vertx.core.json.JsonObject;
import io.restassured.RestAssured;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.folio.inventoryupdate.unittests.fixtures.Service;
import org.folio.inventoryupdate.importing.foliodata.ConfigurationsClient;
import org.folio.inventoryupdate.importing.foliodata.SettingsClient;

public class FakeFolioApisForImporting extends FakeApis {
  public static final int PORT_OKAPI = 9031;
  public static final String INSTANCE_STORAGE_PATH = "/instance-storage/instances";
  public static final String INSTANCE_SET_PATH = "/inventory-view/instance-set";
  public static final String INSTANCE_RELATIONSHIP_STORAGE_PATH = "/instance-storage/instance-relationships";
  public static final String PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH = "/preceding-succeeding-titles";
  public static final String HOLDINGS_STORAGE_PATH = "/holdings-storage/holdings";
  public static final String ITEM_STORAGE_PATH = "/item-storage/items";
  public static final String LOCATION_STORAGE_PATH = "/locations";

  public static final String INSTANCE_STORAGE_BATCH_PATH = "/instance-storage/batch/synchronous";
  public static final String HOLDINGS_STORAGE_BATCH_PATH = "/holdings-storage/batch/synchronous";
  public static final String ITEM_STORAGE_BATCH_PATH = "/item-storage/batch/synchronous";

  public static final String ORDER_LINES_STORAGE_PATH = "/orders-storage/po-lines";

  public static final String RESULT_SET_INSTANCES = "instances";
  public static final String RESULT_SET_HOLDINGS_RECORDS = "holdingsRecords";
  public static final String RESULT_SET_ITEMS = "items";


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
