package org.folio.inventoryupdate.importing.test.fakestorage;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.importing.test.fixtures.Service;

public abstract class FakeApis {
  public final ConfigurationStorage configurationStorage = new ConfigurationStorage();
  public final SettingsStorage settingsStorage = new SettingsStorage();
  public LocationStorage locationStorage = new LocationStorage();
  public InstanceStorage instanceStorage = new InstanceStorage();
  public InstanceSetView instanceSetview = new InstanceSetView();
  public HoldingsStorage holdingsStorage = new HoldingsStorage();
  public ItemStorage itemStorage = new ItemStorage();
  public InstanceRelationshipStorage instanceRelationshipStorage = new InstanceRelationshipStorage();
  public PrecedingSucceedingStorage precedingSucceedingStorage = new PrecedingSucceedingStorage();

  public OrdersStorage ordersStorage = new OrdersStorage();

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
    RestAssured.port = FakeFolioApisForImporting.PORT_OKAPI;
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
