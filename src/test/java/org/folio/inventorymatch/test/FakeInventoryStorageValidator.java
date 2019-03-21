package org.folio.inventorymatch.test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;


/**
 *
 * Validates that the fake storage API behaves as expected for testing the instance match service.
 */
public class FakeInventoryStorageValidator {

  public static void validateStorage(FakeInventoryStorage storage, TestContext context) {
    validateGetByQuery(storage, context);
    validatePost(storage, context);
    validatePut(storage, context);
  }

    /**
   * Tests fake storage GET method
   * @param storage
   * @param testContext
   */
  private static void validateGetByQuery (FakeInventoryStorage storage, TestContext testContext) {

    String bodyAsString;
    Response response1;

    RestAssured.port = storage.PORT_INVENTORY_STORAGE;
    response1 = RestAssured.given()
      .get(storage.URL_INSTANCES+"?query="+ encode("title=\"Initial Instance\""))
      .then()
      .log().ifValidationFails()
      .statusCode(200).extract().response();

    bodyAsString = response1.getBody().asString();
    JsonObject responseJson1 = new JsonObject(bodyAsString);

    testContext.assertEquals(responseJson1.getInteger("totalRecords"), 1,
                             "Number of instance records expected: 1" );
  }

  /**
   * Tests fake storage POST method
   * @param storage
   * @param testContext
   */
  private static void validatePost (FakeInventoryStorage storage, TestContext testContext) {

    String bodyAsString1;
    Response response1;
    JsonObject newInstance = new Instance().setTitle("New Instance").setInstanceTypeId("123").getJson();

    response1 = RestAssured.given()
      .body(newInstance.toString())
      .post(storage.URL_INSTANCES)
      .then()
      .log().ifValidationFails()
      .statusCode(201).extract().response();

    bodyAsString1 = response1.getBody().asString();
    JsonObject instanceResponse = new JsonObject(bodyAsString1);

    testContext.assertEquals(instanceResponse.getString("title"), "New Instance");

    // Fetch new instance by ID to validate POST
    String bodyAsString2;
    Response response2;
    RestAssured.port = storage.PORT_INVENTORY_STORAGE;
    response2 = RestAssured.given()
      .get(storage.URL_INSTANCES+"/"+instanceResponse.getString("id"))
      .then()
      .log().ifValidationFails()
      .statusCode(200).extract().response();

    bodyAsString2 = response2.getBody().asString();
    JsonObject instance = new JsonObject(bodyAsString2);

    testContext.assertEquals(instance.getString("title"), "New Instance");

  }

  /**
   * Test fake storage PUT method
   * @param storage
   * @param testContext
   */
  private static void validatePut(FakeInventoryStorage storage, TestContext testContext) {

    // Find existing instance
    String bodyAsString1;
    Response response1;
    RestAssured.port = storage.PORT_INVENTORY_STORAGE;
    response1 = RestAssured.given()
      .get(storage.URL_INSTANCES+"?query="+ encode("title=\"Initial Instance\""))
      .then()
      .log().ifValidationFails()
      .statusCode(200).extract().response();
    bodyAsString1 = response1.getBody().asString();
    JsonObject responseJson1 = new JsonObject(bodyAsString1);

    JsonObject existingInstance = responseJson1.getJsonArray("instances").getJsonObject(0);

    // Update property
    existingInstance.put("instanceTypeId", "456");

    // Update existing instance with PUT
    RestAssured.given()
      .body(existingInstance.toString())
      .put(storage.URL_INSTANCES+"/"+existingInstance.getString("id"))
      .then()
      .log().ifValidationFails()
      .statusCode(204).extract().response();

    // Fetch instance by ID to validate PUT with updated property
    String bodyAsString2;
    Response response2;
    RestAssured.port = storage.PORT_INVENTORY_STORAGE;
    response2 = RestAssured.given()
      .get(storage.URL_INSTANCES+"/"+existingInstance.getString("id"))
      .then()
      .log().ifValidationFails()
      .statusCode(200).extract().response();

    bodyAsString2 = response2.getBody().asString();
    JsonObject instance = new JsonObject(bodyAsString2);

    testContext.assertEquals(instance.getString("instanceTypeId"), "456");

  }

  private static String encode (String string) {
    try {
      return URLEncoder.encode(string, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      return "";
    }
  }

}
