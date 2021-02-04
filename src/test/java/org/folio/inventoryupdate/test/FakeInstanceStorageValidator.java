package org.folio.inventoryupdate.test;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;


/**
 *
 * Validates that the fake storage API behaves as expected for testing the instance match service.
 */
public class FakeInstanceStorageValidator {

  public static void validateStorage(TestContext context) {
    validateGetByQuery(context);
    validatePost(context);
    validatePut(context);
  }

    /**
   * Tests fake storage GET method
   * @param testContext
   */
  private static void validateGetByQuery (TestContext testContext) {

    String bodyAsString;
    Response response1;

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    response1 = RestAssured.given()
      .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="+ FakeInventoryStorage.encode("title==\"Initial Instance\""))
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
   * @param testContext
   */
  private static void validatePost (TestContext testContext) {

    String bodyAsString1;
    Response response1;
    JsonObject newInstance = new Instance().setTitle("New Instance").setInstanceTypeId("12345").getJson();

    response1 = RestAssured.given()
      .body(newInstance.toString())
      .post(FakeInventoryStorage.INSTANCE_STORAGE_PATH)
      .then()
      .log().ifValidationFails()
      .statusCode(201).extract().response();

    bodyAsString1 = response1.getBody().asString();
    JsonObject instanceResponse = new JsonObject(bodyAsString1);

    testContext.assertEquals(instanceResponse.getString("title"), "New Instance");

    // Fetch new instance by ID to validate POST
    String bodyAsString2;
    Response response2;
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    response2 = RestAssured.given()
      .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"/"+instanceResponse.getString("id"))
      .then()
      .log().ifValidationFails()
      .statusCode(200).extract().response();

    bodyAsString2 = response2.getBody().asString();
    JsonObject instance = new JsonObject(bodyAsString2);

    testContext.assertEquals(instance.getString("title"), "New Instance");

  }

  /**
   * Test fake storage PUT method
   * @param testContext
   */
  private static void validatePut(TestContext testContext) {

    // Find existing instance
    String bodyAsString1;
    Response response1;
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    response1 = RestAssured.given()
      .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="+ FakeInventoryStorage.encode("title==\"Initial Instance\""))
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
      .put(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"/"+existingInstance.getString("id"))
      .then()
      .log().ifValidationFails()
      .statusCode(204).extract().response();

    // Fetch instance by ID to validate PUT with updated property
    String bodyAsString2;
    Response response2;
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    response2 = RestAssured.given()
      .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"/"+existingInstance.getString("id"))
      .then()
      .log().ifValidationFails()
      .statusCode(200).extract().response();

    bodyAsString2 = response2.getBody().asString();
    JsonObject instance = new JsonObject(bodyAsString2);

    testContext.assertEquals(instance.getString("instanceTypeId"), "456");

  }

}
