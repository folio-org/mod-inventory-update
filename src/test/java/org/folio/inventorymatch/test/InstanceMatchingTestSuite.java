package org.folio.inventorymatch.test;

import org.folio.inventorymatch.MainVerticle;
import org.folio.inventorymatch.MatchKey;
import org.folio.inventorymatch.MatchService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class InstanceMatchingTestSuite {

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,
        "io.vertx.core.logging.Log4jLogDelegateFactory");
  }
  Vertx vertx;
  private final int PORT_INVENTORY_MATCH = 9031;
  private final Header TENANT_HEADER = new Header("X-Okapi-Tenant", "testlib");
  private final Header OKAPI_URL_HEADER = new Header("X-Okapi-Url", "http://localhost:"
      + FakeInventoryStorage.PORT_INVENTORY_STORAGE);
  private final Header CONTENT_TYPE = new Header("Content-type", "application/json");

  private FakeInventoryStorage inventoryStorage;

  public InstanceMatchingTestSuite() {}

  @Before
  public void setUp(TestContext testContext) {
    vertx = Vertx.vertx();

    // Register the testContext exception handler to catch assertThat
    vertx.exceptionHandler(testContext.exceptionHandler());

    setUpMatch(testContext, testContext.async());
  }

  private void setUpMatch(TestContext testContext, Async async) {
    System.setProperty("port", String.valueOf(PORT_INVENTORY_MATCH));
    vertx.deployVerticle(MainVerticle.class.getName(), new DeploymentOptions(),
      r -> {
        testContext.assertTrue(r.succeeded());
        inventoryStorage = new FakeInventoryStorage(vertx, testContext, async);
      });
  }

  @Test
  public void testFakeInventoryStorage(TestContext testContext) {
    FakeInventoryStorageValidator.validateStorage(inventoryStorage, testContext);
  }

  @Test
  public void testPushOfNewInstanceWillCreateNewInstance (TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Instance instance = new Instance()
        .setTitle("New title")
        .setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    Response instancesBeforePut =
      RestAssured.given()
        .get(FakeInventoryStorage.URL_INSTANCES+"?query="+ FakeInventoryStorage
            .encode("matchKey==\"" + matchKey.getKey() + "\""))
        .then()
        .log().ifValidationFails()
        .statusCode(200).extract().response();
    String bodyAsStringBeforePut = instancesBeforePut.getBody().asString();
    JsonObject instancesBeforePutJson = new JsonObject(bodyAsStringBeforePut);

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
        "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    Response response;
    RestAssured.port = PORT_INVENTORY_MATCH;
    response = RestAssured.given()
            .body(instance.getJson().toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(MatchService.INSTANCE_MATCH_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response instancesAfterPut =
      RestAssured.given()
        .get(FakeInventoryStorage.URL_INSTANCES+"?query=" 
            + FakeInventoryStorage.encode("matchKey==\"" + matchKey.getKey() + "\""))
        .then()
        .log().ifValidationFails()
        .statusCode(200).extract().response();
    String bodyAsStringAfterPut = instancesAfterPut.getBody().asString();
    JsonObject instancesAfterPutJson = new JsonObject(bodyAsStringAfterPut);

    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
                             "Number of instance records for query by matchKey 'new_title' after PUT expected: 1" );

  }

  @Test
  public void testPushOfExistingInstanceWillUpdateExistingInstance (TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Instance instance = new Instance()
        .setTitle("Initial Instance")
        .setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    Response instancesBeforePut =
      RestAssured.given()
        .get(FakeInventoryStorage.URL_INSTANCES+"?query="+ FakeInventoryStorage
            .encode("matchKey==\"" + matchKey.getKey() + "\""))
        .then()
        .log().ifValidationFails()
        .statusCode(200).extract().response();
    String bodyAsStringBeforePut = instancesBeforePut.getBody().asString();
    JsonObject instancesBeforePutJson = new JsonObject(bodyAsStringBeforePut);

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Number of instance records for query by matchKey 'initial instance' before PUT expected: 1" );

    String instanceTypeIdBefore = instancesBeforePutJson.getJsonArray("instances")
        .getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdBefore,"123",
                    "Expected instanceTypeId to be '123' before PUT");

    Response response;
    RestAssured.port = PORT_INVENTORY_MATCH;
    //Instance instance = new Instance().setTitle("Initial Instance").setInstanceTypeId("12345");
    response = RestAssured.given()
            .body(instance.getJson().toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(MatchService.INSTANCE_MATCH_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response instancesAfterPut =
      RestAssured.given()
        .get(FakeInventoryStorage.URL_INSTANCES+"?query="
            + FakeInventoryStorage.encode("matchKey==\"" + matchKey.getKey() + "\""))
        .then()
        .log().ifValidationFails()
        .statusCode(200).extract().response();
    String bodyAsStringAfterPut = instancesAfterPut.getBody().asString();
    JsonObject instancesAfterPutJson = new JsonObject(bodyAsStringAfterPut);

    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
        "Number of instance records for query by matchKey 'initial instance' after PUT expected: 1" );
    String instanceTypeIdAfter = instancesAfterPutJson.getJsonArray("instances")
        .getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdAfter,"12345",
        "Expected instanceTypeId to be '12345' after PUT");

  }

  @After
  public void tearDown(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      async.complete();
    }));
  }

}
