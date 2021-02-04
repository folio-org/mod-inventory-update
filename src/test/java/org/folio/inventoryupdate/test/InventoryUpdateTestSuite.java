package org.folio.inventoryupdate.test;

import org.folio.inventoryupdate.MainVerticle;
import org.folio.inventoryupdate.MatchKey;
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
public class InventoryUpdateTestSuite {

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,
        "io.vertx.core.logging.Log4jLogDelegateFactory");
  }
  Vertx vertx;
  private final int PORT_INVENTORY_UPDATE = 9031;
  private final Header OKAPI_URL_HEADER = new Header("X-Okapi-Url", "http://localhost:"
      + FakeInventoryStorage.PORT_INVENTORY_STORAGE);

  private FakeInventoryStorage fakeInventoryStorage;

  public InventoryUpdateTestSuite() {}

  @Before
  public void setUp(TestContext testContext) {
    vertx = Vertx.vertx();

    // Register the testContext exception handler to catch assertThat
    vertx.exceptionHandler(testContext.exceptionHandler());

    deployService(testContext, testContext.async());
  }

  private void deployService(TestContext testContext, Async async) {
    System.setProperty("port", String.valueOf(PORT_INVENTORY_UPDATE));
    vertx.deployVerticle(MainVerticle.class.getName(), new DeploymentOptions(),
      r -> {
        testContext.assertTrue(r.succeeded());
        fakeInventoryStorage = new FakeInventoryStorage(vertx, testContext, async);
        initializeStorages();
      });
  }

  public void initializeStorages() {
    Instance instance = new Instance().setInstanceTypeId("123").setTitle("Initial Instance").setHrid("1");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    fakeInventoryStorage.instanceStorage.insert(instance);
    fakeInventoryStorage.instanceStorage.setFakeStorage(fakeInventoryStorage);
    fakeInventoryStorage.precedingSucceedingStorage.setFakeStorage(fakeInventoryStorage);
  }


  @Test
  public void testFakeInventoryStorage(TestContext testContext) {
    StorageValidatorInstances.validateStorage(testContext);
    StorageValidatorPrecedingSucceeding.validateStorage(testContext);
  }

  /**
   * Tests API /instance-storage-match/instances
   * @param testContext
   */
  @Test
  public void testPushOfNewInstanceWillCreateNewInstanceForMatchKey (TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Instance instance = new Instance()
        .setTitle("New title")
        .setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    Response instancesBeforePut =
      RestAssured.given()
        .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="+ RecordStorage
            .encode("matchKey==\"" + matchKey.getKey() + "\""))
        .then()
        .log().ifValidationFails()
        .statusCode(200).extract().response();
    String bodyAsStringBeforePut = instancesBeforePut.getBody().asString();
    JsonObject instancesBeforePutJson = new JsonObject(bodyAsStringBeforePut);

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
        "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    RestAssured.port = PORT_INVENTORY_UPDATE;
    RestAssured.given()
            .body(instance.getJson().toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(MainVerticle.INSTANCE_MATCH_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response instancesAfterPut =
      RestAssured.given()
        .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="
            + RecordStorage.encode("matchKey==\"" + matchKey.getKey() + "\""))
        .then()
        .log().ifValidationFails()
        .statusCode(200).extract().response();
    String bodyAsStringAfterPut = instancesAfterPut.getBody().asString();
    JsonObject instancesAfterPutJson = new JsonObject(bodyAsStringAfterPut);

    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
                             "Number of instance records for query by matchKey 'new_title' after PUT expected: 1" );

  }

  /**
   * Tests API /shared-inventory-upsert-matchkey
   * @param testContext
   */
  @Test
  public void testUpsertByMatchKeyWillCreateNewInstanceForMatchKey (TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Instance instance = new Instance()
            .setTitle("New title")
            .setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    InventoryRecordSet recordSet = new InventoryRecordSet(instance);
    Response instancesBeforePut =
            RestAssured.given()
                    .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="+ RecordStorage
                            .encode("matchKey==\"" + matchKey.getKey() + "\""))
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    String bodyAsStringBeforePut = instancesBeforePut.getBody().asString();
    JsonObject instancesBeforePutJson = new JsonObject(bodyAsStringBeforePut);

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
            "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    RestAssured.port = PORT_INVENTORY_UPDATE;
    RestAssured.given()
            .body(recordSet.getJson().toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response instancesAfterPut =
            RestAssured.given()
                    .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="
                            + RecordStorage.encode("matchKey==\"" + matchKey.getKey() + "\""))
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    String bodyAsStringAfterPut = instancesAfterPut.getBody().asString();
    JsonObject instancesAfterPutJson = new JsonObject(bodyAsStringAfterPut);

    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey 'new_title' after PUT expected: 1" );

  }

  /**
   * Tests API /instance-storage-match/instances
   * @param testContext
   */
  @Test
  public void testPushOfExistingInstanceWillUpdateExistingInstanceForMatchKey (TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Instance instance = new Instance()
        .setTitle("Initial Instance")
        .setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    Response instancesBeforePut =
      RestAssured.given()
        .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="+ RecordStorage
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

    RestAssured.port = PORT_INVENTORY_UPDATE;
    RestAssured.given()
            .body(instance.getJson().toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(MainVerticle.INSTANCE_MATCH_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response instancesAfterPut =
      RestAssured.given()
        .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="
            + RecordStorage.encode("matchKey==\"" + matchKey.getKey() + "\""))
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

  /**
   * Tests API /instance-storage-match/instances
   * @param testContext
   */
  @Test
  public void testPushOfNewInstanceWillCreateNewInstanceForHrid (TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Instance instance = new Instance()
        .setTitle("New title")
        .setInstanceTypeId("12345")
        .setHrid("2");
    Response instancesBeforePut =
      RestAssured.given()
        .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="+ RecordStorage
            .encode("hrid==\"" + instance.getHrid() + "\""))
        .then()
        .log().ifValidationFails()
        .statusCode(200).extract().response();
    String bodyAsStringBeforePut = instancesBeforePut.getBody().asString();
    JsonObject instancesBeforePutJson = new JsonObject(bodyAsStringBeforePut);

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
        "Number of instance records for query by hrid '2' before PUT expected: 0" );

    RestAssured.port = PORT_INVENTORY_UPDATE;
    RestAssured.given()
            .body(instance.getJson().toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(MainVerticle.INSTANCE_MATCH_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response instancesAfterPut =
      RestAssured.given()
        .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="
            + RecordStorage.encode("hrid==\"" + instance.getHrid() + "\""))
        .then()
        .log().ifValidationFails()
        .statusCode(200).extract().response();
    String bodyAsStringAfterPut = instancesAfterPut.getBody().asString();
    JsonObject instancesAfterPutJson = new JsonObject(bodyAsStringAfterPut);

    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
                             "Number of instance records for query by hrid '2' after PUT expected: 1" );

  }

  /**
   * Tests API /instance-storage-match/instances
   * @param testContext
   */
  @Test
  public void testPushOfExistingInstanceWillUpdateExistingInstanceForHrid (TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Instance instance = new Instance()
        .setTitle("Initial Instance")
        .setInstanceTypeId("12345")
        .setHrid("1");
    Response instancesBeforePut =
      RestAssured.given()
        .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="+ RecordStorage
            .encode("hrid==\"" + instance.getHrid() + "\""))
        .then()
        .log().ifValidationFails()
        .statusCode(200).extract().response();
    String bodyAsStringBeforePut = instancesBeforePut.getBody().asString();
    JsonObject instancesBeforePutJson = new JsonObject(bodyAsStringBeforePut);

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Number of instance records for query by hrid '1' before PUT expected: 1" );

    String instanceTypeIdBefore = instancesBeforePutJson.getJsonArray("instances")
        .getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdBefore,"123",
                    "Expected instanceTypeId to be '123' before PUT");

    RestAssured.port = PORT_INVENTORY_UPDATE;
    RestAssured.given()
            .body(instance.getJson().toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(MainVerticle.INSTANCE_MATCH_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response instancesAfterPut =
      RestAssured.given()
        .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="
            + RecordStorage.encode("hrid==\"" + instance.getHrid() + "\""))
        .then()
        .log().ifValidationFails()
        .statusCode(200).extract().response();
    String bodyAsStringAfterPut = instancesAfterPut.getBody().asString();
    JsonObject instancesAfterPutJson = new JsonObject(bodyAsStringAfterPut);

    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
        "Number of instance records for query by hrid '1' after PUT expected: 1" );
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
