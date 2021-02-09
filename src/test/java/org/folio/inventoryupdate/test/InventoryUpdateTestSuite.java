package org.folio.inventoryupdate.test;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.json.JsonArray;
import org.folio.inventoryupdate.MainVerticle;
import org.folio.inventoryupdate.MatchKey;
import org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage;
import org.folio.inventoryupdate.test.fakestorage.RecordStorage;
import org.folio.inventoryupdate.test.fakestorage.entitites.*;
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

  private final Logger logger = io.vertx.core.impl.logging.LoggerFactory.getLogger("InventoryUpdateTestSuite");

  public InventoryUpdateTestSuite() {

  }

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
            });
  }

  public void populateStoragesWithInitialRecords() {
    TestInstance instance = new TestInstance().setInstanceTypeId("123").setTitle("Initial TestInstance").setHrid("1");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    fakeInventoryStorage.instanceStorage.insert(instance);
  }

  public void createInitialInstanceWithHrid1() {
    TestInstance instance = new TestInstance().setInstanceTypeId("123").setTitle("Initial TestInstance").setHrid("1");
    fakeInventoryStorage.instanceStorage.insert(instance);
  }


  @Test
  public void testFakeInventoryStorage(TestContext testContext) {
    populateStoragesWithInitialRecords();
    new StorageValidatorInstances().validateStorage(testContext);
    new StorageValidatorHoldingsRecords().validateStorage(testContext);
    new StorageValidatorItems().validateStorage(testContext);
    new StorageValidatorPrecedingSucceeding().validateStorage(testContext);
    new StorageValidatorInstanceRelationships().validateStorage(testContext);
  }

  /**
   * Tests API /instance-storage-match/instances
   * @param testContext
   */
  @Test
  public void testPushOfNewInstanceWillCreateNewInstanceForMatchKey (TestContext testContext) {
    populateStoragesWithInitialRecords();
    if (logger.isDebugEnabled()) {
      fakeInventoryStorage.instanceStorage.logRecords(logger);
    }
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    TestInstance instance = new TestInstance()
            .setTitle("New title")
            .setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    InventoryRecordSet inventoryRecordSet = new InventoryRecordSet(instance);
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
            .body(inventoryRecordSet.getJson().toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response instancesAfterPut =
            RestAssured.given()
                    .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query=matchKey==\"" + matchKey.getKey() + "\"")
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
    populateStoragesWithInitialRecords();
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    TestInstance instance = new TestInstance()
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
   * Tests API /shared-inventory-upsert-matchkey/instances
   * @param testContext
   */
  @Test
  public void testPushOfExistingInstanceWillUpdateExistingInstanceForMatchKey (TestContext testContext) {
    populateStoragesWithInitialRecords();
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    TestInstance instance = new TestInstance().setTitle("Initial TestInstance").setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    InventoryRecordSet inventoryRecordSet = new InventoryRecordSet(instance);

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
            .body(inventoryRecordSet.getJson().toString())
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
            "Number of instance records for query by matchKey 'initial instance' after PUT expected: 1" );
    String instanceTypeIdAfter = instancesAfterPutJson.getJsonArray("instances")
            .getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdAfter,"12345",
            "Expected instanceTypeId to be '12345' after PUT");

  }

  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void testUpsertWithNewInstanceWillCreateNewInstanceForHrid(TestContext testContext) {
    createInitialInstanceWithHrid1();
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    TestInstance instance = new TestInstance().setTitle("New title").setInstanceTypeId("12345").setHrid("2");
    InventoryRecordSet inventoryRecordSet = new InventoryRecordSet(instance);
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
            .body(inventoryRecordSet.getJson().toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(MainVerticle.INVENTORY_UPSERT_HRID_PATH)
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
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void testUpsertOfExistingInstanceWillUpdateExistingInstanceForHrid (TestContext testContext) {
    createInitialInstanceWithHrid1();
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject();
    inventoryRecordSet.put("instance", new TestInstance().setTitle("Initial TestInstance")
            .setInstanceTypeId("12345").setHrid(instanceHrid).getJson());

    Response instancesBeforeUpsert =
            RestAssured.given()
                    .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="+ RecordStorage
                            .encode("hrid==\"" + instanceHrid + "\""))
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    String bodyAsStringBeforePut = instancesBeforeUpsert.getBody().asString();
    JsonObject instancesBeforePutJson = new JsonObject(bodyAsStringBeforePut);

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by hrid '1' before PUT expected: 1" );

    String instanceTypeIdBefore = instancesBeforePutJson.getJsonArray("instances")
            .getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdBefore,"123",
            "Expected instanceTypeId to be '123' before PUT");

    // PUT InventoryRecordSet
    RestAssured.port = PORT_INVENTORY_UPDATE;
    RestAssured.given()
            .body(inventoryRecordSet.toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(MainVerticle.INVENTORY_UPSERT_HRID_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response instancesAfterUpsert =
            RestAssured.given()
                    .get(FakeInventoryStorage.INSTANCE_STORAGE_PATH +"?query="
                            + RecordStorage.encode("hrid==\"" + instanceHrid + "\""))
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    String bodyAsStringAfterPut = instancesAfterUpsert.getBody().asString();
    JsonObject instancesAfterUpsertJson = new JsonObject(bodyAsStringAfterPut);

    testContext.assertEquals(instancesAfterUpsertJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by hrid '1' after PUT expected: 1 " + instancesAfterUpsertJson.encodePrettily() );
    JsonObject instanceResponse = instancesAfterUpsertJson.getJsonArray("instances").getJsonObject(0);
    String instanceTypeIdAfter = instanceResponse.getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdAfter,"12345",
            "Expected instanceTypeId to be '12345' after upsert by HRID");
  }

  @Test
  public void testRecordSetWithHoldingsAndItemsToCreate(TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;

    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
               new TestInstance().setTitle("Initial TestInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
              .add(new TestHoldingsRecord().setHrid("HOL-001").setCallNumber("test-cn-1").getJson()
                      .put("items", new JsonArray()
                        .add(new TestItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                        .add(new TestItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
              .add(new TestHoldingsRecord().setHrid("HOL-002").setCallNumber("test-cn-2").getJson()
                      .put("items", new JsonArray()
                        .add(new TestItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));

    // PUT InventoryRecordSet
    RestAssured.port = PORT_INVENTORY_UPDATE;
    Response upsertResponse =
            RestAssured.given()
                    .body(inventoryRecordSet.toString())
                    .header("Content-type","application/json")
                    .header(OKAPI_URL_HEADER)
                    .put(MainVerticle.INVENTORY_UPSERT_HRID_PATH)
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    JsonObject upsertResponseJson = new JsonObject(upsertResponse.getBody().asString());
    logger.debug("UpsertResponse: " + upsertResponseJson.encodePrettily());
    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "CREATED" , "COMPLETED"), 2,
            "Metrics should report two holdings records successfully created " + upsertResponseJson.encodePrettily());

    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "CREATED" , "COMPLETED"), 3,
            "Metrics should report three items successfully created " + upsertResponseJson.encodePrettily());

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response holdingsAfterUpsert =
            RestAssured.given()
                    .get(FakeInventoryStorage.HOLDINGS_STORAGE_PATH +"?query="
                            + RecordStorage.encode("instanceId==\"" + instanceId + "\""))
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    String bodyAsStringAfterPut = holdingsAfterUpsert.getBody().asString();
    JsonObject holdingsAfterUpsertJson = new JsonObject(bodyAsStringAfterPut);

    logger.debug("Holdings after upsert 2: " + holdingsAfterUpsertJson.encodePrettily());

    testContext.assertEquals(holdingsAfterUpsertJson.getInteger("totalRecords"), 2,
            "Number of holdings records for query by instanceId " + instanceId + " after upsert expected: 2 " + holdingsAfterUpsertJson.encodePrettily() );
  }



  @Test
  public void testDeletionOfSelectHoldingsAndItems(TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new TestInstance().setTitle("Initial TestInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new TestHoldingsRecord().setHrid("HOL-001").setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new TestItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new TestItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new TestHoldingsRecord().setHrid("HOL-002").setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new TestItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));
    // PUT InventoryRecordSet
    RestAssured.port = PORT_INVENTORY_UPDATE;
    Response upsertResponse =
            RestAssured.given()
                    .body(inventoryRecordSet.toString())
                    .header("Content-type","application/json")
                    .header(OKAPI_URL_HEADER)
                    .put(MainVerticle.INVENTORY_UPSERT_HRID_PATH)
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    JsonObject upsertResponseJson = new JsonObject(upsertResponse.getBody().asString());
    logger.debug("UpsertResponse: " + upsertResponseJson.encodePrettily());
    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    // Leave out one holdings record
    inventoryRecordSet = new JsonObject()
            .put("instance",
                    new TestInstance().setTitle("Initial TestInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new TestHoldingsRecord().setHrid("HOL-002").setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new TestItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));

    // PUT InventoryRecordSet again
    RestAssured.port = PORT_INVENTORY_UPDATE;
    upsertResponse =
            RestAssured.given()
                    .body(inventoryRecordSet.toString())
                    .header("Content-type","application/json")
                    .header(OKAPI_URL_HEADER)
                    .put(MainVerticle.INVENTORY_UPSERT_HRID_PATH)
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    upsertResponseJson = new JsonObject(upsertResponse.getBody().asString());
    logger.debug("UpsertResponse 2: " + upsertResponseJson.encodePrettily());

    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "DELETED" , "COMPLETED"), 1,
            "Metrics should report one (1) holdings record successfully deleted " + upsertResponseJson.encodePrettily());

    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "DELETED" , "COMPLETED"), 2,
            "Metrics should report two items successfully deleted " + upsertResponseJson.encodePrettily());

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response holdingsAfterUpsert =
            RestAssured.given()
                    .get(FakeInventoryStorage.HOLDINGS_STORAGE_PATH +"?query="
                            + RecordStorage.encode("instanceId==\"" + instanceId + "\""))
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    String bodyAsStringAfterPut = holdingsAfterUpsert.getBody().asString();
    JsonObject holdingsAfterUpsertJson = new JsonObject(bodyAsStringAfterPut);

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response itemsAfterUpsert =
            RestAssured.given()
                    .get(FakeInventoryStorage.HOLDINGS_STORAGE_PATH)
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    JsonObject itemsAfterUpsertJson = new JsonObject(itemsAfterUpsert.getBody().asString());

    testContext.assertEquals(itemsAfterUpsertJson.getInteger("totalRecords"), 1,
            "Number of item records after upsert expected: 1 " + itemsAfterUpsertJson.encodePrettily() );
  }

  @Test
  public void testRecordSetWithParentInstanceRelation(TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;

    // CREATE PARENT TO-BE
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new TestInstance().setTitle("Parent TestInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson());

    // CREATE CHILD
    String childHrid = "2";
    JsonObject childRecordSet = new JsonObject()
            .put("instance",
                    new TestInstance().setTitle("Child TestInstance").setInstanceTypeId("12345").setHrid(childHrid).getJson())
            .put("instanceRelations", new JsonObject()
              .put("parentInstances", new JsonArray()
                .add(new TestInstanceRelationship().setInstanceIdentifierHrid(instanceHrid).getJson())));

    // PUT PARENT InventoryRecordSet
    RestAssured.port = PORT_INVENTORY_UPDATE;
    Response upsertResponse =
            RestAssured.given()
                    .body(inventoryRecordSet.toString())
                    .header("Content-type","application/json")
                    .header(OKAPI_URL_HEADER)
                    .put(MainVerticle.INVENTORY_UPSERT_HRID_PATH)
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    JsonObject upsertResponseJson = new JsonObject(upsertResponse.getBody().asString());
    logger.debug("UpsertResponse: " + upsertResponseJson.encodePrettily());
    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    // PUT CHILD InventoryRecordSet
    RestAssured.port = PORT_INVENTORY_UPDATE;
    Response childResponse =
            RestAssured.given()
                    .body(childRecordSet.toString())
                    .header("Content-type","application/json")
                    .header(OKAPI_URL_HEADER)
                    .put(MainVerticle.INVENTORY_UPSERT_HRID_PATH)
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    JsonObject childResponseJson = new JsonObject(childResponse.getBody().asString());
    logger.debug("childResponse: " + childResponseJson.encodePrettily());

    testContext.assertEquals(getMetric(childResponseJson, "INSTANCE_RELATIONSHIP", "CREATED" , "COMPLETED"), 1,
            "Metrics should report one (1) instance relationship successfully created " + childResponseJson.encodePrettily());

    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response relationshipsAfterUpsert =
            RestAssured.given()
                    .get(FakeInventoryStorage.INSTANCE_RELATIONSHIP_STORAGE_PATH)
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    JsonObject relationshipsAfterUpsertJson = new JsonObject(relationshipsAfterUpsert.getBody().asString());

    logger.debug("Relationships after creating child Instance: " + relationshipsAfterUpsertJson.encodePrettily());

    testContext.assertEquals(relationshipsAfterUpsertJson.getInteger("totalRecords"), 1,
            "Number of holdings records for query by instanceId " + instanceId + " after upsert expected: 2 " + relationshipsAfterUpsertJson.encodePrettily() );
  }

  @After
  public void tearDown(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      async.complete();
    }));
  }

  private int getMetric (JsonObject upsertResponse, String entity, String transaction, String outcome) {
    logger.debug("Checking " + entity + " " + transaction + " " + outcome);
    if (upsertResponse.containsKey("metrics")
            && upsertResponse.getJsonObject("metrics").containsKey(entity)
            && upsertResponse.getJsonObject("metrics").getJsonObject(entity).containsKey(transaction)
            && upsertResponse.getJsonObject("metrics").getJsonObject(entity).getJsonObject(transaction).containsKey(outcome)) {
      return upsertResponse.getJsonObject("metrics").getJsonObject(entity).getJsonObject(transaction).getInteger(outcome);
    } else {
      return -1;
    }
  }

}
