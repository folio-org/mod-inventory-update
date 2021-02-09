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
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;


@RunWith(VertxUnitRunner.class)
public class InventoryUpdateTestSuite {

  Vertx vertx;
  private static final int PORT_INVENTORY_UPDATE = 9031;
  private static final Header OKAPI_URL_HEADER = new Header("X-Okapi-Url", "http://localhost:"
          + FakeInventoryStorage.PORT_INVENTORY_STORAGE);

  private FakeInventoryStorage fakeInventoryStorage;

  private final Logger logger = io.vertx.core.impl.logging.LoggerFactory.getLogger("InventoryUpdateTestSuite");

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
            });
  }

  public void populateStoragesWithInitialRecords() {
    InputInstance instance = new InputInstance().setInstanceTypeId("123").setTitle("Initial InputInstance").setHrid("1");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    fakeInventoryStorage.instanceStorage.insert(instance);
  }

  public void createInitialInstanceWithHrid1() {
    InputInstance instance = new InputInstance().setInstanceTypeId("123").setTitle("Initial InputInstance").setHrid("1");
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
   * Tests API /shared-inventory-upsert-matchkey
   * @param testContext
   */
  @Test
  public void testPushOfNewInstanceWillCreateNewInstanceForMatchKey (TestContext testContext) {
    populateStoragesWithInitialRecords();
    if (logger.isDebugEnabled()) {
      fakeInventoryStorage.instanceStorage.logRecords(logger);
    }
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    InputInstance instance = new InputInstance().setTitle("New title").setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    InventoryRecordSet inventoryRecordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
            "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    inventoryRecordSetPUT(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH, inventoryRecordSet.getJson());

    JsonObject instancesAfterPutJson = getFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");

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
    InputInstance instance = new InputInstance()
            .setTitle("New title")
            .setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    InventoryRecordSet recordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
            "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    inventoryRecordSetPUT(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH, recordSet.getJson());

    JsonObject instancesAfterPutJson = getFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");

    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey 'new_title' after PUT expected: 1" );

  }

  /**
   * Tests API /shared-inventory-upsert-matchkey
   * @param testContext
   */
  @Test
  public void testPushOfExistingInstanceWillUpdateExistingInstanceForMatchKey (TestContext testContext) {
    populateStoragesWithInitialRecords();
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    InputInstance instance = new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    InventoryRecordSet inventoryRecordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH,"matchKey==\"" + matchKey.getKey() + "\"");

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey 'initial instance' before PUT expected: 1" );

    String instanceTypeIdBefore = instancesBeforePutJson.getJsonArray("instances")
            .getJsonObject(0).getString("instanceTypeId");

    testContext.assertEquals(instanceTypeIdBefore,"123",
            "Expected instanceTypeId to be '123' before PUT");

    inventoryRecordSetPUT(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH,inventoryRecordSet.getJson());

    JsonObject instancesAfterPutJson = getFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH,"matchKey==\"" + matchKey.getKey() + "\"");

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
  public void testUpsertWithNewInstanceByHrid(TestContext testContext) {
    createInitialInstanceWithHrid1();
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    InputInstance instance = new InputInstance().setTitle("New title").setInstanceTypeId("12345").setHrid("2");
    InventoryRecordSet inventoryRecordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "hrid==\"" + instance.getHrid() + "\"");

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
            "Before upserting with new Instance, number of Instance with that HRID expected to be 0" );

    inventoryRecordSetPUT(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet.getJson());

    JsonObject instancesAfterPutJson = getFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH,"hrid==\"" + instance.getHrid() + "\"");

    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "After upserting with new Instance, number of Instances with that HRID expected to be 1" );

  }

  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void testUpsertOfExistingInstanceByHrid (TestContext testContext) {
    createInitialInstanceWithHrid1();
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject();
    inventoryRecordSet.put("instance", new InputInstance().setTitle("Initial InputInstance")
            .setInstanceTypeId("12345").setHrid(instanceHrid).getJson());

    JsonObject instancesBeforePutJson = getFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "hrid==\"" + instanceHrid + "\"");

    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
            "Before upsert of existing Instance, number of Instances with that HRID expected to be [1]" );

    String instanceTypeIdBefore = instancesBeforePutJson.getJsonArray("instances")
            .getJsonObject(0).getString("instanceTypeId");

    testContext.assertEquals(instanceTypeIdBefore,"123",
            "Before upsert of existing Instance, the instanceTypeId expected to be [123]");

    inventoryRecordSetPUT(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet);

    JsonObject instancesAfterUpsertJson = getFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "hrid==\"" + instanceHrid + "\"");

    testContext.assertEquals(instancesAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert of existing Instance, number of Instances with that HRID still expected to be [1]" + instancesAfterUpsertJson.encodePrettily() );

    JsonObject instanceResponse = instancesAfterUpsertJson.getJsonArray("instances").getJsonObject(0);

    String instanceTypeIdAfter = instanceResponse.getString("instanceTypeId");

    testContext.assertEquals(instanceTypeIdAfter,"12345",
            "After upsert of existing Instance, the instanceTypeId expected to have changed to [12345]");
  }

  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void testCreationOfHoldingsAndItemsByHrid(TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;

    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
               new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
              .add(new InputHoldingsRecord().setHrid("HOL-001").setCallNumber("test-cn-1").getJson()
                      .put("items", new JsonArray()
                        .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                        .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
              .add(new InputHoldingsRecord().setHrid("HOL-002").setCallNumber("test-cn-2").getJson()
                      .put("items", new JsonArray()
                        .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));

    JsonObject upsertResponseJson = inventoryRecordSetPUT(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet);

    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "CREATED" , "COMPLETED"), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());

    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "CREATED" , "COMPLETED"), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    JsonObject storedHoldings = getFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");

    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After upsert the number of holdings records for instance " + instanceId + " expected to be [2] " + storedHoldings.encodePrettily() );

    JsonObject storedItems = getFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);

    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
            "After upsert the total number of items expected to be [3] " + storedHoldings.encodePrettily() );

  }

  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void testDeletionOfHoldingsAndItemsByHrid(TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));

    JsonObject upsertResponseJson = inventoryRecordSetPUT(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet);

    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    // Leave out one holdings record
    inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));

    upsertResponseJson =  inventoryRecordSetPUT(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet);

    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "DELETED" , "COMPLETED"), 1,
            "After upsert with one holdings record removed from set, metrics should report [1] holdings record successfully deleted " + upsertResponseJson.encodePrettily());

    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "DELETED" , "COMPLETED"), 2,
            "After upsert with one holdings record removed from set, metrics should report [2] items successfully deleted " + upsertResponseJson.encodePrettily());

    JsonObject holdingsAfterUpsertJson = getFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");

    testContext.assertEquals(holdingsAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert with one holdings record removed from set, number of holdings records left for the Instance expected to be [1] " + holdingsAfterUpsertJson.encodePrettily());

    JsonObject itemsAfterUpsertJson = getFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);

    testContext.assertEquals(itemsAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert with one holdings record removed from set, the total number of item records expected to be [1] " + itemsAfterUpsertJson.encodePrettily() );
  }


  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void testRecordSetWithParentInstanceRelation(TestContext testContext) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;

    // CREATE PARENT TO-BE
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson());

    // CREATE CHILD
    String childHrid = "2";
    JsonObject childRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).getJson())
            .put("instanceRelations", new JsonObject()
              .put("parentInstances", new JsonArray()
                .add(new InputInstanceRelationship().setInstanceIdentifierHrid(instanceHrid).getJson())));

    JsonObject upsertResponseJson = inventoryRecordSetPUT(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet);

    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    JsonObject childResponseJson = inventoryRecordSetPUT(MainVerticle.INVENTORY_UPSERT_HRID_PATH, childRecordSet);

    testContext.assertEquals(getMetric(childResponseJson, "INSTANCE_RELATIONSHIP", "CREATED" , "COMPLETED"), 1,
            "After upsert of new Instance with parent relation, metrics should report [1] instance relationship successfully created " + childResponseJson.encodePrettily());

    JsonObject relationshipsAfterUpsertJson = getFromStorage(FakeInventoryStorage.INSTANCE_RELATIONSHIP_STORAGE_PATH, null);

    testContext.assertEquals(relationshipsAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert of new Instance with parent relation, the total number of relationship records expected to be [1] " + relationshipsAfterUpsertJson.encodePrettily() );
  }

  @After
  public void tearDown(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      async.complete();
    }));
  }

  private JsonObject inventoryRecordSetPUT (String apiPath, JsonObject inventoryRecordSet) {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    Response response = RestAssured.given()
            .body(inventoryRecordSet.toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(apiPath)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();
    return new JsonObject(response.getBody().asString());
  }

  private JsonObject getFromStorage (String apiPath, String query) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response response =
            RestAssured.given()
                    .get(apiPath + (query == null ? "" : "?query=" + RecordStorage.encode(query)))
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    return new JsonObject(response.getBody().asString());
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
