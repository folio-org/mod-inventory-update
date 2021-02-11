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

import java.util.Arrays;


@RunWith(VertxUnitRunner.class)
public class InventoryUpdateTestSuite {

  Vertx vertx;
  private static final int PORT_INVENTORY_UPDATE = 9031;
  private static final Header OKAPI_URL_HEADER = new Header("X-Okapi-Url", "http://localhost:"
          + FakeInventoryStorage.PORT_INVENTORY_STORAGE);

  private FakeInventoryStorage fakeInventoryStorage;
  public static final String LOCATION_ID_1 = "LOC1";
  public static final String INSTITUTION_ID_1 = "INST1";
  public static final String LOCATION_ID_2 = "LOC2";
  public static final String INSTITUTION_ID_2 = "INST2";

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
              createReferenceRecords();
            });
  }

  public void createReferenceRecords () {
    fakeInventoryStorage.locationStorage.insert(
            new InputLocation().setId(LOCATION_ID_1).setInstitutionId(INSTITUTION_ID_1));
    fakeInventoryStorage.locationStorage.insert(
            new InputLocation().setId(LOCATION_ID_2).setInstitutionId(INSTITUTION_ID_2));

  }
  public void createInitialInstanceWithMatchKey() {
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
    createInitialInstanceWithMatchKey();
    new StorageValidatorLocations().validateStorage(testContext);
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
  public void testPutNewInstanceWillCreateNewInstance (TestContext testContext) {
    createInitialInstanceWithMatchKey();
    InputInstance instance = new InputInstance()
            .setTitle("New title")
            .setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
            "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    putToInstanceMatch(instance.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey 'new_title' after PUT expected: 1" );

  }

  /**
   * Tests API /instance-storage-match/instances
   * @param testContext
   */
  @Test
  public void testPutExistingInstanceWillUpdateExistingInstance (TestContext testContext) {
    createInitialInstanceWithMatchKey();
    InputInstance instance = new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH,"matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey 'initial instance' before PUT expected: 1" );
    String instanceTypeIdBefore = instancesBeforePutJson.getJsonArray("instances")
            .getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdBefore,"123",
            "Expected instanceTypeId to be '123' before PUT");

    putToInstanceMatch(instance.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH,"matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey 'initial instance' after PUT expected: 1" );
    String instanceTypeIdAfter = instancesAfterPutJson.getJsonArray("instances")
            .getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdAfter,"12345","Expected instanceTypeId to be '12345' after PUT");
  }

  /**
   * Tests API /shared-inventory-upsert-matchkey
   * @param testContext
   */
  @Test
  public void upsertByMatchKeyWillCreateNewInstance (TestContext testContext) {
    createInitialInstanceWithMatchKey();
    InputInstance instance = new InputInstance()
            .setTitle("New title")
            .setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    InventoryRecordSet recordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
            "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    upsertByMatchKey(recordSet.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey 'new_title' after PUT expected: 1" );

  }

  /**
   * Tests API /shared-inventory-upsert-matchkey
   * @param testContext
   */
  @Test
  public void upsertByMatchKeyWillUpdateExistingInstance (TestContext testContext) {
    createInitialInstanceWithMatchKey();
    InputInstance instance = new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    InventoryRecordSet inventoryRecordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH,"matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey 'initial instance' before PUT expected: 1" );
    String instanceTypeIdBefore = instancesBeforePutJson.getJsonArray("instances")
            .getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdBefore,"123",
            "Expected instanceTypeId to be '123' before PUT");

    upsertByMatchKey(inventoryRecordSet.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH,"matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey 'initial instance' after PUT expected: 1" );
    String instanceTypeIdAfter = instancesAfterPutJson.getJsonArray("instances")
            .getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdAfter,"12345","Expected instanceTypeId to be '12345' after PUT");

  }

  /**
   * Tests API /shared-inventory-upsert-matchkey
   * @param testContext
   */
  @Test
  public void upsertByMatchKeyWillCreateHoldingsAndItems(TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "CREATED" , "COMPLETED"), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "CREATED" , "COMPLETED"), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());
    JsonObject storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
            "After upsert the total number of items should be [3] " + storedHoldings.encodePrettily() );

  }

  /**
   * Tests API /shared-inventory-upsert-matchkey
   * @param testContext
   */
  @Test
  public void upsertByMatchKeyWillUpdateHoldingsAndItems (TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "CREATED" , "COMPLETED"), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "CREATED" , "COMPLETED"), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    upsertResponseJson = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("updated").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("updated").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("updated").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "DELETED" , "COMPLETED"), 2,
            "Upsert metrics response should report [2] holdings records successfully deleted " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "CREATED" , "COMPLETED"), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "DELETED" , "COMPLETED"), 3,
            "Upsert metrics response should report [3] items successfully deleted " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "CREATED" , "COMPLETED"), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH,null).getJsonArray("items").stream().forEach(item -> {
      testContext.assertEquals(((JsonObject)item).getString("barcode"), "updated",
              "The barcode of all items should be updated to 'updated' after upsert of existing record set with holdings and items");
    });

  }

  @Test
  public void deleteSharedInstanceWillDeleteSelectHoldingsItems (TestContext testContext) {

    final String identifierTypeId1 = "iti-001";
    final String identifierValue1 = "111";
    final String identifierTypeId2 = "iti-002";
    final String identifierValue2 = "222";

    JsonObject upsertResponseJson1 = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Shared InputInstance")
                            .setInstanceTypeId("12345")
                            .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId",identifierTypeId1).put("value",identifierValue1))).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-001").getJson())
                                    .add(new InputItem().setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-003").getJson())))));

    String instanceId = upsertResponseJson1.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson1, "HOLDINGS_RECORD", "CREATED" , "COMPLETED"), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson1.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson1, "ITEM", "CREATED" , "COMPLETED"), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson1.encodePrettily());

    JsonObject storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
            "After upsert the total number of items should be [3] " + storedItems.encodePrettily() );

    JsonObject instanceFromStorage = getRecordFromStorageById(FakeInventoryStorage.INSTANCE_STORAGE_PATH, instanceId);
    testContext.assertEquals(instanceFromStorage.getJsonArray("identifiers").size(),1,
            "After first upsert of Shared InputInstance there should be [1] identifier on the instance " + instanceFromStorage.encodePrettily());

    JsonObject upsertResponseJson2 = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Shared InputInstance")
                            .setInstanceTypeId("12345")
                            .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId",identifierTypeId2).put("value",identifierValue2))).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_2).setCallNumber("test-cn-3").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-004").getJson())
                                    .add(new InputItem().setBarcode("BC-005").getJson())))
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_2).setCallNumber("test-cn-4").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-006").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson2, "HOLDINGS_RECORD", "CREATED" , "COMPLETED"), 2,
            "Metrics after second upsert should report additional [2] holdings records successfully created " + upsertResponseJson2.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson2, "ITEM", "CREATED" , "COMPLETED"), 3,
            "Metrics after second upsert should report additional [3] items successfully created " + upsertResponseJson2.encodePrettily());

    storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 4,
            "After second upsert there should be [4] holdings records for instance " + instanceId + ": " + storedHoldings.encodePrettily() );
    storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 6,
            "After second upsert there should be [6] items " + storedItems.encodePrettily() );

    instanceFromStorage = getRecordFromStorageById(FakeInventoryStorage.INSTANCE_STORAGE_PATH, instanceId);
    testContext.assertEquals(instanceFromStorage.getJsonArray("identifiers").size(),2,
            "After second upsert of Shared InputInstance there should be [2] identifiers on the instance " + instanceFromStorage.encodePrettily());

    JsonObject deleteSignal = new JsonObject()
            .put("institutionId", INSTITUTION_ID_1)
            .put("oaiIdentifier","oai:"+identifierValue1)
            .put("identifierTypeId", identifierTypeId1);

    JsonObject deleteResponse = delete(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH,deleteSignal);
    testContext.assertEquals(getMetric(deleteResponse, "HOLDINGS_RECORD", "DELETED" , "COMPLETED"), 2,
            "Upsert metrics response should report [2] holdings records successfully deleted " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, "ITEM", "DELETED" , "COMPLETED"), 3,
            "Delete metrics response should report [3] items successfully deleted " + deleteResponse.encodePrettily());

    storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After delete the number of holdings records left for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
            "After delete the total number of items left should be [3] " + storedItems.encodePrettily() );

    instanceFromStorage = getRecordFromStorageById(FakeInventoryStorage.INSTANCE_STORAGE_PATH, instanceId);
    testContext.assertEquals(instanceFromStorage.getJsonArray("identifiers").size(), 1,
            "After delete request to Shared InputInstance there should be [1] identifier left on the instance " + instanceFromStorage.encodePrettily());

  }


  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void testUpsertByHridWillCreateNewInstance(TestContext testContext) {
    createInitialInstanceWithHrid1();
    InputInstance instance = new InputInstance().setTitle("New title").setInstanceTypeId("12345").setHrid("2");
    InventoryRecordSet inventoryRecordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "hrid==\"" + instance.getHrid() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
            "Before upserting with new Instance, the number of Instances with that HRID should be [0]" );

    upsertByHrid(inventoryRecordSet.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH,"hrid==\"" + instance.getHrid() + "\"");
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "After upserting with new Instance, the number of Instances with that HRID should be [1]" );
  }

  @Test
  public void upsertByHridWillGraciouslyFailWithMissingInstanceTitle (TestContext testContext) {
    //TODO
  }



  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void upsertByHridWillUpdateExistingInstance (TestContext testContext) {
    createInitialInstanceWithHrid1();
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject();
    inventoryRecordSet.put("instance", new InputInstance()
            .setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson());

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "hrid==\"" + instanceHrid + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
            "Before upsert of existing Instance, the number of Instances with that HRID should be [1]" );
    String instanceTypeIdBefore = instancesBeforePutJson
            .getJsonArray("instances").getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdBefore,"123",
            "Before upsert of existing Instance, the instanceTypeId should be [123]");

    upsertByHrid(inventoryRecordSet);

    JsonObject instancesAfterUpsertJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "hrid==\"" + instanceHrid + "\"");
    testContext.assertEquals(instancesAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert of existing Instance, number of Instances with that HRID should still be [1]" + instancesAfterUpsertJson.encodePrettily() );
    JsonObject instanceResponse = instancesAfterUpsertJson.getJsonArray("instances").getJsonObject(0);
    String instanceTypeIdAfter = instanceResponse.getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdAfter,"12345",
            "After upsert of existing Instance, the instanceTypeId should have changed to [12345]");
  }

  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void upsertByHridWillCreateHoldingsAndItems(TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
               new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
              .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                      .put("items", new JsonArray()
                        .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                        .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
              .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                      .put("items", new JsonArray()
                        .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "CREATED" , "COMPLETED"), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "CREATED" , "COMPLETED"), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());
    JsonObject storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
            "After upsert the total number of items should be [3] " + storedHoldings.encodePrettily() );

  }

  @Test
  public void upsertByHridWillUpdateHoldingsAndItems (TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "CREATED" , "COMPLETED"), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "CREATED" , "COMPLETED"), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("updated").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("updated").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("updated").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "UPDATED" , "COMPLETED"), 2,
            "Upsert metrics response should report [2] holdings records successfully updated " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "UPDATED" , "COMPLETED"), 3,
            "Upsert metrics response should report [3] items successfully updated " + upsertResponseJson.encodePrettily());

    getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH,null).getJsonArray("items").stream().forEach(item -> {
      testContext.assertEquals(((JsonObject)item).getString("barcode"), "updated",
              "The barcode of all items should be updated to 'updated' after upsert of existing record set with holdings and items");
    });

  }

  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void upsertByHridWillDeleteSelectHoldingsAndItems(TestContext testContext) {
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));

    JsonObject upsertResponseJson = upsertByHrid(inventoryRecordSet);
    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    // Leave out one holdings record
    inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));

    upsertResponseJson =  upsertByHrid(inventoryRecordSet);

    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "DELETED" , "COMPLETED"), 1,
            "After upsert with one holdings record removed from set, metrics should report [1] holdings record successfully deleted " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "DELETED" , "COMPLETED"), 2,
            "After upsert with one holdings record removed from set, metrics should report [2] items successfully deleted " + upsertResponseJson.encodePrettily());
    JsonObject holdingsAfterUpsertJson = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(holdingsAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert with one holdings record removed from set, number of holdings records left for the Instance should be [1] " + holdingsAfterUpsertJson.encodePrettily());
    JsonObject itemsAfterUpsertJson = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(itemsAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert with one holdings record removed from set, the total number of item records should be [1] " + itemsAfterUpsertJson.encodePrettily() );
  }

  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void upsertByHridWillCreateParentAndChildRelations(TestContext testContext) {

    String instanceHrid = "1";
    String childHrid = "2";
    String grandParentHrid = "3";

    upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson()));

    JsonObject childResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(instanceHrid).getJson()))));

    testContext.assertEquals(getMetric(childResponseJson, "INSTANCE_RELATIONSHIP", "CREATED" , "COMPLETED"), 1,
            "After upsert of new Instance with parent relation, metrics should report [1] instance relationship successfully created " + childResponseJson.encodePrettily());
    JsonObject relationshipsAfterUpsertJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_RELATIONSHIP_STORAGE_PATH, null);
    testContext.assertEquals(relationshipsAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert of new Instance with parent relation, the total number of relationship records should be [1] " + relationshipsAfterUpsertJson.encodePrettily() );

    JsonObject grandParentResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(grandParentHrid).getJson())
            .put("instanceRelations", new JsonObject()
                    .put("childInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(instanceHrid).getJson()))));

    testContext.assertEquals(getMetric(grandParentResponseJson, "INSTANCE_RELATIONSHIP", "CREATED" , "COMPLETED"), 1,
            "After upsert of new Instance with child relation, metrics should report [1] instance relationship successfully created " + grandParentResponseJson.encodePrettily());

    JsonObject relationshipsAfterGrandParent = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_RELATIONSHIP_STORAGE_PATH, null);

    testContext.assertEquals(relationshipsAfterGrandParent.getInteger("totalRecords"), 2,
            "After upsert of Instance with parent and Instance with child relation, the total number of relationship records should be [2] " + relationshipsAfterGrandParent.encodePrettily() );

  }

  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void upsertsByHridWillNotDeleteThenWillDeleteParentInstanceRelation (TestContext testContext) {

    // PARENT INSTANCE TO-BE
    String instanceHrid = "1";
    upsertByHrid(
            new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson()));
    // CHILD INSTANCE
    String childHrid = "2";
    JsonObject childResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(instanceHrid).getJson()))));

    testContext.assertEquals(getMetric(childResponseJson, "INSTANCE_RELATIONSHIP", "CREATED" , "COMPLETED"), 1,
      "After upsert of Instance with parent relation, metrics should report [1] instance relationship successfully created " + childResponseJson.encodePrettily());

    // POST child Instance again with no parent list
    childResponseJson = upsertByHrid(new JsonObject()
       .put("instance",
              new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).getJson())
       .put("instanceRelations", new JsonObject()));
    testContext.assertNull(childResponseJson.getJsonObject("metrics").getJsonObject("INSTANCE_RELATIONSHIP"),
    "After upsert with no parent list, metrics should not report any instance relations updates " + childResponseJson.encodePrettily());

    // POST child Instance again with empty parent list.
    childResponseJson = upsertByHrid(new JsonObject()
      .put("instance",
          new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).getJson())
      .put("instanceRelations", new JsonObject()
             .put("parentInstances", new JsonArray())));

    testContext.assertEquals(getMetric(childResponseJson, "INSTANCE_RELATIONSHIP", "DELETED", "COMPLETED"), 1,
            "After upsert with empty parent list, metrics should report [1] instance relationship successfully deleted " + childResponseJson.encodePrettily());

  }


  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void upsertsByHridWillDeleteRemovedRelations (TestContext testContext) {

    // PARENT INSTANCE 1
    String parent1Hrid = "1";
    String parent2Hrid = "2";
    String child1Hrid = "3";
    String child2Hrid = "4";
    String preceding1Hrid = "5";
    String preceding2Hrid = "6";
    String succeeding1Hrid = "7";
    String succeeding2Hrid = "8";
    for (String hrid : Arrays.asList(parent1Hrid, parent2Hrid, child1Hrid, child2Hrid, preceding1Hrid, preceding2Hrid, succeeding1Hrid, succeeding2Hrid)) {
      upsertByHrid(new JsonObject().put("instance",
              new InputInstance().setTitle("InputInstance "+hrid).setInstanceTypeId("12345").setHrid(hrid).getJson()));
    }

    JsonObject firstResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("InputInstance with 8 relations").setInstanceTypeId("12345").setHrid("MAIN-INSTANCE").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                      .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parent1Hrid).setInstanceRelationshipTypeId("multipart").getJson())
                      .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parent2Hrid).setInstanceRelationshipTypeId("multipart").getJson()))
                    .put("childInstances", new JsonArray()
                      .add(new InputInstanceRelationship().setInstanceIdentifierHrid(child1Hrid).setInstanceRelationshipTypeId("multipart").getJson())
                      .add(new InputInstanceRelationship().setInstanceIdentifierHrid(child2Hrid).setInstanceRelationshipTypeId("multipart").getJson()))
                    .put("precedingTitles", new JsonArray()
                      .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid(preceding1Hrid).getJson())
                      .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid(preceding2Hrid).getJson()))
                    .put("succeedingTitles", new JsonArray()
                      .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid(succeeding1Hrid).getJson())
                      .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid(succeeding2Hrid).getJson()))));

    testContext.assertEquals(getMetric(firstResponseJson, "INSTANCE_RELATIONSHIP", "CREATED" , "COMPLETED"), 4,
            "After upsert of Instance with multiple relations, metrics should report [4] instance relationship successfully created " + firstResponseJson.encodePrettily());

    testContext.assertEquals(getMetric(firstResponseJson, "INSTANCE_TITLE_SUCCESSION", "CREATED" , "COMPLETED"), 4,
            "After upsert of Instance with multiple relations, metrics should report [4] instance title successions successfully created " + firstResponseJson.encodePrettily());

    JsonObject secondResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("InputInstance with 8 relations").setInstanceTypeId("12345").setHrid("MAIN-INSTANCE").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parent2Hrid).setInstanceRelationshipTypeId("multipart").getJson()))
                    .put("childInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(child2Hrid).setInstanceRelationshipTypeId("multipart").getJson()))
                    .put("precedingTitles", new JsonArray()
                            .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid(preceding2Hrid).getJson()))
                    .put("succeedingTitles", new JsonArray()
                            .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid(succeeding2Hrid).getJson()))));

    testContext.assertEquals(getMetric(secondResponseJson, "INSTANCE_RELATIONSHIP", "DELETED" , "COMPLETED"), 2,
            "After upsert of Instance with some relations removed, metrics should report [2] instance relationship successfully deleted " + firstResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(secondResponseJson, "INSTANCE_TITLE_SUCCESSION", "DELETED" , "COMPLETED"), 2,
            "After upsert of Instance with some relations removed, metrics should report [2] instance title successions successfully deleted " + firstResponseJson.encodePrettily());


  }


  /**
   * Tests API /inventory-upsert-hrid
   * @param testContext
   */
  @Test
  public void upsertsByHridWillNotDeleteThenWillDeleteChildInstanceRelation (TestContext testContext) {

    // CHILD INSTANCE TO-BE
    String instanceHrid = "1";
    upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson()));
    // PARENT INSTANCE
    String parentHrid = "2";
    JsonObject parentResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(parentHrid).getJson())
            .put("instanceRelations", new JsonObject()
                    .put("childInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(instanceHrid).getJson()))));

    testContext.assertEquals(getMetric(parentResponseJson, "INSTANCE_RELATIONSHIP", "CREATED" , "COMPLETED"), 1,
            "After upsert of Instance with child relation, metrics should report [1] instance relationship successfully created " + parentResponseJson.encodePrettily());

    // POST child Instance again with no parent list
    parentResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(parentHrid).getJson())
            .put("instanceRelations", new JsonObject()));
    testContext.assertNull(parentResponseJson.getJsonObject("metrics").getJsonObject("INSTANCE_RELATIONSHIP"),
            "After upsert with no child list, metrics should not report any instance relations updates " + parentResponseJson.encodePrettily());

    // POST child Instance again with empty parent list.
    parentResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(parentHrid).getJson())
            .put("instanceRelations", new JsonObject()
                    .put("childInstances", new JsonArray())));

    testContext.assertEquals(getMetric(parentResponseJson, "INSTANCE_RELATIONSHIP", "DELETED", "COMPLETED"), 1,
            "After upsert with empty child list, metrics should report [1] instance relationship successfully deleted " + parentResponseJson.encodePrettily());

  }

  @Test
  public void upsertByHridWillCreatePrecedingAndSucceedingTitleRelations (TestContext testContext) {

    upsertByHrid(
      new JsonObject()
              .put("instance",
                    new InputInstance().setTitle("A title").setInstanceTypeId("123").setHrid("002").getJson()));

    JsonObject upsertResponseJson2 = upsertByHrid(
      new JsonObject()
              .put("instance",
                      new InputInstance().setTitle("A preceding title").setInstanceTypeId("123").setHrid("001").getJson())
              .put("instanceRelations", new JsonObject()
               .put("succeedingTitles", new JsonArray()
                .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid("002").getJson()))));

    testContext.assertEquals(getMetric(upsertResponseJson2, "INSTANCE_TITLE_SUCCESSION", "CREATED", "COMPLETED"), 1,
            "After upsert of preceding title, metrics should report [1] instance title successions successfully created " + upsertResponseJson2.encodePrettily());

    JsonObject upsertResponseJson3 = upsertByHrid(
      new JsonObject()
              .put("instance",
                      new InputInstance().setTitle("A succeeding title").setInstanceTypeId("123").setHrid("003").getJson())
              .put("instanceRelations", new JsonObject()
                .put("precedingTitles", new JsonArray()
                  .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid("002").getJson()))));

    testContext.assertEquals(getMetric(upsertResponseJson3, "INSTANCE_TITLE_SUCCESSION", "CREATED", "COMPLETED"), 1,
            "After upsert of succeeding title, metrics should report [1] instance title successions successfully created " + upsertResponseJson3.encodePrettily());

    JsonObject titleSuccessions = getRecordsFromStorage(FakeInventoryStorage.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH,null);
    testContext.assertEquals(titleSuccessions.getInteger("totalRecords"), 2,
            "After two upserts with title successions, the total number of title successions should be [2] " + titleSuccessions.encodePrettily() );
  }

  @Test
  public void upsertsByHridWillNotDeleteThenWillDeleteSucceeding (TestContext testContext) {

    upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A title").setInstanceTypeId("123").setHrid("002").getJson()));

    JsonObject upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A preceding title").setInstanceTypeId("123").setHrid("001").getJson())
                    .put("instanceRelations", new JsonObject()
                            .put("succeedingTitles", new JsonArray()
                                    .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid("002").getJson()))));

    testContext.assertEquals(getMetric(upsertResponseJson2, "INSTANCE_TITLE_SUCCESSION", "CREATED", "COMPLETED"), 1,
            "After upsert of preceding title, metrics should report [1] instance title successions successfully created " + upsertResponseJson2.encodePrettily());

    // POST preceding title again with no succeeding titles list
    upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A preceding title").setInstanceTypeId("123").setHrid("001").getJson())
                    .put("instanceRelations", new JsonObject()));
    testContext.assertNull(upsertResponseJson2.getJsonObject("metrics").getJsonObject("INSTANCE_TITLE_SUCCESSION"),
            "After upsert with no succeeding titles list, metrics should not report any instance title succession updates " + upsertResponseJson2.encodePrettily());

    // POST preceding title again with empty succeeding titles list.
    upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A preceding title").setInstanceTypeId("123").setHrid("001").getJson())
                    .put("instanceRelations", new JsonObject()
                            .put("succeedingTitles", new JsonArray())));

    testContext.assertEquals(getMetric(upsertResponseJson2, "INSTANCE_TITLE_SUCCESSION", "DELETED", "COMPLETED"), 1,
            "After upsert with empty succeedingTitles list, metrics should report [1] instance title successions successfully deleted " + upsertResponseJson2.encodePrettily());

    JsonObject titleSuccessions = getRecordsFromStorage(FakeInventoryStorage.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH,null);
    testContext.assertEquals(titleSuccessions.getInteger("totalRecords"), 0,
            "After two upserts -- with and without title successions -- the number of title successions should be [0] " + titleSuccessions.encodePrettily() );
  }

  @Test
  public void upsertsByHridWillNotDeleteThenWillDeletePreceding (TestContext testContext) {

    upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A title").setInstanceTypeId("123").setHrid("001").getJson()));

    JsonObject upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A succeeding title").setInstanceTypeId("123").setHrid("002").getJson())
                    .put("instanceRelations", new JsonObject()
                            .put("precedingTitles", new JsonArray()
                                    .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid("001").getJson()))));

    testContext.assertEquals(getMetric(upsertResponseJson2, "INSTANCE_TITLE_SUCCESSION", "CREATED", "COMPLETED"), 1,
            "After upsert of succeeding title, metrics should report [1] instance title successions successfully created " + upsertResponseJson2.encodePrettily());

    // POST succeeding title again with no preceding titles list
    upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A succeeding title").setInstanceTypeId("123").setHrid("002").getJson())
                    .put("instanceRelations", new JsonObject()));
    testContext.assertNull(upsertResponseJson2.getJsonObject("metrics").getJsonObject("INSTANCE_TITLE_SUCCESSION"),
            "After upsert with no preceding titles list, metrics should not report any instance title succession updates " + upsertResponseJson2.encodePrettily());

    // POST succeeding title again with empty preceding titles list.
    upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A preceding title").setInstanceTypeId("123").setHrid("002").getJson())
                    .put("instanceRelations", new JsonObject()
                            .put("precedingTitles", new JsonArray())));

    testContext.assertEquals(getMetric(upsertResponseJson2, "INSTANCE_TITLE_SUCCESSION", "DELETED", "COMPLETED"), 1,
            "After upsert with empty precedingTitles list, metrics should report [1] instance title successions successfully deleted " + upsertResponseJson2.encodePrettily());

    JsonObject titleSuccessions = getRecordsFromStorage(FakeInventoryStorage.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH,null);
    testContext.assertEquals(titleSuccessions.getInteger("totalRecords"), 0,
            "After two upserts -- with and without title successions -- the number of title successions should be [0] " + titleSuccessions.encodePrettily() );
  }

  @Test
  public void upsertByHridWillCreateProvisionalInstanceIfNeededForRelation (TestContext testContext) {
    String childHrid = "002";
    String parentHrid = "001";

    JsonObject childResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parentHrid)
                                    .setProvisionalInstance(
                                            new InputInstance()
                                                    .setTitle("Provisional Instance")
                                                    .setSource("MARC")
                                                    .setInstanceTypeId("12345").getJson()).getJson()))));

    testContext.assertEquals(getMetric(childResponseJson, "INSTANCE_RELATIONSHIP", "CREATED", "COMPLETED"), 1,
            "Upsert metrics response should report [1] instance relationship successfully created " + childResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(childResponseJson, "INSTANCE_RELATIONSHIP", "PROVISIONAL_INSTANCE", "COMPLETED"), 1,
            "Upsert metrics response should report [1] provisional instance successfully created " + childResponseJson.encodePrettily());
    JsonObject instancesAfterUpsertJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterUpsertJson.getInteger("totalRecords"), 2,
            "After upsert with provisional instance the total number of instances should be [2] " + instancesAfterUpsertJson.encodePrettily() );
  }

  @Test
  public void upsertByHridWillGraciouslyFailToCreateRelationWithoutProvisionalInstance (TestContext testContext) {
    String childHrid = "002";
    String parentHrid = "001";

    Response childResponse = upsertByHrid(422, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parentHrid).getJson()))));

    JsonObject responseJson = new JsonObject(childResponse.getBody().asString());
    testContext.assertEquals(getMetric(responseJson, "INSTANCE_RELATIONSHIP", "CREATED", "FAILED"), 1,
            "Upsert metrics response should report [1] relation creation failure due to missing provisional instance  " + responseJson.encodePrettily());

    childResponse = upsertByHrid(422, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parentHrid)
                                    .setProvisionalInstance(
                                            new InputInstance()
                                                    .setSource("MARC")
                                                    .setInstanceTypeId("12345").getJson()).getJson()))));

    responseJson = new JsonObject(childResponse.getBody().asString());
    testContext.assertEquals(getMetric(responseJson, "INSTANCE_RELATIONSHIP", "CREATED", "FAILED"), 1,
            "Upsert metrics response should report [1] relation creation failure due to missing mandatory properties in provisional instance  " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, "INSTANCE_RELATIONSHIP", "PROVISIONAL_INSTANCE", "FAILED"), 1,
            "Upsert metrics response should report [1] provisional Instance creation failure due to missing mandatory properties in provisional instance  " + responseJson.encodePrettily());

  }

  @Test
  public void deleteByHridWillDeleteInstanceRelationsHoldingsItems (TestContext testContext) {
     // Create succeeding title
     upsertByHrid(
             new JsonObject()
                     .put("instance",
                             new InputInstance().setTitle("A title").setInstanceTypeId("123").setHrid("001").getJson()));

     String instanceHrid = "002";
     JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
             .put("instance",
                     new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
             .put("holdingsRecords", new JsonArray()
                     .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                             .put("items", new JsonArray()
                                     .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                     .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                     .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                             .put("items", new JsonArray()
                                     .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))))
            .put("instanceRelations", new JsonObject()
                     .put("succeedingTitles", new JsonArray()
                             .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid("001").getJson()))));

     String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

     testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "CREATED" , "COMPLETED"), 2,
             "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
     testContext.assertEquals(getMetric(upsertResponseJson, "ITEM", "CREATED" , "COMPLETED"), 3,
             "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());
     testContext.assertEquals(getMetric(upsertResponseJson, "INSTANCE_TITLE_SUCCESSION", "CREATED", "COMPLETED"), 1,
             "Upsert metrics response should report [1] succeeding title relations successfully created " + upsertResponseJson.encodePrettily());

     JsonObject storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
     testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
             "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
     JsonObject storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
     testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
             "After upsert the total number of items should be [3] " + storedItems.encodePrettily() );
     JsonObject storedRelations = getRecordsFromStorage(FakeInventoryStorage.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH, null);
     testContext.assertEquals(storedRelations.getInteger("totalRecords"), 1,
             "After upsert the total number of relations should be [1] " + storedRelations.encodePrettily() );

     JsonObject deleteSignal = new JsonObject().put("hrid",instanceHrid);

     JsonObject deleteResponse = delete(MainVerticle.INVENTORY_UPSERT_HRID_PATH,deleteSignal);
     testContext.assertEquals(getMetric(deleteResponse, "HOLDINGS_RECORD", "DELETED" , "COMPLETED"), 2,
             "Upsert metrics response should report [2] holdings records successfully deleted " + deleteResponse.encodePrettily());
     testContext.assertEquals(getMetric(deleteResponse, "ITEM", "DELETED" , "COMPLETED"), 3,
             "Delete metrics response should report [3] items successfully deleted " + deleteResponse.encodePrettily());
     testContext.assertEquals(getMetric(deleteResponse, "INSTANCE_TITLE_SUCCESSION", "DELETED" , "COMPLETED"), 1,
             "Delete metrics response should report [1] relation successfully deleted " + deleteResponse.encodePrettily());

     storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
     testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 0,
             "After delete the number of holdings records for instance " + instanceId + " should be [0] " + storedHoldings.encodePrettily() );
     storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
     testContext.assertEquals(storedItems.getInteger("totalRecords"), 0,
             "After delete the total number of items should be [3] " + storedItems.encodePrettily() );
     storedRelations = getRecordsFromStorage(FakeInventoryStorage.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH, null);
     testContext.assertEquals(storedRelations.getInteger("totalRecords"), 0,
             "After delete the total number of relations should be [0] " + storedRelations.encodePrettily() );

   }

  @Test
  public void upsertByHridWillMoveHoldingsAndItems (TestContext testContext) {
    String instanceHrid1 = "1";
    JsonObject inventoryRecordSet1 = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid1).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));

    JsonObject firstResponse = upsertByHrid(inventoryRecordSet1);
    String instanceId1 = firstResponse.getJsonObject("instance").getString("id");
    JsonObject storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId1 + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After upsert the number of holdings records for instance " + instanceId1 + " should be [2] " + storedHoldings.encodePrettily() );

    String instanceHrid2 = "2";
    JsonObject inventoryRecordSet2 = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("InputInstance 2").setInstanceTypeId("12345").setHrid(instanceHrid2).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));

    upsertByHrid(inventoryRecordSet2);

    storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId1 + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 0,
            "After move of holdings the number of holdings records for instance " + instanceId1 + " should be [0] " + storedHoldings.encodePrettily() );

    storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "hrid==\"HOL-001\" or hrid==\"HOL-002\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After move of holdings they should still exist, count should be [2] " + storedHoldings.encodePrettily() );

    JsonObject inventoryRecordSet3 = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid1).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-003").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-3").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));

    JsonObject thirdResponse = upsertByHrid(inventoryRecordSet3);
    testContext.assertEquals(getMetric(thirdResponse, "HOLDINGS_RECORD", "CREATED" , "COMPLETED"), 1,
            "Third update should report [1] holdings record successfully created  " + thirdResponse.encodePrettily());
    testContext.assertEquals(getMetric(thirdResponse, "ITEM", "UPDATED" , "COMPLETED"), 1,
            "Third update should report [1] item successfully updated (moved)  " + thirdResponse.encodePrettily());

    JsonObject storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
            "After two moves of holdings/items there should still be [3] items total in storage " + storedItems.encodePrettily() );


  }

  @Test
  public void upsertByHridWithMissingItemHridWillBeRejected (TestContext testContext) {
    String instanceHrid = "1";
    Response upsertResponse = upsertByHrid(422, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-003").getJson())))));

  }

  @Test
  public void upsertByHridWithMissingHoldingsHridWillBeRejected (TestContext testContext) {
    String instanceHrid = "1";
    Response upsertResponse = upsertByHrid(422, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

  }


  @Test
  public void upsertByHridWillHaveErrorsWithWrongHoldingsLocation (TestContext testContext) {
    String instanceHrid = "1";
    // fail if response status code is not 422
    Response upsertResponse = upsertByHrid(422, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId("BAD_LOCATION").setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));
    JsonObject upsertResponseJson = new JsonObject(upsertResponse.getBody().asString());
    testContext.assertTrue(upsertResponseJson.containsKey("errors"),
            "After upsert with holdings record with bad location id, the response should contain error reports");
    testContext.assertEquals(getMetric(upsertResponseJson, "HOLDINGS_RECORD", "CREATED" , "FAILED"), 1,
            "Upsert metrics response should report [1] holdings records update failure for wrong location ID " + upsertResponseJson.encodePrettily());
  }

  @Test
  public void upsertByHridWillReturnErrorResponseOnMissingInstanceInRequestBody (TestContext testContext) {
    upsertByHrid(400, new JsonObject().put("invalid", "No Instance here"));
  }

  @After
  public void tearDown(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      async.complete();
    }));
  }

  private JsonObject upsertByMatchKey(JsonObject inventoryRecordSet) {
    return putJsonObject(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH, inventoryRecordSet);
  }

  private JsonObject upsertByHrid (JsonObject inventoryRecordSet) {
    return putJsonObject(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet);
  }

  private Response upsertByHrid (int expectedStatusCode, JsonObject inventoryRecordSet) {
    return putJsonObject(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet, expectedStatusCode);
  }

  private JsonObject putToInstanceMatch (JsonObject instance) {
    return putJsonObject(MainVerticle.INSTANCE_MATCH_PATH, instance);
  }

  private Response putJsonObject(String apiPath, JsonObject requestJson, int expectedStatusCode) {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    return RestAssured.given()
            .body(requestJson.toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(apiPath)
            .then()
            .log().ifValidationFails()
            .statusCode(expectedStatusCode).extract().response();
  }

  private JsonObject putJsonObject(String apiPath, JsonObject requestJson) {
    return new JsonObject(putJsonObject(apiPath, requestJson, 200).getBody().asString());
  }

  private JsonObject delete(String apiPath, JsonObject requestJson) {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    Response response = RestAssured.given()
            .body(requestJson.toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .delete(apiPath)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();
    return new JsonObject(response.getBody().asString());
  }

  private JsonObject getRecordsFromStorage(String apiPath, String query) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response response =
            RestAssured.given()
                    .get(apiPath + (query == null ? "" : "?query=" + RecordStorage.encode(query)))
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    return new JsonObject(response.getBody().asString());
  }

  private JsonObject getRecordFromStorageById(String apiPath, String id) {
    RestAssured.port = FakeInventoryStorage.PORT_INVENTORY_STORAGE;
    Response response =
            RestAssured.given()
                    .get(apiPath + "/"+id)
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    return new JsonObject(response.getBody().asString());

  }


  private int getMetric (JsonObject upsertResponse, String entity, String transaction, String outcome) {
    if (upsertResponse.containsKey("metrics")
            && upsertResponse.getJsonObject("metrics").containsKey(entity)
            && upsertResponse.getJsonObject("metrics").getJsonObject(entity).containsKey(transaction)
            && upsertResponse.getJsonObject("metrics").getJsonObject(entity).getJsonObject(transaction). containsKey(outcome)) {
      return upsertResponse.getJsonObject("metrics").getJsonObject(entity).getJsonObject(transaction).getInteger(outcome);
    } else {
      return -1;
    }
  }

}
