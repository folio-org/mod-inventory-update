package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonArray;
import org.folio.inventoryupdate.MainVerticle;
import org.folio.inventoryupdate.MatchKey;
import org.folio.inventoryupdate.ProcessingInstructions;
import org.folio.inventoryupdate.UpdatePlanSharedInventory;
import org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage;
import org.folio.inventoryupdate.test.fakestorage.InputProcessingInstructions;
import org.folio.inventoryupdate.test.fakestorage.RecordStorage;
import org.folio.inventoryupdate.test.fakestorage.entitites.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import io.restassured.RestAssured;
import io.restassured.http.Header;
import io.restassured.response.Response;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import java.util.Arrays;
import static org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage.RESULT_SET_HOLDINGS_RECORDS;


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

  public static final String CREATE = org.folio.inventoryupdate.entities.InventoryRecord.Transaction.CREATE.name();
  public static final String UPDATE = org.folio.inventoryupdate.entities.InventoryRecord.Transaction.UPDATE.name();
  public static final String DELETE = org.folio.inventoryupdate.entities.InventoryRecord.Transaction.DELETE.name();

  public static final String COMPLETED = org.folio.inventoryupdate.entities.InventoryRecord.Outcome.COMPLETED.name();
  public static final String FAILED = org.folio.inventoryupdate.entities.InventoryRecord.Outcome.FAILED.name();
  public static final String SKIPPED = org.folio.inventoryupdate.entities.InventoryRecord.Outcome.SKIPPED.name();

  public static final String HOLDINGS_RECORD = org.folio.inventoryupdate.entities.InventoryRecord.Entity.HOLDINGS_RECORD.name();
  public static final String INSTANCE = org.folio.inventoryupdate.entities.InventoryRecord.Entity.INSTANCE.name();
  public static final String ITEM = org.folio.inventoryupdate.entities.InventoryRecord.Entity.ITEM.name();
  public static final String INSTANCE_TITLE_SUCCESSION = org.folio.inventoryupdate.entities.InventoryRecord.Entity.INSTANCE_TITLE_SUCCESSION.name();
  public static final String INSTANCE_RELATIONSHIP = org.folio.inventoryupdate.entities.InventoryRecord.Entity.INSTANCE_RELATIONSHIP.name();
  public static final String PROVISIONAL_INSTANCE = "PROVISIONAL_INSTANCE";

  private final Logger logger = io.vertx.core.impl.logging.LoggerFactory.getLogger("InventoryUpdateTestSuite");
  @Rule
  public final TestName name = new TestName();

  public InventoryUpdateTestSuite() {}

  @Before
  public void setUp(TestContext testContext) {
    logger.debug("setUp " + name.getMethodName());

    vertx = Vertx.vertx();

    // Register the testContext exception handler to catch assertThat
    vertx.exceptionHandler(testContext.exceptionHandler());

    deployService(testContext);
  }

  private void deployService(TestContext testContext) {
    System.setProperty("port", String.valueOf(PORT_INVENTORY_UPDATE));
    vertx.deployVerticle(MainVerticle.class.getName(), new DeploymentOptions())
    .onComplete(testContext.asyncAssertSuccess(x -> {
      fakeInventoryStorage = new FakeInventoryStorage(vertx, testContext);
      createReferenceRecords();
    }));
  }

  public void createReferenceRecords () {
    fakeInventoryStorage.locationStorage.insert(
            new InputLocation().setId(LOCATION_ID_1).setInstitutionId(INSTITUTION_ID_1));
    fakeInventoryStorage.locationStorage.insert(
            new InputLocation().setId(LOCATION_ID_2).setInstitutionId(INSTITUTION_ID_2));

  }
  public void createInitialInstanceWithMatchKey() {
    InputInstance instance = new InputInstance()
            .setInstanceTypeId("123")
            .setTitle("Initial InputInstance")
            .setHrid("1")
            .setSource("test");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKeyAsString(matchKey.getKey());
    fakeInventoryStorage.instanceStorage.insert(instance);
  }

  public void createInitialInstanceWithHrid1() {
    InputInstance instance = new InputInstance().setInstanceTypeId("123").setTitle("Initial InputInstance").setHrid("1").setSource("test");
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
    new StorageValidatorQueries().validateQueries(testContext);
  }

  @Test
  public void testHealthCheck (TestContext testContext) {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    RestAssured.given()
            .header(OKAPI_URL_HEADER)
            .get("/admin/health")
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();
  }

  @Test
  public void upsertByMatchKeyWillCreateNewInstance (TestContext testContext) {
    createInitialInstanceWithMatchKey();
    InputInstance instance = new InputInstance()
            .setTitle("New title")
            .setInstanceTypeId("12345")
            .setSource("test");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKeyAsString(matchKey.getKey());
    InventoryRecordSet recordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
            "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    upsertByMatchKey(recordSet.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey 'new_title' after PUT expected: 1" );

  }

  @Test
  public void batchUpsertByMatchKeyWillCreateNewInstance (TestContext testContext) {
    createInitialInstanceWithMatchKey();
    InputInstance instance = new InputInstance()
            .setTitle("New title")
            .setInstanceTypeId("12345")
            .setSource("test");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKeyAsString(matchKey.getKey());
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets().addRecordSet(new InventoryRecordSet(instance));
    logger.info(batch.getJson().encodePrettily());

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
            "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    batchUpsertByMatchKey(batch.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey 'new_title' after PUT expected: 1" );

  }

  @Test
  public void batchUpsertByMatchKeyWillCreateXNewInstances (TestContext testContext) {
    createInitialInstanceWithMatchKey();
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    for (int i=0; i<5; i++) {
      InputInstance instance = new InputInstance()
              .setTitle("New title " + i)
              .setInstanceTypeId("12345")
              .setSource("test");
      MatchKey matchKey = new MatchKey(instance.getJson());
      instance.setMatchKeyAsString(matchKey.getKey());
      batch.addRecordSet(new InventoryRecordSet(instance));
    }
    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
            "Number of instance records for before PUT expected: 1" );
    batchUpsertByMatchKey(batch.getJson());
    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 6,
            "Number of instance records after PUT expected: 6" );

  }

  @Test
  public void batchUpsertByMatchKeyWillCreate200NewInstances (TestContext testContext) {
    createInitialInstanceWithMatchKey();
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    for (int i=0; i<200; i++) {
      InputInstance instance = new InputInstance()
              .setTitle("New title " + i)
              .setSource("test")
              .setInstanceTypeId("12345");
      MatchKey matchKey = new MatchKey(instance.getJson());
      instance.setMatchKeyAsString(matchKey.getKey());
      batch.addRecordSet(new InventoryRecordSet(instance));
    }
    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
            "Number of instance records for before PUT expected: 1" );
    batchUpsertByMatchKey(batch.getJson());
    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 201,
            "Number of instance records after PUT expected: 201" );

  }

  @Test
  public void batchUpsertByHridWillCreate200NewInstances (TestContext testContext) {
    createInitialInstanceWithHrid1();
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    for (int i=0; i<200; i++) {
      InputInstance instance = new InputInstance()
              .setTitle("New title " + i)
              .setHrid("in"+i)
              .setSource("test")
              .setInstanceTypeId("12345");
      MatchKey matchKey = new MatchKey(instance.getJson());
      instance.setMatchKeyAsString(matchKey.getKey());
      batch.addRecordSet(new InventoryRecordSet(instance));
    }
    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
            "Number of instance records for before PUT expected: 1" );
    batchUpsertByHrid(batch.getJson());
    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 201,
            "Number of instance records after PUT expected: 201" );
  }

  @Test
  public void batchByHridWithOneErrorWillCreate99NewInstances (TestContext testContext) {
    createInitialInstanceWithHrid1();
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    for (int i=0; i<100; i++) {
      InputInstance instance = new InputInstance()
              .setTitle("New title " + i)
              .setHrid("in"+i)
              .setInstanceTypeId("12345");
      if (i!=50) {
        instance.setSource("test");
      }
      batch.addRecordSet(new InventoryRecordSet(instance));
    }
    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
            "Number of instance records for before PUT expected: 1" );
    batchUpsertByHrid(207,batch.getJson());
    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 100,
            "Number of instance records after PUT expected: 200" );
  }

  @Test
  public void batchByHridWithOneMissingHridWillCreate99NewInstances (TestContext testContext) {
    createInitialInstanceWithHrid1();
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    for (int i=0; i<100; i++) {
      InputInstance instance = new InputInstance()
              .setTitle("New title " + i)
              .setSource("test")
              .setInstanceTypeId("12345");
      if (i!=50) {
        instance.setHrid("in"+i);
      }
      batch.addRecordSet(new InventoryRecordSet(instance));
    }
    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
            "Number of instance records for before PUT expected: 1" );
    batchUpsertByHrid(207,batch.getJson());
    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 100,
            "Number of instance records after PUT expected: 200" );
  }



  @Test
  public void upsertByMatchKeyWithMultipleMatchKeyPartsWillCreateNewInstance (TestContext testContext) {
    final String GOV_DOC_NUMBER_TYPE = "9075b5f8-7d97-49e1-a431-73fdd468d476";
    createInitialInstanceWithMatchKey();
    JsonObject matchKeyAsObject = new JsonObject();
    String longTitle = "A long title which should exceed 70 characters of length";
    matchKeyAsObject.put("title", longTitle);
    matchKeyAsObject.put("remainder-of-title", " - together with the remainder of title");
    matchKeyAsObject.put("medium", "[microform]");

    InputInstance instance = new InputInstance()
            .setTitle(longTitle)
            .setInstanceTypeId("12345")
            .setSource("test")
            .setDateOfPublication( "[2000]" )
            .setClassification( GOV_DOC_NUMBER_TYPE, "12345" )
            .setContributor( InputInstance.PERSONAL_NAME_TYPE, "Doe, John" )
            .setPhysicalDescription( "125 pages" )
            .setEdition("1st edition")
            .setMatchKeyAsObject( matchKeyAsObject );
    InventoryRecordSet recordSet = new InventoryRecordSet(instance);
    logger.info("Instance " + instance.getJson().encodePrettily());

    MatchKey matchKey = new MatchKey( instance.getJson() );
    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
            "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    upsertByMatchKey(recordSet.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "Number of instance records for query by matchKey " + matchKey.getKey() + " after PUT expected: 1" );

  }

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

  @Test
  public void upsertByMatchKeyWillCreateHoldingsAndItems(TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());
    JsonObject storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
            "After upsert the total number of items should be [3] " + storedHoldings.encodePrettily() );

  }

  @Test
  public void upsertByMatchKeyWillUpdateHoldingsAndItems (TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    upsertResponseJson = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("updated").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("updated").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("updated").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, DELETE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully deleted " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, DELETE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully deleted " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH,null).getJsonArray("items").stream().forEach(item -> {
      testContext.assertEquals(((JsonObject)item).getString("barcode"), "updated",
              "The barcode of all items should be updated to 'updated' after upsert of existing record set with holdings and items");
    });

  }

  @Test
  public void upsertsByMatchKeyWillCreateSharedInstanceFromTwoInstitutionsAndDeleteByOaiIdentifier (TestContext testContext) {

    final String identifierTypeId1 = "iti-001";
    final String identifierValue1 = "111";
    final String identifierTypeId2 = "iti-002";
    final String identifierValue2 = "222";

    JsonObject upsertResponseJson1 = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Shared InputInstance")
                            .setInstanceTypeId("12345")
                            .setSource("source")
                            .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId",identifierTypeId1).put("value",identifierValue1))).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-001").getJson())
                                    .add(new InputItem().setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-003").getJson()))))
            .put("processing", new JsonObject()
                    .put("localIdentifier",identifierValue1)));

    String instanceId = upsertResponseJson1.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson1, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson1.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson1, ITEM, CREATE , COMPLETED), 3,
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
                            .setSource("test")
                            .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId",identifierTypeId2).put("value",identifierValue2))).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_2).setCallNumber("test-cn-3").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-004").getJson())
                                    .add(new InputItem().setBarcode("BC-005").getJson())))
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_2).setCallNumber("test-cn-4").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-006").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson2, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Metrics after second upsert should report additional [2] holdings records successfully created " + upsertResponseJson2.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson2, ITEM, CREATE , COMPLETED), 3,
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
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully deleted " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , COMPLETED), 3,
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

  @Test
  public void upsertsByMatchKeyWillCreateSharedInstanceFromTwoInstitutionsAndDeleteByLocalIdentifier (TestContext testContext) {
    final String identifierTypeId1 = "iti-001";
    final String identifierValue1 = "111";
    final String identifierTypeId2 = "iti-002";
    final String identifierValue2 = "222";

    JsonObject upsertResponseJson1 = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Shared InputInstance")
                            .setInstanceTypeId("12345")
                            .setSource("test")
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

    testContext.assertEquals(getMetric(upsertResponseJson1, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson1.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson1, ITEM, CREATE , COMPLETED), 3,
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
                            .setSource("test")
                            .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId",identifierTypeId2).put("value",identifierValue2))).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_2).setCallNumber("test-cn-3").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-004").getJson())
                                    .add(new InputItem().setBarcode("BC-005").getJson())))
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_2).setCallNumber("test-cn-4").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-006").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson2, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Metrics after second upsert should report additional [2] holdings records successfully created " + upsertResponseJson2.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson2, ITEM, CREATE , COMPLETED), 3,
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
            .put("localIdentifier",identifierValue1)
            .put("identifierTypeId", identifierTypeId1);
    JsonObject deleteResponse = delete(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH,deleteSignal);
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully deleted " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , COMPLETED), 3,
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

  @Test
  public void upsertByShiftingMatchKeyWillCleanUpRecordsForPreviousMatchKey( TestContext testContext) {
    // institution 1, record 111
    final String identifierTypeId1 = "iti-001";
    final String identifierValue1 = "111";
    // institution 2, record 222
    final String identifierTypeId2 = "iti-002";
    final String identifierValue2 = "222";

    // record from institution 1
    JsonObject upsertResponseJson1 = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Shared InputInstance")
                            .setInstanceTypeId("12345")
                            .setSource("test")
                            .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId",identifierTypeId1).put("value",identifierValue1))).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-001").getJson())
                                    .add(new InputItem().setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-003").getJson()))))
            .put("processing", new JsonObject()
                    .put("identifierTypeId", identifierTypeId1)
                    .put("localIdentifier", identifierValue1)));

    String instanceId = upsertResponseJson1.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson1, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson1.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson1, ITEM, CREATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson1.encodePrettily());

    JsonObject storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
            "After upsert the total number of items should be [3] " + storedItems.encodePrettily() );

    JsonObject instanceFromStorage = getRecordFromStorageById(FakeInventoryStorage.INSTANCE_STORAGE_PATH, instanceId);
    testContext.assertEquals(instanceFromStorage.getJsonArray("identifiers").size(),1,
            "After first upsert of 'Shared InputInstance' there should be [1] identifier on the instance " + instanceFromStorage.encodePrettily());

    // matching record from institution 2
    JsonObject upsertResponseJson2 = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Shared InputInstance")
                            .setInstanceTypeId("12345")
                            .setSource("test")
                            .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId",identifierTypeId2).put("value",identifierValue2))).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_2).setCallNumber("test-cn-3").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-004").getJson())
                                    .add(new InputItem().setBarcode("BC-005").getJson())))
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_2).setCallNumber("test-cn-4").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-006").getJson()))))
            .put("processing", new JsonObject()
                    .put("identifierTypeId", identifierTypeId2)
                    .put("localIdentifier", identifierValue2)));

    testContext.assertEquals(getMetric(upsertResponseJson2, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Metrics after second upsert should report additional [2] holdings records successfully created " + upsertResponseJson2.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson2, ITEM, CREATE , COMPLETED), 3,
            "Metrics after second upsert should report additional [3] items successfully created " + upsertResponseJson2.encodePrettily());

    storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 4,
            "After second upsert there should be [4] holdings records for instance " + instanceId + ": " + storedHoldings.encodePrettily() );
    storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 6,
            "After second upsert there should be [6] items " + storedItems.encodePrettily() );

    instanceFromStorage = getRecordFromStorageById(FakeInventoryStorage.INSTANCE_STORAGE_PATH, instanceId);
    testContext.assertEquals(instanceFromStorage.getJsonArray("identifiers").size(),2,
            "After second upsert of 'Shared InputInstance' there should be [2] identifiers on the instance " + instanceFromStorage.encodePrettily());

    // update, record 111 from institution 1, with shifting match key
    JsonObject upsertResponseJsonForChangedMatchKey = upsertByMatchKey(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Shared Input Instance")
                            .setInstanceTypeId("12345")
                            .setSource("test")
                            .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId",identifierTypeId1).put("value",identifierValue1))).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-001").getJson())
                                    .add(new InputItem().setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-003").getJson()))))
            .put("processing", new JsonObject()
                    .put("identifierTypeId", identifierTypeId1)
                    .put("localIdentifier", identifierValue1)));

    String instanceIdSameRecordNewMatchKey = upsertResponseJsonForChangedMatchKey.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJsonForChangedMatchKey, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJsonForChangedMatchKey.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJsonForChangedMatchKey, ITEM, CREATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJsonForChangedMatchKey.encodePrettily());

    JsonObject storedHoldingsSameRecordNewInstance = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceIdSameRecordNewMatchKey + "\"");
    testContext.assertEquals(storedHoldingsSameRecordNewInstance.getInteger("totalRecords"), 2,
            "After third upsert, with 'Shared Input Instance', the number of holdings records for the new Instance " + instanceIdSameRecordNewMatchKey + " should be [2] " + storedHoldingsSameRecordNewInstance.encodePrettily() );
    JsonObject allStoredItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(allStoredItems.getInteger("totalRecords"), 6,
            "After third upsert, with 'Shared Input Instance', the total number of items should be [6] " + allStoredItems.encodePrettily() );

    JsonObject newInstanceFromStorage = getRecordFromStorageById(FakeInventoryStorage.INSTANCE_STORAGE_PATH, instanceIdSameRecordNewMatchKey);
    testContext.assertEquals(newInstanceFromStorage.getJsonArray("identifiers").size(),1,
            "After third upsert, with 'Shared Input Instance', there should be [1] identifier on a new instance " + newInstanceFromStorage.encodePrettily());

    JsonObject oldInstanceFromStorage = getRecordFromStorageById( FakeInventoryStorage.INSTANCE_STORAGE_PATH, instanceId );
    testContext.assertEquals(oldInstanceFromStorage.getJsonArray("identifiers").size(),1,
            "After third upsert, with 'Shared Input Instance', there should be [1] identifier left on the previous instance " + oldInstanceFromStorage.encodePrettily());

    JsonObject previousInstanceStoredHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(previousInstanceStoredHoldings.getInteger("totalRecords"), 2,
            "After third upsert, with 'Shared Input Instance',  there should be [2] holdings records left for the Instance with the previous match key " + instanceId + ": " + previousInstanceStoredHoldings.encodePrettily() );

  }

  @Test
  public void deleteByIdentifiersThatDoNotExistInSharedInventoryWillReturn404 (TestContext testContext) {
    delete(404, MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH,
              new JsonObject()
                    .put("institutionId", INSTITUTION_ID_1)
                    .put("localIdentifier","DOES_NOT_EXIST")
                    .put("identifierTypeId", "DOES_NOT_EXIST"));
  }

  /**
   * Tests API /inventory-upsert-hrid
  *
   */
  @Test
  public void testUpsertByHridWillCreateNewInstance(TestContext testContext) {
    createInitialInstanceWithHrid1();
    InputInstance instance = new InputInstance().setTitle("New title").setInstanceTypeId("12345").setHrid("2").setSource("test");
    InventoryRecordSet inventoryRecordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, "hrid==\"" + instance.getHrid() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
            "Before upserting with new Instance, the number of Instances with that HRID should be [0]" );

    upsertByHrid(inventoryRecordSet.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH,"hrid==\"" + instance.getHrid() + "\"");
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
            "After upserting with new Instance, the number of Instances with that HRID should be [1]" );
  }

  /**
   * Tests API /inventory-upsert-hrid
  *
   */
  @Test
  public void upsertByHridWillUpdateExistingInstance (TestContext testContext) {
    createInitialInstanceWithHrid1();
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject();
    inventoryRecordSet.put("instance", new InputInstance()
            .setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson());

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
  *
   */
  @Test
  public void upsertByHridWillCreateHoldingsAndItems(TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
               new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
              .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                      .put("items", new JsonArray()
                        .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                        .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
              .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                      .put("items", new JsonArray()
                        .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());
    JsonObject storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
            "After upsert the total number of items should be [3] " + storedHoldings.encodePrettily() );

  }

  @Test
  public void upsertByHridWillCreateHoldingsWithoutItems (TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson())
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson())));

    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");
    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 0,
            "Upsert metrics response should report [0] items created " + upsertResponseJson.encodePrettily());
  }

  @Test
  public void upsertByHridWillUpdateHoldingsAndItems (TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("updated").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("updated").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("updated").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, UPDATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully updated " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, UPDATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully updated " + upsertResponseJson.encodePrettily());

    getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH,null).getJsonArray("items").stream().forEach(item -> {
      testContext.assertEquals(((JsonObject)item).getString("barcode"), "updated",
              "The barcode of all items should be updated to 'updated' after upsert of existing record set with holdings and items");
    });

  }

  @Test
  public void upsertByHridWillRetainItemStatusUnlessStatusIsInOverwriteList (TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").setStatus("On order").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").setStatus("Unknown").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("updated").setStatus("Available").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("updated").setStatus("Available").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("updated").getJson()))))
            .put("processing", new InputProcessingInstructions()
                    .setItemStatusPolicy(ProcessingInstructions.ITEM_STATUS_POLICY_OVERWRITE)
                    .setListOfStatuses("On order").getJson()));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, UPDATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully updated " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, UPDATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully updated " + upsertResponseJson.encodePrettily());

    JsonArray item001 = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, "hrid==\"ITM-001\"").getJsonArray("items");
    testContext.assertEquals(item001.getJsonObject(0).getJsonObject("status").getString("name"),
            "Available",
            "Status for item ITM-001 should have been updated from 'On order' to 'Available");

    JsonArray item002 = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, "hrid==\"ITM-002\"").getJsonArray("items");
    testContext.assertEquals(item002.getJsonObject(0).getJsonObject("status").getString("name"),
            "Unknown",
            "Status for item ITM-002 should have been retained as 'Unknown'");
  }

  @Test
  public void upsertByHridWillOnlyUpdateItemStatusIfStatusIsInOverwriteList (TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").setStatus("On order").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").setStatus("Unknown").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").setStatus("Checked out").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("updated").setStatus("Available").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("updated").setStatus("Available").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("updated").setStatus("Available").getJson()))))
            .put("processing", new InputProcessingInstructions()
                    .setItemStatusPolicy(ProcessingInstructions.ITEM_STATUS_POLICY_OVERWRITE)
                    .setListOfStatuses("On order", "Unknown").getJson()));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, UPDATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully updated " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, UPDATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully updated " + upsertResponseJson.encodePrettily());

    JsonArray item001 = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, "hrid==\"ITM-001\"").getJsonArray("items");
    testContext.assertEquals(item001.getJsonObject(0).getJsonObject("status").getString("name"),
            "Available",
            "Status for item ITM-001 should have been updated to 'Available' from 'On order'");

    JsonArray item002 = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, "hrid==\"ITM-002\"").getJsonArray("items");
    testContext.assertEquals(item002.getJsonObject(0).getJsonObject("status").getString("name"),
            "Available",
            "Status for item ITM-002 should have been updated to 'Available' from 'Unknown'");

    JsonArray item003 = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, "hrid==\"ITM-003\"").getJsonArray("items");
    testContext.assertEquals(item003.getJsonObject(0).getJsonObject("status").getString("name"),
            "Checked out",
            "Status for item ITM-003 should have been retained as 'Checked out'");
  }


  @Test
  public void upsertByHridWillRetainAllItemStatusesIfInstructionIsRetain (TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").setStatus("On order").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").setStatus("Unknown").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("updated").setStatus("Available").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("updated").setStatus("Available").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("updated").getJson()))))
            .put("processing", new InputProcessingInstructions()
                    .setItemStatusPolicy(ProcessingInstructions.ITEM_STATUS_POLICY_RETAIN).getJson()));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, UPDATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully updated " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, UPDATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully updated " + upsertResponseJson.encodePrettily());

    JsonArray item001 = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, "hrid==\"ITM-001\"").getJsonArray("items");
    testContext.assertEquals(item001.getJsonObject(0).getJsonObject("status").getString("name"),
            "On order",
            "Status for item ITM-001 should have been retained as 'On order'");

    JsonArray item002 = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, "hrid==\"ITM-002\"").getJsonArray("items");
    testContext.assertEquals(item002.getJsonObject(0).getJsonObject("status").getString("name"),
            "Unknown",
            "Status for item ITM-002 should have been retained as 'Unknown'");
  }

  @Test
  public void upsertByHridWillOverwriteAllItemStatusesIfInstructionIsOverwrite (TestContext testContext) {
    String instanceHrid = "1";
    JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").setStatus("On order").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").setStatus("Unknown").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))))
            .put("processing", new JsonObject()));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    upsertResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("updated").setStatus("Available").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("updated").setStatus("Available").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("updated").getJson()))))
            .put("processing", new InputProcessingInstructions()
                    .setItemStatusPolicy(ProcessingInstructions.ITEM_STATUS_POLICY_OVERWRITE).getJson()));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, UPDATE , COMPLETED), 2,
            "Upsert metrics response should report [2] holdings records successfully updated " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, UPDATE , COMPLETED), 3,
            "Upsert metrics response should report [3] items successfully updated " + upsertResponseJson.encodePrettily());

    JsonArray item001 = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, "hrid==\"ITM-001\"").getJsonArray("items");
    testContext.assertEquals(item001.getJsonObject(0).getJsonObject("status").getString("name"),
            "Available",
            "Status for item ITM-001 should have been overwritten to 'Avaliable' from 'On order'");

    JsonArray item002 = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, "hrid==\"ITM-002\"").getJsonArray("items");
    testContext.assertEquals(item002.getJsonObject(0).getJsonObject("status").getString("name"),
            "Available",
            "Status for item ITM-002 should have been overwritten to 'Available' from 'Unknown'");
  }


  @Test
  public void upsertByHridWillDeleteSelectHoldingsAndItems(TestContext testContext) {
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
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
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));

    upsertResponseJson =  upsertByHrid(inventoryRecordSet);

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, DELETE , COMPLETED), 1,
            "After upsert with one holdings record removed from set, metrics should report [1] holdings record successfully deleted " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, DELETE , COMPLETED), 2,
            "After upsert with one holdings record removed from set, metrics should report [2] items successfully deleted " + upsertResponseJson.encodePrettily());
    JsonObject holdingsAfterUpsertJson = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(holdingsAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert with one holdings record removed from set, number of holdings records left for the Instance should be [1] " + holdingsAfterUpsertJson.encodePrettily());
    JsonObject itemsAfterUpsertJson = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(itemsAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert with one holdings record removed from set, the total number of item records should be [1] " + itemsAfterUpsertJson.encodePrettily() );
  }

  @Test
  public void upsertsByHridWillNotDeleteThenWillDeleteAllHoldingsAndItems (TestContext testContext) {

    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
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
    JsonObject itemsAfterUpsert0Json = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);

    // Upsert should not delete holdings/items when there's no holdings array in the request document
    upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("Updated InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson()));

    JsonObject holdingsAfterUpsert1Json = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);

    // Upsert should delete any attached holdings/items when there's an empty holdings array in the request
    upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("2nd Updated InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
                    .put("holdingsRecords", new JsonArray()));

    JsonObject holdingsAfterUpsert2Json = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);

    testContext.assertEquals(itemsAfterUpsert0Json.getInteger("totalRecords"), 3, "After creating base inventory record set there should be [3] items in it " + itemsAfterUpsert0Json.encodePrettily());
    testContext.assertEquals(holdingsAfterUpsert1Json.getInteger("totalRecords"), 2,
            "After upsert with no holdings record property in request, [2] holdings records should remain " +  holdingsAfterUpsert1Json.encodePrettily());

    testContext.assertEquals(holdingsAfterUpsert2Json.getInteger("totalRecords"), 0,
            "After upsert with empty holdings record array in request, [0] holdings records should remain " + holdingsAfterUpsert2Json.encodePrettily());

  }

  @Test
  public void upsertByHridWillCreateParentAndChildRelations(TestContext testContext) {

    String instanceHrid = "1";
    String childHrid = "2";
    String grandParentHrid = "3";

    upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson()));

    JsonObject childResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(instanceHrid).getJson()))));

    testContext.assertEquals(getMetric(childResponseJson, INSTANCE_RELATIONSHIP, CREATE , COMPLETED), 1,
            "After upsert of new Instance with parent relation, metrics should report [1] instance relationship successfully created " + childResponseJson.encodePrettily());
    JsonObject relationshipsAfterUpsertJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_RELATIONSHIP_STORAGE_PATH, null);
    testContext.assertEquals(relationshipsAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert of new Instance with parent relation, the total number of relationship records should be [1] " + relationshipsAfterUpsertJson.encodePrettily() );

    JsonObject grandParentResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(grandParentHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("childInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(instanceHrid).getJson()))));

    testContext.assertEquals(getMetric(grandParentResponseJson, INSTANCE_RELATIONSHIP, CREATE , COMPLETED), 1,
            "After upsert of new Instance with child relation, metrics should report [1] instance relationship successfully created " + grandParentResponseJson.encodePrettily());

    JsonObject relationshipsAfterGrandParent = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_RELATIONSHIP_STORAGE_PATH, null);

    testContext.assertEquals(relationshipsAfterGrandParent.getInteger("totalRecords"), 2,
            "After upsert of Instance with parent and Instance with child relation, the total number of relationship records should be [2] " + relationshipsAfterGrandParent.encodePrettily() );

  }

  @Test
  public void upsertByHridWillCreateParentAndChildRelationsUsingUUUIDsForIdentifiers(TestContext testContext) {

    String instanceHrid = "1";
    String childHrid = "2";
    String grandParentHrid = "3";

    JsonObject parentResponse = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson()));
    JsonObject childResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierUuid(parentResponse.getJsonObject( "instance" ).getString( "id" )).getJson()))));

    testContext.assertEquals(getMetric(childResponseJson, INSTANCE_RELATIONSHIP, CREATE , COMPLETED), 1,
            "After upsert of new Instance with parent relation, metrics should report [1] instance relationship successfully created " + childResponseJson.encodePrettily());
    JsonObject relationshipsAfterUpsertJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_RELATIONSHIP_STORAGE_PATH, null);
    testContext.assertEquals(relationshipsAfterUpsertJson.getInteger("totalRecords"), 1,
            "After upsert of new Instance with parent relation, the total number of relationship records should be [1] " + relationshipsAfterUpsertJson.encodePrettily() );

    JsonObject grandParentResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(grandParentHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("childInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(instanceHrid).getJson()))));

    testContext.assertEquals(getMetric(grandParentResponseJson, INSTANCE_RELATIONSHIP, CREATE , COMPLETED), 1,
            "After upsert of new Instance with child relation, metrics should report [1] instance relationship successfully created " + grandParentResponseJson.encodePrettily());

    JsonObject relationshipsAfterGrandParent = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_RELATIONSHIP_STORAGE_PATH, null);

    testContext.assertEquals(relationshipsAfterGrandParent.getInteger("totalRecords"), 2,
            "After upsert of Instance with parent and Instance with child relation, the total number of relationship records should be [2] " + relationshipsAfterGrandParent.encodePrettily() );

  }


  @Test
  public void upsertsByHridWillNotDeleteThenWillDeleteParentInstanceRelation (TestContext testContext) {

    // PARENT INSTANCE TO-BE
    String instanceHrid = "1";
    upsertByHrid(
            new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson()));
    // CHILD INSTANCE
    String childHrid = "2";
    JsonObject childResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(instanceHrid).getJson()))));

    testContext.assertEquals(getMetric(childResponseJson, INSTANCE_RELATIONSHIP, CREATE , COMPLETED), 1,
      "After upsert of Instance with parent relation, metrics should report [1] instance relationship successfully created " + childResponseJson.encodePrettily());

    // POST child Instance again with no parent list
    childResponseJson = upsertByHrid(new JsonObject()
       .put("instance",
              new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
       .put("instanceRelations", new JsonObject()));
    testContext.assertNull(childResponseJson.getJsonObject("metrics").getJsonObject(INSTANCE_RELATIONSHIP),
    "After upsert with no parent list, metrics should not report any instance relations updates " + childResponseJson.encodePrettily());

    // POST child Instance again with empty parent list.
    childResponseJson = upsertByHrid(new JsonObject()
      .put("instance",
          new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
      .put("instanceRelations", new JsonObject()
             .put("parentInstances", new JsonArray())));

    testContext.assertEquals(getMetric(childResponseJson, INSTANCE_RELATIONSHIP, DELETE, COMPLETED), 1,
            "After upsert with empty parent list, metrics should report [1] instance relationship successfully deleted " + childResponseJson.encodePrettily());

  }

  @Test
  public void upsertsByHridWillChangeTypeOfRelationshipBetweenTwoInstances (TestContext testContext) {
    // PARENT INSTANCE TO-BE
    String instanceHrid = "1";
    upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson()));
    // CHILD INSTANCE
    String childHrid = "2";
    JsonObject childResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceRelationshipTypeId("3333").setInstanceIdentifierHrid(instanceHrid).getJson()))));

    testContext.assertEquals(getMetric(childResponseJson, INSTANCE_RELATIONSHIP, CREATE , COMPLETED), 1,
            "After upsert of Instance with parent relation, metrics should report [1] instance relationship successfully created " + childResponseJson.encodePrettily());

    // POST child Instance again with no parent list
    childResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceRelationshipTypeId("4444").setInstanceIdentifierHrid(instanceHrid).getJson()))));

    testContext.assertEquals(getMetric(childResponseJson, INSTANCE_RELATIONSHIP, DELETE, COMPLETED), 1,
            "After upsert with different instance relationship type, metrics should report one instance relation deleted " + childResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(childResponseJson, INSTANCE_RELATIONSHIP, CREATE, COMPLETED), 1,
            "After upsert with different instance relationship type, metrics should report one instance relation created " + childResponseJson.encodePrettily());

  }

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
              new InputInstance().setTitle("InputInstance "+hrid).setInstanceTypeId("12345").setHrid(hrid).setSource("test").getJson()));
    }

    JsonObject firstResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("InputInstance with 8 relations").setInstanceTypeId("12345").setHrid("MAIN-INSTANCE").setSource("test").getJson())
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

    testContext.assertEquals(getMetric(firstResponseJson, INSTANCE_RELATIONSHIP, CREATE , COMPLETED), 4,
            "After upsert of Instance with multiple relations, metrics should report [4] instance relationship successfully created " + firstResponseJson.encodePrettily());

    testContext.assertEquals(getMetric(firstResponseJson, INSTANCE_TITLE_SUCCESSION, CREATE , COMPLETED), 4,
            "After upsert of Instance with multiple relations, metrics should report [4] instance title successions successfully created " + firstResponseJson.encodePrettily());

    JsonObject secondResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("InputInstance with 8 relations").setInstanceTypeId("12345").setHrid("MAIN-INSTANCE").setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parent2Hrid).setInstanceRelationshipTypeId("multipart").getJson()))
                    .put("childInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(child2Hrid).setInstanceRelationshipTypeId("multipart").getJson()))
                    .put("precedingTitles", new JsonArray()
                            .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid(preceding2Hrid).getJson()))
                    .put("succeedingTitles", new JsonArray()
                            .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid(succeeding2Hrid).getJson()))));

    testContext.assertEquals(getMetric(secondResponseJson, INSTANCE_RELATIONSHIP, DELETE , COMPLETED), 2,
            "After upsert of Instance with some relations removed, " +
                    "metrics should report [2] instance relationship successfully deleted "
                    + secondResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(secondResponseJson, INSTANCE_TITLE_SUCCESSION, DELETE , COMPLETED), 2,
            "After upsert of Instance with some relations removed, " +
                    "metrics should report [2] instance title successions successfully deleted "
                    + secondResponseJson.encodePrettily());


  }

  @Test
  public void upsertsByHridWillNotDeleteThenWillDeleteChildInstanceRelation (TestContext testContext) {

    // CHILD INSTANCE TO-BE
    String instanceHrid = "1";
    upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson()));
    // PARENT INSTANCE
    String parentHrid = "2";
    JsonObject parentResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(parentHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("childInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(instanceHrid).getJson()))));

    testContext.assertEquals(getMetric(parentResponseJson, INSTANCE_RELATIONSHIP, CREATE , COMPLETED), 1,
            "After upsert of Instance with child relation, metrics should report [1] instance relationship successfully created " + parentResponseJson.encodePrettily());

    // POST child Instance again with no parent list
    parentResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(parentHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()));
    testContext.assertNull(parentResponseJson.getJsonObject("metrics").getJsonObject(INSTANCE_RELATIONSHIP),
            "After upsert with no child list, metrics should not report any instance relations updates " + parentResponseJson.encodePrettily());

    // POST child Instance again with empty parent list.
    parentResponseJson = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Parent InputInstance").setInstanceTypeId("12345").setHrid(parentHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("childInstances", new JsonArray())));

    testContext.assertEquals(getMetric(parentResponseJson, INSTANCE_RELATIONSHIP, DELETE, COMPLETED), 1,
            "After upsert with empty child list, metrics should report [1] instance relationship successfully deleted " + parentResponseJson.encodePrettily());

  }

  @Test
  public void upsertByHridWillCreatePrecedingAndSucceedingTitleRelations (TestContext testContext) {

    upsertByHrid(
      new JsonObject()
              .put("instance",
                    new InputInstance().setTitle("A title").setInstanceTypeId("123").setHrid("002").setSource("test").getJson()));

    JsonObject upsertResponseJson2 = upsertByHrid(
      new JsonObject()
              .put("instance",
                      new InputInstance().setTitle("A preceding title").setInstanceTypeId("123").setHrid("001").setSource("test").getJson())
              .put("instanceRelations", new JsonObject()
               .put("succeedingTitles", new JsonArray()
                .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid("002").getJson()))));

    testContext.assertEquals(getMetric(upsertResponseJson2, INSTANCE_TITLE_SUCCESSION, CREATE, COMPLETED), 1,
            "After upsert of preceding title, metrics should report [1] instance title successions successfully created " + upsertResponseJson2.encodePrettily());

    JsonObject upsertResponseJson3 = upsertByHrid(
      new JsonObject()
              .put("instance",
                      new InputInstance().setTitle("A succeeding title").setInstanceTypeId("123").setHrid("003").setSource("test").getJson())
              .put("instanceRelations", new JsonObject()
                .put("precedingTitles", new JsonArray()
                  .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid("002").getJson()))));

    testContext.assertEquals(getMetric(upsertResponseJson3, INSTANCE_TITLE_SUCCESSION, CREATE, COMPLETED), 1,
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
                            new InputInstance().setTitle("A title").setInstanceTypeId("123").setHrid("002").setSource("test").getJson()));

    JsonObject upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A preceding title").setInstanceTypeId("123").setHrid("001").setSource("test").getJson())
                    .put("instanceRelations", new JsonObject()
                            .put("succeedingTitles", new JsonArray()
                                    .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid("002").getJson()))));

    testContext.assertEquals(getMetric(upsertResponseJson2, INSTANCE_TITLE_SUCCESSION, CREATE, COMPLETED), 1,
            "After upsert of preceding title, metrics should report [1] instance title successions successfully created " + upsertResponseJson2.encodePrettily());

    // POST preceding title again with no succeeding titles list
    upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A preceding title").setInstanceTypeId("123").setHrid("001").setSource("test").getJson())
                    .put("instanceRelations", new JsonObject()));
    testContext.assertNull(upsertResponseJson2.getJsonObject("metrics").getJsonObject("INSTANCE_TITLE_SUCCESSION"),
            "After upsert with no succeeding titles list, metrics should not report any instance title succession updates " + upsertResponseJson2.encodePrettily());

    // POST preceding title again with empty succeeding titles list.
    upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A preceding title").setInstanceTypeId("123").setHrid("001").setSource("test").getJson())
                    .put("instanceRelations", new JsonObject()
                            .put("succeedingTitles", new JsonArray())));

    testContext.assertEquals(getMetric(upsertResponseJson2, INSTANCE_TITLE_SUCCESSION, DELETE, COMPLETED), 1,
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
                            new InputInstance().setTitle("A title").setInstanceTypeId("123").setHrid("001").setSource("test").getJson()));

    JsonObject upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A succeeding title").setInstanceTypeId("123").setHrid("002").setSource("test").getJson())
                    .put("instanceRelations", new JsonObject()
                            .put("precedingTitles", new JsonArray()
                                    .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid("001").getJson()))));

    testContext.assertEquals(getMetric(upsertResponseJson2, INSTANCE_TITLE_SUCCESSION, CREATE, COMPLETED), 1,
            "After upsert of succeeding title, metrics should report [1] instance title successions successfully created " + upsertResponseJson2.encodePrettily());

    // POST succeeding title again with no preceding titles list
    upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A succeeding title").setInstanceTypeId("123").setHrid("002").setSource("test").getJson())
                    .put("instanceRelations", new JsonObject()));
    testContext.assertNull(upsertResponseJson2.getJsonObject("metrics").getJsonObject("INSTANCE_TITLE_SUCCESSION"),
            "After upsert with no preceding titles list, metrics should not report any instance title succession updates " + upsertResponseJson2.encodePrettily());

    // POST succeeding title again with empty preceding titles list.
    upsertResponseJson2 = upsertByHrid(
            new JsonObject()
                    .put("instance",
                            new InputInstance().setTitle("A preceding title").setInstanceTypeId("123").setHrid("002").setSource("test").getJson())
                    .put("instanceRelations", new JsonObject()
                            .put("precedingTitles", new JsonArray())));

    testContext.assertEquals(getMetric(upsertResponseJson2, INSTANCE_TITLE_SUCCESSION, DELETE, COMPLETED), 1,
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
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parentHrid)
                                    .setProvisionalInstance(
                                            new InputInstance()
                                                    .setTitle("Provisional Instance")
                                                    .setSource("MARC")
                                                    .setInstanceTypeId("12345").getJson()).getJson()))));

    testContext.assertEquals(getMetric(childResponseJson, INSTANCE_RELATIONSHIP, CREATE, COMPLETED), 1,
            "Upsert metrics response should report [1] instance relationship successfully created " + childResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(childResponseJson, INSTANCE_RELATIONSHIP, PROVISIONAL_INSTANCE, COMPLETED), 1,
            "Upsert metrics response should report [1] provisional instance successfully created " + childResponseJson.encodePrettily());
    JsonObject instancesAfterUpsertJson = getRecordsFromStorage(FakeInventoryStorage.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterUpsertJson.getInteger("totalRecords"), 2,
            "After upsert with provisional instance the total number of instances should be [2] " + instancesAfterUpsertJson.encodePrettily() );
  }

  @Test
  public void upsertByHridWillGraciouslyFailToCreateRelationWithoutProvisionalInstance (TestContext testContext) {
    String childHrid = "002";
    String parentHrid = "001";

    Response childResponse = upsertByHrid(207, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parentHrid).getJson()))));

    JsonObject responseJson = new JsonObject(childResponse.getBody().asString());
    testContext.assertEquals(getMetric(responseJson, INSTANCE_RELATIONSHIP, CREATE, FAILED), 1,
            "Upsert metrics response should report [1] relation creation failure due to missing provisional instance  " + responseJson.encodePrettily());

    childResponse = upsertByHrid(207, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parentHrid)
                                    .setProvisionalInstance(
                                            new InputInstance()
                                                    .setSource("MARC")
                                                    .setInstanceTypeId("12345").getJson()).getJson()))));

    responseJson = new JsonObject(childResponse.getBody().asString());
    testContext.assertEquals(getMetric(responseJson, INSTANCE_RELATIONSHIP, CREATE, FAILED), 1,
            "Upsert metrics response should report [1] relation creation failure due to missing mandatory properties in provisional instance  " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, INSTANCE_RELATIONSHIP, PROVISIONAL_INSTANCE, FAILED), 1,
            "Upsert metrics response should report [1] provisional Instance creation failure due to missing mandatory properties in provisional instance  " + responseJson.encodePrettily());

  }

  @Test
  public void upsertByHridWillSilentlyOmitRelationWithoutInstanceIdentifier (TestContext testContext) {
    String childHrid = "002";
    String parentHrid = "001";

    Response childResponse = upsertByHrid(200, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship()
                                    .setProvisionalInstance(
                                            new InputInstance()
                                                    .setSource("MARC")
                                                    .setInstanceTypeId("12345").getJson()).getJson()))));

    JsonObject responseJson = new JsonObject(childResponse.getBody().asString());
    testContext.assertTrue(responseJson.getJsonObject("instanceRelations").isEmpty(),
            "No Instance relations should have been created  due to missing Instance identifier " + responseJson.encodePrettily());

    childResponse = upsertByHrid(200, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierHrid(null)
                                    .setProvisionalInstance(
                                            new InputInstance()
                                                    .setSource("MARC")
                                                    .setInstanceTypeId("12345").getJson()).getJson()))));

    responseJson = new JsonObject(childResponse.getBody().asString());
    testContext.assertTrue(responseJson.getJsonObject("instanceRelations").isEmpty(),
            "No Instance relations should have been created due to empty Instance identifier " + responseJson.encodePrettily());

  }


  @Test
  public void upsertByHridWillRunWithBadUuidAsRelationIdentifierButNotFindTheRelation (TestContext testContext) {
    String childHrid = "002";
    String badUuid = "bad";

    Response childResponse = upsertByHrid(207, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Child InputInstance").setInstanceTypeId("12345").setHrid(childHrid).setSource("test").getJson())
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances", new JsonArray()
                            .add(new InputInstanceRelationship().setInstanceIdentifierUuid(badUuid).getJson()))));

    JsonObject responseJson = new JsonObject(childResponse.getBody().asString());
    testContext.assertEquals(getMetric(responseJson, INSTANCE_RELATIONSHIP, CREATE, FAILED), 1,
            "Upsert metrics response should report [1] relation creation failure due to missing provisional instance  " + responseJson.encodePrettily());

  }

  @Test
  public void deleteByHridWillDeleteInstanceRelationsHoldingsItems (TestContext testContext) {
     // Create succeeding title
     upsertByHrid(
             new JsonObject()
                     .put("instance",
                             new InputInstance().setTitle("A title").setInstanceTypeId("123").setHrid("001").setSource("test").getJson()));

     String instanceHrid = "002";
     JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
             .put("instance",
                     new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
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

     testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
             "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
     testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
             "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());
     testContext.assertEquals(getMetric(upsertResponseJson, INSTANCE_TITLE_SUCCESSION, CREATE, COMPLETED), 1,
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
     testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE , COMPLETED), 2,
             "Upsert metrics response should report [2] holdings records successfully deleted " + deleteResponse.encodePrettily());
     testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , COMPLETED), 3,
             "Delete metrics response should report [3] items successfully deleted " + deleteResponse.encodePrettily());
     testContext.assertEquals(getMetric(deleteResponse, INSTANCE_TITLE_SUCCESSION, DELETE , COMPLETED), 1,
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
   public void deleteSignalByHridForNonExistingInstanceWillReturn404 (TestContext testContext) {
     JsonObject deleteSignal = new JsonObject().put("hrid","DOES_NOT_EXIST");
     delete(404, MainVerticle.INVENTORY_UPSERT_HRID_PATH,deleteSignal);
   }

   @Test
   public void upsertByHridWillMoveHoldingsAndItems (TestContext testContext) {
     String instanceHrid1 = "1";
     JsonObject firstResponse = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("InputInstance 1").setInstanceTypeId("12345").setHrid(instanceHrid1).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

     String instanceId1 = firstResponse.getJsonObject("instance").getString("id");
     JsonObject storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId1 + "\"");
     testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After upsert the number of holdings records for instance " + instanceId1 + " should be [2] " + storedHoldings.encodePrettily() );

     String instanceHrid2 = "2";
     upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("InputInstance 2X").setInstanceTypeId("12345").setHrid(instanceHrid2).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

     storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId1 + "\"");
     testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 0,
            "After move of holdings the number of holdings records for instance " + instanceId1 + " should be [0] " + storedHoldings.encodePrettily() );

     storedHoldings = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "hrid==\"HOL-001\" or hrid==\"HOL-002\"");
     testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
            "After move of holdings they should still exist, count should be [2] " + storedHoldings.encodePrettily() );

     JsonObject thirdResponse = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("InputInstance 1X").setInstanceTypeId("12345").setHrid(instanceHrid1).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-003").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-3").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

     testContext.assertEquals(getMetric(thirdResponse, HOLDINGS_RECORD, CREATE , COMPLETED), 1,
            "Third update should report [1] holdings record successfully created  " + thirdResponse.encodePrettily());
     testContext.assertEquals(getMetric(thirdResponse, ITEM, UPDATE , COMPLETED), 1,
            "Third update should report [1] item successfully updated (moved)   " + thirdResponse.encodePrettily());

     JsonObject storedItems = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, null);
     testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
            "After two moves of holdings/items there should still be [3] items total in storage " + storedItems.encodePrettily() );

     JsonObject fourthResponse = upsertByHrid(new JsonObject()
             .put("instance",
                     new InputInstance().setTitle("InputInstance 2X").setInstanceTypeId("12345").setHrid(instanceHrid2).setSource("test").getJson())
             .put("holdingsRecords", new JsonArray()
                     .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                             .put("items", new JsonArray()
                                     .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                     .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))));

     JsonObject storedHoldings002 = getRecordsFromStorage(FakeInventoryStorage.HOLDINGS_STORAGE_PATH, "hrid==\"HOL-002\"");
     JsonObject holdings002 = storedHoldings002.getJsonArray(RESULT_SET_HOLDINGS_RECORDS).getJsonObject(0);
     JsonObject storedItemsHol002 = getRecordsFromStorage(FakeInventoryStorage.ITEM_STORAGE_PATH, "holdingsRecordId==\""+holdings002.getString("id")+"\"");
     testContext.assertEquals(storedItemsHol002.getInteger("totalRecords"), 2,
             "After moves of items from one holding to the other there should be [2] items on HOL-002 in storage " + storedItemsHol002.encodePrettily() );

   }

   @Test
   public void canFetchInventoryRecordSetFromUpsertHridApiWithHridAndUuid (TestContext testContext) {
     String instanceHrid1 = "1";

     // Create succeeding title
     upsertByHrid(
             new JsonObject()
                     .put("instance",
                             new InputInstance().setTitle("A succeeding title").setInstanceTypeId("123").setHrid("001").setSource("test").getJson()));
     // Create preceding title
     upsertByHrid(
             new JsonObject()
                     .put("instance",
                             new InputInstance().setTitle("A preceding title").setInstanceTypeId("123").setHrid("002").setSource("test").getJson()));

     JsonObject newInstance = upsertByHrid(new JsonObject()
             .put("instance",
                     new InputInstance().setTitle("InputInstance 1").setInstanceTypeId("12345").setHrid(instanceHrid1).setSource("test").getJson())
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
                             .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid("001").getJson()))
                     .put("precedingTitles", new JsonArray()
                             .add(new InputInstanceTitleSuccession().setInstanceIdentifierHrid("002").getJson()))
                     .put("parentInstances", new JsonArray()
                             .add(new InputInstanceRelationship().setInstanceIdentifierHrid( "001" ).getJson() ))
                     .put("childInstances", new JsonArray()
                             .add(new InputInstanceRelationship().setInstanceIdentifierHrid( "002" ).getJson()))));

     fetchRecordSetFromUpsertHrid( "1" );
     fetchRecordSetFromUpsertHrid (newInstance.getJsonObject( "instance" ).getString( "id" ));
     getJsonObjectById( MainVerticle.FETCH_INVENTORY_RECORD_SETS_ID_PATH, "2", 404 );
   }

  @Test
  public void canFetchInventoryRecordSetFromUpsertSharedInventoryApiWithHridAndUuid (TestContext testContext) {
    String instanceHrid1 = "1";
    JsonObject newInstance = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("InputInstance 1")
                            .setInstanceTypeId("12345")
                            .setHrid(instanceHrid1)
                            .setSource("test")
                            .setMatchKeyAsString( "inputinstance_1" ).getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));


    JsonObject irs = fetchRecordSetFromUpsertSharedInventory( "1" );
    logger.info(irs.getJsonObject( "instanceRelations" ));
    fetchRecordSetFromUpsertSharedInventory (newInstance.getJsonObject( "instance" ).getString( "id" ));
    getJsonObjectById( MainVerticle.FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH, "2", 404 );

  }

  @Test
  public void cannotFetchFromUpsertSharedInventoryApiIfInstanceHasNoMatchKey (TestContext testContext) {
    String instanceHrid1 = "1";
    JsonObject newInstance = upsertByHrid(new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("InputInstance 1")
                            .setInstanceTypeId("12345")
                            .setHrid(instanceHrid1)
                            .setSource("test")
                            .getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));


    getJsonObjectById( MainVerticle.FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH, "1", 400 );
  }


  @Test
   public void upsertByHridWithMissingInstanceHridWillBeRejected (TestContext testContext) {
    upsertByHrid(422, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-003").getJson())))));

    Response response = upsertByHrid(422, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setBarcode("BC-003").getJson())))));
    logger.info(response.asPrettyString());

  }


  @Test
  public void upsertByHridWithMissingItemHridWillBeRejected (TestContext testContext) {
    String instanceHrid = "1";
    Response upsertResponse = upsertByHrid(422, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
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
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
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
    Response upsertResponse = upsertByHrid(207, new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
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
    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , FAILED), 2,
            "Upsert metrics response should report [2] holdings records create failure for wrong location ID on one of them (whole batch fails) " + upsertResponseJson.encodePrettily());
  }

  @Test
  public void upsertByHridWillReturnErrorResponseOnMissingInstanceInRequestBody (TestContext testContext) {
    upsertByHrid(400, new JsonObject().put("invalid", "No Instance here"));
  }

  @Test
  public void testInvalidApiPath (TestContext testContext) {
    JsonObject inventoryRecordSet = new JsonObject();
    inventoryRecordSet.put("instance", new InputInstance()
            .setTitle("Initial InputInstance").setInstanceTypeId("12345").getJson());
    putJsonObject(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH + "/invalid",inventoryRecordSet,404);
  }

  @Test
  public void testSendingNonJson (TestContext testContext) {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    RestAssured.given()
            .body("bad request body")
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(MainVerticle.INVENTORY_UPSERT_HRID_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(400).extract().response();

    RestAssured.given()
            .body(new JsonObject().toString())
            .header("Content-type","text/plain")
            .header(OKAPI_URL_HEADER)
            .put(MainVerticle.INVENTORY_UPSERT_HRID_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(400).extract().response();

    RestAssured.given()
            .body(new JsonObject().toString())
            .header("Content-type","text/plain")
            .header(OKAPI_URL_HEADER)
            .delete(MainVerticle.INVENTORY_UPSERT_HRID_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(400).extract().response();

    RestAssured.given()
            .body(new JsonObject().toString())
            .header("Content-type","text/plain")
            .header(OKAPI_URL_HEADER)
            .put(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(400).extract().response();

    RestAssured.given()
            .body(new JsonObject().toString())
            .header("Content-type","text/plain")
            .header(OKAPI_URL_HEADER)
            .delete(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(400).extract().response();

  }

  @Test
  public void testForcedItemCreateFailure (TestContext testContext) {
    fakeInventoryStorage.itemStorage.failOnCreate = true;
    Response response = upsertByHrid(207,new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    JsonObject responseJson = new JsonObject(response.getBody().asString());
    testContext.assertEquals(getMetric(responseJson, ITEM, CREATE , FAILED), 3,
            "Upsert metrics response should report [3] item record create failures (forced) " + responseJson.encodePrettily());

  }

  @Test
  public void testForcedHoldingsCreateFailure (TestContext testContext) {
    fakeInventoryStorage.holdingsStorage.failOnCreate = true;
    Response response = upsertByHrid(207,new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    JsonObject responseJson = new JsonObject(response.getBody().asString());

    testContext.assertEquals(getMetric(responseJson, HOLDINGS_RECORD, CREATE , FAILED), 2,
            "Upsert metrics response should report [2] holdings record create failures (forced) " + responseJson.encodePrettily());

    testContext.assertEquals(getMetric(responseJson, ITEM, CREATE , SKIPPED), 3,
            "Upsert metrics response should report [3] item record creates skipped " + responseJson.encodePrettily());

  }

  @Test
  public void testForcedItemUpdateFailure (TestContext testContext) {
    fakeInventoryStorage.itemStorage.failOnUpdate = true;
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));
    upsertByHrid (inventoryRecordSet);
    Response response = upsertByHrid(207,inventoryRecordSet);
    JsonObject responseJson = new JsonObject(response.getBody().asString());

    testContext.assertEquals(getMetric(responseJson, ITEM, UPDATE , FAILED), 3,
            "Upsert metrics response should report [3] item record update failures (forced) " + responseJson.encodePrettily());

  }

  @Test
  public void testForcedHoldingsUpdateFailure (TestContext testContext) {
    fakeInventoryStorage.holdingsStorage.failOnUpdate = true;
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));
    upsertByHrid (inventoryRecordSet);
    Response response = upsertByHrid(207,inventoryRecordSet);

    JsonObject responseJson = new JsonObject(response.getBody().asString());

    testContext.assertEquals(getMetric(responseJson, HOLDINGS_RECORD, UPDATE , FAILED), 2,
            "Upsert metrics response should report [2] holdings record update failures (forced) " + responseJson.encodePrettily());

  }

  @Test
  public void testForcedItemDeleteFailure (TestContext testContext) {
    fakeInventoryStorage.itemStorage.failOnDelete = true;
    upsertByHrid (new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    Response response = upsertByHrid(207,new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    JsonObject responseJson = new JsonObject(response.getBody().asString());

    testContext.assertEquals(getMetric(responseJson, ITEM, DELETE , FAILED), 1,
            "Upsert metrics response should report [1] item delete failure (forced) " + responseJson.encodePrettily());

  }

  @Test
  public void testForcedHoldingsDeleteFailure (TestContext testContext) {
    fakeInventoryStorage.holdingsStorage.failOnDelete = true;
    upsertByHrid (new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    Response response = upsertByHrid(207,new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    JsonObject responseJson = new JsonObject(response.getBody().asString());

    testContext.assertEquals(getMetric(responseJson, HOLDINGS_RECORD, DELETE , FAILED), 1,
            "Upsert metrics response should report [1] holdings record delete failure (forced) " + responseJson.encodePrettily());

    testContext.assertEquals(getMetric(responseJson, ITEM, DELETE, COMPLETED), 2,
            "Upsert metrics response should report [2] items successfully deleted " + responseJson.encodePrettily());

  }

  @Test
  public void testForcedItemGetRecordsFailure (TestContext testContext) {
    fakeInventoryStorage.itemStorage.failOnGetRecords = true;
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));
    upsertByHrid (500,inventoryRecordSet);

  }

  @Test
  public void testForcedHoldingsGetRecordsFailure (TestContext testContext) {
    fakeInventoryStorage.holdingsStorage.failOnGetRecords = true;
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));
    upsertByHrid (500,inventoryRecordSet);

  }

  @Test
  public void testForcedInstanceGetRecordsFailure (TestContext testContext) {
    fakeInventoryStorage.instanceStorage.failOnGetRecords = true;
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))));
    upsertByHrid (500,inventoryRecordSet);

  }

  @Test
  public void testForcedInstanceRelationshipsGetRecordsFailure (TestContext testContext) {
    fakeInventoryStorage.instanceRelationshipStorage.failOnGetRecords = true;
    fakeInventoryStorage.precedingSucceedingStorage.failOnGetRecords = true;
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Test forcedInstanceRelationshipGetRecordsFailure").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))))
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances",new JsonArray())
                    .put("childInstances", new JsonArray())
                    .put("succeedingTitles", new JsonArray())
                    .put("precedingTitles", new JsonArray()));
    upsertByHrid (inventoryRecordSet);
    upsertByHrid (500,inventoryRecordSet);

  }

  @Test
  public void testForcedLocationsGetRecordsFailure (TestContext testContext) {
    fakeInventoryStorage.locationStorage.failOnGetRecords = true;
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Test forcedLocationsGetRecordsFailure").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId("UNKNOWN_LOCATION").setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId("UNKNOWN_LOCATION").setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))))
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances",new JsonArray())
                    .put("childInstances", new JsonArray())
                    .put("succeedingTitles", new JsonArray())
                    .put("precedingTitles", new JsonArray()));
    upsertByMatchKey (500, inventoryRecordSet);

  }

  @Test
  public void testWithEmptyLocationsTable (TestContext testContext) {
    RestAssured.given()
            .body("{}")
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .delete(FakeInventoryStorage.LOCATION_STORAGE_PATH)
            .then()
            .log().ifValidationFails()
            .statusCode(200).extract().response();

    UpdatePlanSharedInventory.locationsToInstitutionsMap.clear();
    JsonObject inventoryRecordSet = new JsonObject()
            .put("instance",
                    new InputInstance().setTitle("Test forcedLocationsGetRecordsFailure").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()
                    .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId("ANOTHER_UNKNOWN_LOCATION").setCallNumber("test-cn-1").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-001").setBarcode("BC-001").getJson())
                                    .add(new InputItem().setHrid("ITM-002").setBarcode("BC-002").getJson())))
                    .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId("ANOTHER_UNKNOWN_LOCATION").setCallNumber("test-cn-2").getJson()
                            .put("items", new JsonArray()
                                    .add(new InputItem().setHrid("ITM-003").setBarcode("BC-003").getJson()))))
            .put("instanceRelations", new JsonObject()
                    .put("parentInstances",new JsonArray())
                    .put("childInstances", new JsonArray())
                    .put("succeedingTitles", new JsonArray())
                    .put("precedingTitles", new JsonArray()));
    upsertByMatchKey (500, inventoryRecordSet);

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

  private Response upsertByMatchKey (int expectedStatusCode, JsonObject inventoryRecordSet) {
    return putJsonObject(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH, inventoryRecordSet, expectedStatusCode);
  }

  private JsonObject batchUpsertByMatchKey(JsonObject batchOfInventoryRecordSets) {
    return putJsonObject(MainVerticle.SHARED_INVENTORY_BATCH_UPSERT_MATCHKEY_PATH, batchOfInventoryRecordSets);
  }

  private JsonObject upsertByHrid (JsonObject inventoryRecordSet) {
    return putJsonObject(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet);
  }

  private JsonObject batchUpsertByHrid (JsonObject batchOfInventoryRecordSets) {
    return putJsonObject(MainVerticle.INVENTORY_BATCH_UPSERT_HRID_PATH, batchOfInventoryRecordSets);
  }

  private Response batchUpsertByHrid(int expectedStatusCode, JsonObject batchOfInventoryRecordSets) {
    return putJsonObject(MainVerticle.INVENTORY_BATCH_UPSERT_HRID_PATH, batchOfInventoryRecordSets, expectedStatusCode);
  }

  private JsonObject fetchRecordSetFromUpsertHrid (String hridOrUuid) {
    return getJsonObjectById( MainVerticle.FETCH_INVENTORY_RECORD_SETS_ID_PATH, hridOrUuid );
  }

  private JsonObject fetchRecordSetFromUpsertSharedInventory (String hridOrUuid) {
    return getJsonObjectById( MainVerticle.FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH, hridOrUuid );
  }

  private Response upsertByHrid (int expectedStatusCode, JsonObject inventoryRecordSet) {
    return putJsonObject(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet, expectedStatusCode);
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

  private Response getJsonObjectById (String apiPath, String id, int expectedStatusCode) {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    return RestAssured.given()
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .get(apiPath.replaceAll( ":id", id ))
            .then()
            .log().ifValidationFails()
            .statusCode(expectedStatusCode).extract().response();
  }

  private JsonObject getJsonObjectById(String apiPath, String hridOrUuid) {
    return new JsonObject(getJsonObjectById(apiPath, hridOrUuid, 200).getBody().asString());
  }

  private JsonObject delete(String apiPath, JsonObject requestJson) {
    return new JsonObject(delete(200, apiPath, requestJson).getBody().asString());
  }

  private Response delete(int expectedStatusCode, String apiPath, JsonObject requestJson) {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    return RestAssured.given()
            .body(requestJson.toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .delete(apiPath)
            .then()
            .log().ifValidationFails()
            .statusCode(expectedStatusCode).extract().response();
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
