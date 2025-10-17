package org.folio.inventoryupdate.updating.test;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventoryupdate.updating.MatchKey;
import org.folio.inventoryupdate.updating.UpdatePlanSharedInventory;
import org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApis;
import org.folio.inventoryupdate.updating.test.fakestorage.entitites.*;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApis.*;

@RunWith(VertxUnitRunner.class)
public class MatchKeyApiTests extends InventoryUpdateTestBase {
  public static final String SHARED_INVENTORY_BATCH_UPSERT_MATCHKEY_PATH = "/shared-inventory-batch-upsert-matchkey";
  public static final String SHARED_INVENTORY_UPSERT_MATCHKEY_PATH = "/shared-inventory-upsert-matchkey";
  public static final String FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH = SHARED_INVENTORY_UPSERT_MATCHKEY_PATH+"/fetch/:id";


  @Test
  public void upsertByMatchKeyWillCreateNewInstance (TestContext testContext) {
    InventoryUpdateTestBase.createInitialInstanceWithMatchKey();
    InputInstance instance = new InputInstance()
        .setTitle("New title")
        .setInstanceTypeId("12345")
        .setSource("test")
        .generateMatchKey();
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKeyAsString(matchKey.getKey());
    InventoryRecordSet recordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
        "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    InventoryUpdateTestBase.upsertByMatchKey(recordSet.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
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

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
        "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    batchUpsertByMatchKey(batch.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
        "Number of instance records for query by matchKey 'new_title' after PUT expected: 1" );

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
    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Number of instance records for before PUT expected: 1" );
    batchUpsertByMatchKey(batch.getJson());
    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 201,
        "Number of instance records after PUT expected: 201" );

  }

  @Test
  public void batchByMatchKeyWithOneErrorWillCreate99NewInstances (TestContext testContext) {
    createInitialInstanceWithHrid1();
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    for (int i=0; i<50; i++) {
      InputInstance instance = new InputInstance()
          .setTitle("New title " + i)
          .setInstanceTypeId("12345")
          .generateMatchKey();
      if (i!=25) {
        instance.setSource("test");
      }
      batch.addRecordSet(new JsonObject()
          .put("instance", instance.getJson())
          .put("processing", new JsonObject().put("batchIndex",i)));
    }
    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Number of instance records for before PUT expected: 1" );
    batchUpsertByMatchKey(207, batch.getJson());
    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 50,
        "Number of instance records after PUT expected: 100" );
  }
  @Test
  public void batchByMatchKeyWithRepeatMatchKeyWillCreate29NewInstances (TestContext testContext) {
    createInitialInstanceWithHrid1();
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    for (int i=0; i<29; i++) {
      InputInstance instance = new InputInstance()
          .setTitle("New title " + i)
          .setSource("test")
          .setInstanceTypeId("12345")
          .generateMatchKey();
      batch.addRecordSet(new InventoryRecordSet(instance));
    }
    InputInstance instance = new InputInstance()
        .setTitle("New title 20")
        .setSource("test")
        .setInstanceTypeId("12345")
        .generateMatchKey();
    batch.addRecordSet(new InventoryRecordSet(instance));

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Number of instance records for before PUT expected: 1" );
    Response response = batchUpsertByMatchKey(200,batch.getJson());
    JsonObject responseJson = new JsonObject(response.asString());
    JsonObject metrics = responseJson.getJsonObject("metrics");
    testContext.assertEquals(metrics.getJsonObject("INSTANCE").getJsonObject("CREATE").getInteger("COMPLETED"), 29,
        "Number of instance records created after PUT of batch of 29 expected: 29");
    testContext.assertEquals(metrics.getJsonObject("INSTANCE").getJsonObject("UPDATE").getInteger("COMPLETED"), 1,
        "Number of instance records updated after PUT of batch of 29 expected: 1");
    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 30,
        "Number of instance records after PUT expected: 30" );
  }

  @Test
  public void batchByMatchKeyWithRepeatLocalIdentifierWillCreate29NewInstances (TestContext testContext) {
    createInitialInstanceWithHrid1();
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    for (int i=0; i<29; i++) {
      InputInstance instance = new InputInstance()
          .setTitle("New title " + i)
          .setSource("test")
          .setInstanceTypeId("12345")
          .generateMatchKey();
      batch.addRecordSet(new InventoryRecordSet(instance).getJson().put(PROCESSING,new JsonObject().put("localIdentifier","id" + i)));
    }
    InputInstance instance = new InputInstance()
        .setTitle("New title 20")
        .setSource("test")
        .setInstanceTypeId("12345")
        .generateMatchKey();
    batch.addRecordSet(new InventoryRecordSet(instance).getJson().put(PROCESSING,new JsonObject().put("localIdentifier","id" + 20)));

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Number of instance records for before PUT expected: 1" );
    Response response = batchUpsertByMatchKey(200,batch.getJson());

    JsonObject responseJson = new JsonObject(response.asString());
    JsonObject metrics = responseJson.getJsonObject("metrics");
    testContext.assertEquals(metrics.getJsonObject("INSTANCE").getJsonObject("CREATE").getInteger("COMPLETED"), 29,
        "Number of instance records created after PUT of batch of 29 expected: 29");
    testContext.assertEquals(metrics.getJsonObject("INSTANCE").getJsonObject("UPDATE").getInteger("COMPLETED"), 1,
        "Number of instance records updated after PUT of batch of 100 expected: 1");
    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 30,
        "Number of instance records after PUT expected: 30" );
  }
  @Test
  public void batchByMatchKeyWithMultipleLowLevelProblemsWillRespondWithMultipleErrors (TestContext testContext) {
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    int i = 1;

    for (; i <= 2; i++) {
      // 2 good record sets
      batch.addRecordSet(new JsonObject().put("instance",
              new InputInstance().setTitle("New title " + i).setSource("test").setHrid("in" + i).setInstanceTypeId("12345").generateMatchKey().getJson()).put("holdingsRecords",
              new JsonArray().add(new InputHoldingsRecord().setHrid("H" + i + "-1").setPermanentLocationId(
                  LOCATION_ID_1).getJson().put("items", new JsonArray().add(
                  new InputItem().setStatus(STATUS_UNKNOWN).setMaterialTypeId(MATERIAL_TYPE_TEXT).setHrid(
                      "I" + i + "-1-1").getJson()))).add(new InputHoldingsRecord().setHrid("H" + i + "-2").setPermanentLocationId(
                  LOCATION_ID_2).getJson().put("items", new JsonArray())))
          .put("processing", new JsonObject().put(CLIENTS_RECORD_IDENTIFIER, i)));
    }
    // Missing item.status, .materialType
    batch.addRecordSet(new JsonObject().put("instance",new InputInstance().setTitle("New title a").setSource("test").setHrid("in-a").setInstanceTypeId("12345").generateMatchKey().getJson()).put("holdingsRecords",
            new JsonArray().add(new InputHoldingsRecord().setHrid("H-a-1").setPermanentLocationId(
                LOCATION_ID_1).getJson().put("items", new JsonArray().add(new InputItem().setHrid("I-a-1-1").getJson()))).add(
                new InputHoldingsRecord().setHrid("H-a-2").setPermanentLocationId(
                    LOCATION_ID_2).getJson().put("items", new JsonArray())))
        .put(PROCESSING, new JsonObject()
            .put(CLIENTS_RECORD_IDENTIFIER, i)));
    i++;
    // Missing holdingsRecord.permanentLocationId
    batch.addRecordSet(new JsonObject().put("instance",
            new InputInstance().setTitle("New title b").setSource("test").setHrid("in-b").setInstanceTypeId("12345").generateMatchKey().getJson()).put("holdingsRecords",
            new JsonArray().add(new InputHoldingsRecord().setHrid("H-b-1").getJson().put("items",
                new JsonArray().add(new InputItem().setStatus(STATUS_UNKNOWN).setMaterialTypeId(
                    MATERIAL_TYPE_TEXT).setHrid("I-b-1-1").getJson()))).add(new InputHoldingsRecord().setHrid("H-b-2").setPermanentLocationId(
                LOCATION_ID_2).getJson().put("items", new JsonArray())))
        .put(PROCESSING, new JsonObject()
            .put(CLIENTS_RECORD_IDENTIFIER, i)));
    i++;
    for (; i <= 5; i++) {
      // 1 good record
      batch.addRecordSet(new JsonObject().put("instance",
              new InputInstance()
                  .setTitle("New title " + i)
                  .setSource("test")
                  .setHrid("in" + i)
                  .setInstanceTypeId("12345")
                  .generateMatchKey()
                  .getJson())
          .put("holdingsRecords", new JsonArray()
              .add(new InputHoldingsRecord()
                  .setHrid("H" + i + "-1")
                  .setPermanentLocationId(LOCATION_ID_1)
                  .getJson()
                  .put("items", new JsonArray()
                      .add(new InputItem()
                          .setStatus(STATUS_UNKNOWN)
                          .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                          .setHrid("I" + i + "-1-1")
                          .getJson())))
              .add(new InputHoldingsRecord()
                  .setHrid("H" + i + "-2")
                  .setPermanentLocationId(LOCATION_ID_2)
                  .getJson()
                  .put("items", new JsonArray())))
          .put(PROCESSING, new JsonObject().put(CLIENTS_RECORD_IDENTIFIER, "in" + i)));
    }
    Response response = batchUpsertByHrid(207, batch.getJson());
    JsonObject responseJson = new JsonObject(response.getBody().asString());
    JsonArray errors = responseJson.getJsonArray("errors", new JsonArray());
    testContext.assertTrue(( errors != null && !errors.isEmpty() && errors.size() == 2 ),
        "Response should contain two error reports.");
    boolean hasItemError = false;
    JsonObject itemErrorRequestJson = null;
    boolean hasHoldingsError = false;
    JsonObject holdingsErrorRequestJson = null;
    for (Object o : errors) {
      if ("ITEM".equals(( (JsonObject) o ).getString("entityType"))) {
        hasItemError = true;
        itemErrorRequestJson = ((JsonObject) o).getJsonObject("requestJson");
      }
      if ("HOLDINGS_RECORD".equals(( (JsonObject) o ).getString("entityType"))) {
        hasHoldingsError = true;
        holdingsErrorRequestJson = ((JsonObject) o).getJsonObject("requestJson");
      }
    }
    testContext.assertTrue(hasItemError && hasHoldingsError,
        "Response should have an Item error and a HoldingsRecord error.");
    JsonObject instances = getRecordsFromStorage(INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instances.getInteger("totalRecords"), 5,
        "The batch upsert should create five Instances");
    JsonObject holdings = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, null);
    testContext.assertEquals(holdings.getInteger("totalRecords"), 8,
        "The batch upsert should create eight holdings records");
    JsonObject items = getRecordsFromStorage(ITEM_STORAGE_PATH, null);
    testContext.assertEquals(items.getInteger("totalRecords"), 3,
        "The batch upsert should create three items");
    testContext.assertEquals(getMetric(responseJson, HOLDINGS_RECORD, CREATE, COMPLETED), 8,
        "Upsert metrics response should report [8] holdings records successfully created " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, HOLDINGS_RECORD, CREATE, FAILED), 2,
        "Upsert metrics response should report [2] holdings records creations failed " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, ITEM, CREATE, COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully created " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, ITEM, CREATE, FAILED), 1,
        "Upsert metrics response should report [1] item creation failed " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, ITEM, CREATE, SKIPPED), 1,
        "Upsert metrics response should report [1] item creation skipped " + responseJson.encodePrettily());
    testContext.assertNotNull(itemErrorRequestJson, "Response should contain item error with 'requestJson'");
    testContext.assertNotNull(holdingsErrorRequestJson, "Response should contain holdings record error with 'requestJson'");
    testContext.assertEquals(itemErrorRequestJson.getJsonObject("instance").getString("title"), "New title a","Request JSON with failed item should be reported in response with title 'New title a'");
    testContext.assertEquals(holdingsErrorRequestJson.getJsonObject("instance").getString("title"), "New title b","Request JSON with failed holdings should be reported in response 'New title b'");
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

    MatchKey matchKey = new MatchKey( instance.getJson() );
    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
        "Number of instance records for query by matchKey 'new_title___(etc)' before PUT expected: 0" );

    upsertByMatchKey(recordSet.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH, "matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 1,
        "Number of instance records for query by matchKey " + matchKey.getKey() + " after PUT expected: 1" );

  }

  @Test
  public void upsertByMatchKeyWillUpdateExistingInstance (TestContext testContext) {
    createInitialInstanceWithMatchKey();
    InputInstance instance = new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345");
    MatchKey matchKey = new MatchKey(instance.getJson());
    InventoryRecordSet inventoryRecordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH,"matchKey==\"" + matchKey.getKey() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Number of instance records for query by matchKey 'initial instance' before PUT expected: 1" );
    String instanceTypeIdBefore = instancesBeforePutJson.getJsonArray("instances")
        .getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdBefore,"123",
        "Expected instanceTypeId to be '123' before PUT");

    upsertByMatchKey(inventoryRecordSet.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(FakeFolioApis.INSTANCE_STORAGE_PATH,"matchKey==\"" + matchKey.getKey() + "\"");
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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson())))));

    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());
    JsonObject storedHoldings = getRecordsFromStorage(FakeFolioApis.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
        "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(FakeFolioApis.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
        "After upsert the total number of items should be [3] " + storedHoldings.encodePrettily() );

  }

  @Test
  public void upsertByMatchKeyWillFailToCreateItemIfMaterialTypeIsMissing(TestContext testContext) {
    String instanceHrid = "1";
    upsertByMatchKey(207, new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setBarcode("BC-003").getJson())))));

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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson())))));

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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, DELETE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully deleted " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, DELETE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully deleted " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, CREATE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully created " + upsertResponseJson.encodePrettily());

    getRecordsFromStorage(FakeFolioApis.ITEM_STORAGE_PATH,null).getJsonArray("items").stream()
        .forEach(item -> testContext.assertEquals(((JsonObject)item).getString("barcode"), "updated",
        "The barcode of all items should be updated to 'updated' after upsert of existing record set with holdings and items"));

  }

  @Test
  public void upsertsByMatchKeyWillCreateSharedInstanceFromTwoInstitutionsAndDeleteByOaiIdentifier (TestContext testContext) {

    final String identifierTypeId1 = "iti-001";
    final String identifierValue1 = "111";
    final String identifierTypeId2 = "iti-002";
    final String identifierValue2 = "222";

    JsonObject recordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Shared InputInstance")
                .setInstanceTypeId("12345")
                .setSource("source")
                .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId",identifierTypeId1).put("value",identifierValue1))).getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))))
        .put(PROCESSING, new JsonObject()
            .put("localIdentifier",identifierValue1));

    JsonObject upsertResponseJson1 = upsertByMatchKey(recordSet);
    String instanceId = upsertResponseJson1.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson1, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson1.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson1, ITEM, CREATE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully created " + upsertResponseJson1.encodePrettily());

    JsonObject storedHoldings = getRecordsFromStorage(FakeFolioApis.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
        "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(FakeFolioApis.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
        "After upsert the total number of items should be [3] " + storedItems.encodePrettily() );

    JsonObject instanceFromStorage = getRecordFromStorageById(FakeFolioApis.INSTANCE_STORAGE_PATH, instanceId);
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
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-004").getJson())
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-005").getJson())))
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_2).setCallNumber("test-cn-4").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-006").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson2, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
        "Metrics after second upsert should report additional [2] holdings records successfully created " + upsertResponseJson2.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson2, ITEM, CREATE , COMPLETED), 3,
        "Metrics after second upsert should report additional [3] items successfully created " + upsertResponseJson2.encodePrettily());

    storedHoldings = getRecordsFromStorage(FakeFolioApis.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 4,
        "After second upsert there should be [4] holdings records for instance " + instanceId + ": " + storedHoldings.encodePrettily() );
    storedItems = getRecordsFromStorage(FakeFolioApis.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 6,
        "After second upsert there should be [6] items " + storedItems.encodePrettily() );

    instanceFromStorage = getRecordFromStorageById(FakeFolioApis.INSTANCE_STORAGE_PATH, instanceId);
    testContext.assertEquals(instanceFromStorage.getJsonArray("identifiers").size(),2,
        "After second upsert of Shared InputInstance there should be [2] identifiers on the instance " + instanceFromStorage.encodePrettily());

    JsonObject deleteSignal = new JsonObject()
        .put("institutionId", INSTITUTION_ID_1)
        .put("oaiIdentifier","oai:"+identifierValue1)
        .put("identifierTypeId", identifierTypeId1);

    JsonObject deleteResponse = delete(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH,deleteSignal);
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully deleted " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , COMPLETED), 3,
        "Delete metrics response should report [3] items successfully deleted " + deleteResponse.encodePrettily());

    storedHoldings = getRecordsFromStorage(FakeFolioApis.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
        "After delete the number of holdings records left for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    storedItems = getRecordsFromStorage(FakeFolioApis.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
        "After delete the total number of items left should be [3] " + storedItems.encodePrettily() );

    instanceFromStorage = getRecordFromStorageById(FakeFolioApis.INSTANCE_STORAGE_PATH, instanceId);
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
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson())))));

    String instanceId = upsertResponseJson1.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson1, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson1.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson1, ITEM, CREATE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully created " + upsertResponseJson1.encodePrettily());

    JsonObject storedHoldings = getRecordsFromStorage(FakeFolioApis.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
        "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(FakeFolioApis.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
        "After upsert the total number of items should be [3] " + storedItems.encodePrettily() );

    JsonObject instanceFromStorage = getRecordFromStorageById(FakeFolioApis.INSTANCE_STORAGE_PATH, instanceId);
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
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-004").getJson())
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-005").getJson())))
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_2).setCallNumber("test-cn-4").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-006").getJson())))));

    testContext.assertEquals(getMetric(upsertResponseJson2, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
        "Metrics after second upsert should report additional [2] holdings records successfully created " + upsertResponseJson2.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson2, ITEM, CREATE , COMPLETED), 3,
        "Metrics after second upsert should report additional [3] items successfully created " + upsertResponseJson2.encodePrettily());

    storedHoldings = getRecordsFromStorage(FakeFolioApis.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 4,
        "After second upsert there should be [4] holdings records for instance " + instanceId + ": " + storedHoldings.encodePrettily() );
    storedItems = getRecordsFromStorage(FakeFolioApis.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 6,
        "After second upsert there should be [6] items " + storedItems.encodePrettily() );

    instanceFromStorage = getRecordFromStorageById(FakeFolioApis.INSTANCE_STORAGE_PATH, instanceId);
    testContext.assertEquals(instanceFromStorage.getJsonArray("identifiers").size(),2,
        "After second upsert of Shared InputInstance there should be [2] identifiers on the instance " + instanceFromStorage.encodePrettily());

    JsonObject deleteSignal = new JsonObject()
        .put("institutionId", INSTITUTION_ID_1)
        .put("localIdentifier",identifierValue1)
        .put("identifierTypeId", identifierTypeId1);
    JsonObject deleteResponse = delete(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH,deleteSignal);
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully deleted " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , COMPLETED), 3,
        "Delete metrics response should report [3] items successfully deleted " + deleteResponse.encodePrettily());

    storedHoldings = getRecordsFromStorage(FakeFolioApis.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
        "After delete the number of holdings records left for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    storedItems = getRecordsFromStorage(FakeFolioApis.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
        "After delete the total number of items left should be [3] " + storedItems.encodePrettily() );

    instanceFromStorage = getRecordFromStorageById(FakeFolioApis.INSTANCE_STORAGE_PATH, instanceId);
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
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))))
        .put(PROCESSING, new JsonObject()
            .put("identifierTypeId", identifierTypeId1)
            .put("localIdentifier", identifierValue1)));

    String instanceId = upsertResponseJson1.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJson1, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJson1.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson1, ITEM, CREATE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully created " + upsertResponseJson1.encodePrettily());

    JsonObject storedHoldings = getRecordsFromStorage(FakeFolioApis.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
        "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(FakeFolioApis.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
        "After upsert the total number of items should be [3] " + storedItems.encodePrettily() );

    JsonObject instanceFromStorage = getRecordFromStorageById(FakeFolioApis.INSTANCE_STORAGE_PATH, instanceId);
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
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-004").getJson())
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-005").getJson())))
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_2).setCallNumber("test-cn-4").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-006").getJson()))))
        .put(PROCESSING, new JsonObject()
            .put("identifierTypeId", identifierTypeId2)
            .put("localIdentifier", identifierValue2)));

    testContext.assertEquals(getMetric(upsertResponseJson2, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
        "Metrics after second upsert should report additional [2] holdings records successfully created " + upsertResponseJson2.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson2, ITEM, CREATE , COMPLETED), 3,
        "Metrics after second upsert should report additional [3] items successfully created " + upsertResponseJson2.encodePrettily());

    storedHoldings = getRecordsFromStorage(FakeFolioApis.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 4,
        "After second upsert there should be [4] holdings records for instance " + instanceId + ": " + storedHoldings.encodePrettily() );
    storedItems = getRecordsFromStorage(FakeFolioApis.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 6,
        "After second upsert there should be [6] items " + storedItems.encodePrettily() );

    instanceFromStorage = getRecordFromStorageById(FakeFolioApis.INSTANCE_STORAGE_PATH, instanceId);
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
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))))
        .put(PROCESSING, new JsonObject()
            .put("identifierTypeId", identifierTypeId1)
            .put("localIdentifier", identifierValue1)));

    String instanceIdSameRecordNewMatchKey = upsertResponseJsonForChangedMatchKey.getJsonObject("instance").getString("id");

    testContext.assertEquals(getMetric(upsertResponseJsonForChangedMatchKey, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully created " + upsertResponseJsonForChangedMatchKey.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJsonForChangedMatchKey, ITEM, CREATE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully created " + upsertResponseJsonForChangedMatchKey.encodePrettily());

    JsonObject storedHoldingsSameRecordNewInstance = getRecordsFromStorage(FakeFolioApis.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceIdSameRecordNewMatchKey + "\"");
    testContext.assertEquals(storedHoldingsSameRecordNewInstance.getInteger("totalRecords"), 2,
        "After third upsert, with 'Shared Input Instance', the number of holdings records for the new Instance " + instanceIdSameRecordNewMatchKey + " should be [2] " + storedHoldingsSameRecordNewInstance.encodePrettily() );
    JsonObject allStoredItems = getRecordsFromStorage(FakeFolioApis.ITEM_STORAGE_PATH, null);
    testContext.assertEquals(allStoredItems.getInteger("totalRecords"), 6,
        "After third upsert, with 'Shared Input Instance', the total number of items should be [6] " + allStoredItems.encodePrettily() );

    JsonObject newInstanceFromStorage = getRecordFromStorageById(FakeFolioApis.INSTANCE_STORAGE_PATH, instanceIdSameRecordNewMatchKey);
    testContext.assertEquals(newInstanceFromStorage.getJsonArray("identifiers").size(),1,
        "After third upsert, with 'Shared Input Instance', there should be [1] identifier on a new instance " + newInstanceFromStorage.encodePrettily());

    JsonObject oldInstanceFromStorage = getRecordFromStorageById( FakeFolioApis.INSTANCE_STORAGE_PATH, instanceId );
    testContext.assertEquals(oldInstanceFromStorage.getJsonArray("identifiers").size(),1,
        "After third upsert, with 'Shared Input Instance', there should be [1] identifier left on the previous instance " + oldInstanceFromStorage.encodePrettily());

    JsonObject previousInstanceStoredHoldings = getRecordsFromStorage(FakeFolioApis.HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(previousInstanceStoredHoldings.getInteger("totalRecords"), 2,
        "After third upsert, with 'Shared Input Instance',  there should be [2] holdings records left for the Instance with the previous match key " + instanceId + ": " + previousInstanceStoredHoldings.encodePrettily() );

  }

  @Test
  public void deleteByIdentifiersThatDoNotExistInSharedInventoryWillReturn404 (TestContext testContext) {
    delete(404, SHARED_INVENTORY_UPSERT_MATCHKEY_PATH,
        new JsonObject()
            .put("institutionId", INSTITUTION_ID_1)
            .put("localIdentifier","DOES_NOT_EXIST")
            .put("identifierTypeId", "DOES_NOT_EXIST"));
  }

  @Test
  public void testForcedLocationsGetRecordsFailure (TestContext testContext) {
    fakeFolioApis.locationStorage.failOnGetRecords = true;
    JsonObject inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Test forcedLocationsGetRecordsFailure").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId("UNKNOWN_LOCATION").setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId("UNKNOWN_LOCATION").setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))))
        .put("instanceRelations", new JsonObject()
            .put("parentInstances",new JsonArray())
            .put("childInstances", new JsonArray())
            .put("succeedingTitles", new JsonArray())
            .put("precedingTitles", new JsonArray()));
    upsertByMatchKey (500, inventoryRecordSet);

  }

  @Test
  public void testUpsertByMatchKeyWithEmptyLocationsTable (TestContext testContext) {
    RestAssured.given()
        .body("{}")
        .header("Content-type","application/json")
        .header(OKAPI_URL_HEADER)
        .delete(FakeFolioApis.LOCATION_STORAGE_PATH)
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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId("ANOTHER_UNKNOWN_LOCATION").setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))))
        .put("instanceRelations", new JsonObject()
            .put("parentInstances",new JsonArray())
            .put("childInstances", new JsonArray())
            .put("succeedingTitles", new JsonArray())
            .put("precedingTitles", new JsonArray()));
    upsertByMatchKey (500, inventoryRecordSet);

  }

  @Test
  public void testDeleteByIdentifierWithEmptyLocationsTable (TestContext testContext) {
    final String identifierTypeId1 = "iti-001";
    final String identifierValue1 = "111";

    upsertByMatchKey(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Shared InputInstance")
                .setInstanceTypeId("12345")
                .setSource("test")
                .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId",identifierTypeId1).put("value",identifierValue1))).getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson())))));

    JsonObject deleteSignal = new JsonObject()
        .put("institutionId", INSTITUTION_ID_1)
        .put("localIdentifier",identifierValue1)
        .put("identifierTypeId", identifierTypeId1);

    UpdatePlanSharedInventory.locationsToInstitutionsMap.clear();

    delete(200,SHARED_INVENTORY_UPSERT_MATCHKEY_PATH, deleteSignal);

  }

  @Test
  public void testDeleteByIdentifiersWithDeleteRequestFailure (TestContext testContext) {
    final String identifierTypeId1 = "iti-001";
    final String identifierValue1 = "111";

    upsertByMatchKey(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Shared InputInstance")
                .setInstanceTypeId("12345")
                .setSource("test")
                .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId",identifierTypeId1).put("value",identifierValue1))).getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson())))));

    fakeFolioApis.holdingsStorage.failOnDelete = true;

    JsonObject deleteSignal = new JsonObject()
        .put("institutionId", INSTITUTION_ID_1)
        .put("localIdentifier",identifierValue1)
        .put("identifierTypeId", identifierTypeId1);

    delete(207,SHARED_INVENTORY_UPSERT_MATCHKEY_PATH, deleteSignal);

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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson())))));


    fetchRecordSetFromUpsertSharedInventory( "1" );
    fetchRecordSetFromUpsertSharedInventory (newInstance.getJsonObject( "instance" ).getString( "id" ));
    getJsonObjectById( FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH, "2", 404 );

  }

  @Test
  public void cannotFetchFromUpsertSharedInventoryApiIfInstanceHasNoMatchKey (TestContext testContext) {
    String instanceHrid1 = "1";
    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("InputInstance 1")
                .setInstanceTypeId("12345")
                .setHrid(instanceHrid1)
                .setSource("test")
                .getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson())))));


    getJsonObjectById( FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH, "1", 400 );
  }

  @Test
  public void testInvalidApiPath (TestContext testContext) {
    JsonObject inventoryRecordSet = new JsonObject();
    inventoryRecordSet.put("instance", new InputInstance()
        .setTitle("Initial InputInstance").setInstanceTypeId("12345").getJson());
    putJsonObject(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH + "/invalid",inventoryRecordSet,404);
  }

  @Test
  public void testSendingNonInventoryRecordSetArrayToBatchApi (TestContext testContext) {
    batchUpsertByMatchKey(400, new JsonObject().put("unknownProperty", new JsonArray()));
  }

  @Test
  public void testSendingNonJson (TestContext testContext) {
    RestAssured.port = PORT_INVENTORY_UPDATE;

    RestAssured.given()
        .body(new JsonObject().toString())
        .header("Content-type","text/plain")
        .header(OKAPI_URL_HEADER)
        .put(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH)
        .then()
        .log().ifValidationFails()
        .statusCode(400).extract().response();

    RestAssured.given()
        .body(new JsonObject().toString())
        .header("Content-type","text/plain")
        .header(OKAPI_URL_HEADER)
        .delete(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH)
        .then()
        .log().ifValidationFails()
        .statusCode(400).extract().response();

  }


}
