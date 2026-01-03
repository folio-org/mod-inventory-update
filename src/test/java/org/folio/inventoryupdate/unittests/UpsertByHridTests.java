package org.folio.inventoryupdate.unittests;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import java.util.Arrays;
import org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting;
import org.folio.inventoryupdate.unittests.fakestorage.entities.BatchOfInventoryRecordSets;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputHoldingsRecord;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputInstance;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputInstanceRelationship;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputInstanceTitleSuccession;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputItem;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InventoryRecordSet;
import org.folio.inventoryupdate.updating.instructions.ProcessingInstructionsUpsert;
import org.folio.inventoryupdate.unittests.fakestorage.DeleteProcessingInstructions;
import org.folio.inventoryupdate.unittests.fakestorage.InputProcessingInstructions;
import org.junit.Test;

import static org.folio.inventoryupdate.unittests.fakestorage.FakeApis.post;
import static org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting.HOLDINGS_STORAGE_PATH;
import static org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting.INSTANCE_STORAGE_PATH;
import static org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting.ITEM_STORAGE_PATH;
import static org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting.ORDER_LINES_STORAGE_PATH;
import static org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting.RESULT_SET_HOLDINGS_RECORDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class UpsertByHridTests extends InventoryUpdateTestBase {

  @Test
  public void testHealth() {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    RestAssured.given()
        .get("/admin/health")
        .then().statusCode(200);
  }

  @Test
  public void batchUpsertByHridWillCreate200NewInstances (TestContext testContext) {
    createInitialInstanceWithHrid1();
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    for (int i=0; i<200; i++) {
      batch.addRecordSet(new InventoryRecordSet(new InputInstance()
          .setTitle("New title " + i)
          .setHrid("in"+i)
          .setSource("test")
          .setInstanceTypeId("12345")));
    }
    JsonObject instancesBeforePutJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Number of instance records before PUT expected: 1" );
    batchUpsertByHrid(batch.getJson());
    JsonObject instancesAfterPutJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, null);
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
    JsonObject instancesBeforePutJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Number of instance records for before PUT expected: 1" );
    batchUpsertByHrid(207,batch.getJson());
    JsonObject instancesAfterPutJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 100,
        "Number of instance records after PUT expected: 100" );
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
    JsonObject instancesBeforePutJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Number of instance records for before PUT expected: 1" );
    batchUpsertByHrid(207,batch.getJson());
    JsonObject instancesAfterPutJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 100,
        "Number of instance records after PUT expected: 100" );
  }

  @Test
  public void batchByHridWithRepeatHridsWillCreate29NewInstances (TestContext testContext) {
    createInitialInstanceWithHrid1();
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    for (int i=0; i<29; i++) {
      InputInstance instance = new InputInstance()
          .setTitle("New title " + i)
          .setSource("test")
          .setHrid("in" + i)
          .setInstanceTypeId("12345");
      batch.addRecordSet(new InventoryRecordSet(instance));
    }
    InputInstance instance = new InputInstance()
        .setTitle("New title 20 updated")
        .setSource("test")
        .setHrid("in20")
        .setInstanceTypeId("12345");
    batch.addRecordSet(new InventoryRecordSet(instance));

    JsonObject instancesBeforePutJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Number of instance records for before PUT expected: 1" );
    Response response = batchUpsertByHrid(200,batch.getJson());
    JsonObject responseJson = new JsonObject(response.asString());
    JsonObject metrics = responseJson.getJsonObject("metrics");
    testContext.assertEquals(metrics.getJsonObject("INSTANCE").getJsonObject("CREATE").getInteger("COMPLETED"), 29,
        "Number of instance records created after PUT of batch of 29 expected: 29");
    testContext.assertEquals(metrics.getJsonObject("INSTANCE").getJsonObject("UPDATE").getInteger("COMPLETED"), 1,
        "Number of instance records updated after PUT of batch of 39 expected: 1");
    JsonObject instancesAfterPutJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterPutJson.getInteger("totalRecords"), 30,
        "Number of instance records after PUT expected: 30" );
  }

  @Test
  public void batchByHRIDWithMultipleLowLevelProblemsWillRespondWithMultipleErrors (TestContext testContext) {
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    int i = 1;

    for (; i <= 2; i++) {
      // 2 good record sets
      batch.addRecordSet(new JsonObject().put("instance",
              new InputInstance().setTitle("New title " + i).setSource("test").setHrid("in" + i).setInstanceTypeId("12345").getJson()).put("holdingsRecords",
              new JsonArray().add(new InputHoldingsRecord().setHrid("H" + i + "-1").setPermanentLocationId(
                  LOCATION_ID_1).getJson().put("items", new JsonArray().add(
                  new InputItem().setStatus(STATUS_UNKNOWN).setMaterialTypeId(MATERIAL_TYPE_TEXT).setHrid(
                      "I" + i + "-1-1").getJson()))).add(new InputHoldingsRecord().setHrid("H" + i + "-2").setPermanentLocationId(
                  LOCATION_ID_2).getJson().put("items", new JsonArray())))
          .put("processing", new JsonObject().put(CLIENTS_RECORD_IDENTIFIER, i)));
    }
    // Missing item.status, .materialType
    batch.addRecordSet(new JsonObject().put("instance",
            new InputInstance().setTitle("New title a").setSource("test").setHrid("in-a").setInstanceTypeId("12345").getJson()).put("holdingsRecords",
            new JsonArray().add(new InputHoldingsRecord().setHrid("H-a-1").setPermanentLocationId(
                LOCATION_ID_1).getJson().put("items", new JsonArray().add(new InputItem().setHrid("I-a-1-1").getJson()))).add(
                new InputHoldingsRecord().setHrid("H-a-2").setPermanentLocationId(
                    LOCATION_ID_2).getJson().put("items", new JsonArray())))
        .put(PROCESSING, new JsonObject()
            .put(CLIENTS_RECORD_IDENTIFIER, i)));
    i++;
    // Missing holdingsRecord.permanentLocationId
    batch.addRecordSet(new JsonObject().put("instance",
            new InputInstance().setTitle("New title b").setSource("test").setHrid("in-b").setInstanceTypeId("12345").getJson()).put("holdingsRecords",
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
  public void upsertByHridWillCreateNewInstance(TestContext testContext) {
    createInitialInstanceWithHrid1();
    InputInstance instance = new InputInstance().setTitle("New title").setInstanceTypeId("12345").setHrid("2").setSource("test");
    InventoryRecordSet inventoryRecordSet = new InventoryRecordSet(instance);

    JsonObject instancesBeforePutJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, "hrid==\"" + instance.getHrid() + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 0,
        "Before upserting with new Instance, the number of Instances with that HRID should be [0]" );

    upsertByHrid(inventoryRecordSet.getJson());

    JsonObject instancesAfterPutJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH,"hrid==\"" + instance.getHrid() + "\"");
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

    JsonObject instancesBeforePutJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, "hrid==\"" + instanceHrid + "\"");
    testContext.assertEquals(instancesBeforePutJson.getInteger("totalRecords"), 1,
        "Before upsert of existing Instance, the number of Instances with that HRID should be [1]" );
    String instanceTypeIdBefore = instancesBeforePutJson
        .getJsonArray("instances").getJsonObject(0).getString("instanceTypeId");
    testContext.assertEquals(instanceTypeIdBefore,"123",
        "Before upsert of existing Instance, the instanceTypeId should be [123]");

    upsertByHrid(inventoryRecordSet);

    JsonObject instancesAfterUpsertJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, "hrid==\"" + instanceHrid + "\"");
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
    JsonObject storedHoldings = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
        "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(ITEM_STORAGE_PATH, null);
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

    upsertResponseJson.getJsonObject("instance").getString("id");
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
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setHrid("ITM-003").setBarcode("BC-003").getJson())))));

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

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, UPDATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully updated " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, UPDATE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully updated " + upsertResponseJson.encodePrettily());

    getRecordsFromStorage(ITEM_STORAGE_PATH,null).getJsonArray("items").stream()
        .forEach(item -> testContext.assertEquals(((JsonObject)item).getString("barcode"), "updated",
        "The barcode of all items should be updated to 'updated' after upsert of existing record set with holdings and items"));
  }

  @Test
  public void upsertByHridWillRetainExistingValuesForOmittedPropertiesIfAsked(TestContext testContext) {
    String instanceHrid = "1";
    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance()
                .setTitle("Initial InputInstance")
                .setInstanceTypeId("12345")
                .setHrid(instanceHrid)
                .setSource("test")
                .setEdition("retainMe").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1")
                .setAcquisitionFormat("original")
                .setShelvingTitle("retainMe").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001")
                        .setYearCaption("retainMe").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002")
                        .setYearCaption("retainMe").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2")
                .setAcquisitionFormat("original")
                .setShelvingTitle("retainMe").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setHrid("ITM-003").setBarcode("BC-003")
                        .setYearCaption("retainMe").getJson())))));


    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance()
                .setTitle("Initial InputInstance")
                .setInstanceTypeId("12345")
                .setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1")
                .setAcquisitionFormat("updated").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2")
                .setAcquisitionFormat("updated").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").getJson()))))
        .put(PROCESSING, new InputProcessingInstructions()
            .setRetainOmittedInstanceProperties(true)
            .setRetainOmittedHoldingsRecordProperties(true)
            .setRetainOmittedItemProperties(true)
            .setInstancePropertiesToRetain("instanceTypeId")
            .setHoldingsRecordPropertiesToRetain("shelvingTitle","someOtherProp")
            .setItemPropertiesToRetain("someProp","yearCaption").getJson()));

    getRecordsFromStorage(INSTANCE_STORAGE_PATH,null).getJsonArray("instances").stream()
        .forEach(instance -> testContext.assertEquals(((JsonObject)instance).getJsonArray("editions").getString(0), "retainMe",
        "The editions should be retained as 'retainMe' after upsert of existing record set"));

    getRecordsFromStorage(ITEM_STORAGE_PATH,null).getJsonArray("items").stream().forEach(item -> {
      testContext.assertEquals(((JsonObject)item).getString("barcode"), "updated",
          "The barcode of all items should be updated to 'updated' after upsert of existing record set with holdings and items");
      testContext.assertEquals(((JsonObject)item).getJsonArray("yearCaption").getString(0), "retainMe",
          "The yearCaption of all items should be retained as 'retainMe' after upsert of existing record set with holdings and items");
    });

    getRecordsFromStorage(HOLDINGS_STORAGE_PATH,null).getJsonArray("holdingsRecords").stream().forEach(holdingsRecord -> {
      testContext.assertEquals(((JsonObject)holdingsRecord).getString("acquisitionFormat"), "updated",
          "The acquisitionFormat of all holdings records should be updated to 'updated' after upsert of existing record set with holdings and items");
      testContext.assertEquals(((JsonObject)holdingsRecord).getString("shelvingTitle"), "retainMe",
          "The shelvingTitle of all holdings records should be retained as 'retainMe' after upsert of existing record set with holdings and items");
    });

  }

  @Test
  public void upsertByHridWillRetainExistingValuesOfSpecifiedProperties (TestContext testContext) {
    String instanceHrid = "1";
    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance()
                .setTitle("Initial InputInstance")
                .setInstanceTypeId("12345")
                .setHrid(instanceHrid)
                .setSource("test")
                .setEdition("retainMe").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1")
                .setAcquisitionFormat("original")
                .setShelvingTitle("retainMe").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001")
                        .setYearCaption("retainMe").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002")
                        .setYearCaption("retainMe").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2")
                .setAcquisitionFormat("original")
                .setShelvingTitle("retainMe").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setHrid("ITM-003").setBarcode("BC-003")
                        .setYearCaption("retainMe").getJson()))))
        .put(PROCESSING, new InputProcessingInstructions()
            .setHoldingsRecordPropertiesToRetain("shelvingTitle","someOtherProp")
            .setItemPropertiesToRetain("someProp","yearCaption").getJson()));

    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance()
                .setTitle("Initial InputInstance")
                .setInstanceTypeId("12345")
                .setHrid(instanceHrid)
                .setSource("updated")
                .setEdition("updated").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1")
                .setAcquisitionFormat("updated")
                .setShelvingTitle("updated").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated")
                        .setYearCaption("updated").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated")
                        .setYearCaption("updated").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2")
                .setAcquisitionFormat("updated")
                .setShelvingTitle("updated").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated")
                        .setYearCaption("updated").getJson()))))
        .put(PROCESSING, new InputProcessingInstructions()
            .setInstancePropertiesToRetain("editions")
            .setHoldingsRecordPropertiesToRetain("shelvingTitle","someOtherProp")
            .setItemPropertiesToRetain("someProp","yearCaption").getJson()));

    getRecordsFromStorage(INSTANCE_STORAGE_PATH,null).getJsonArray("instances").stream().forEach(instance -> {
      testContext.assertEquals(((JsonObject)instance).getString("source"), "updated",
          "The Instance.source should be updated to 'updated' after upsert of existing record set");
      testContext.assertEquals(((JsonObject)instance).getJsonArray("editions").getString(0), "retainMe",
          "The Instance.edition should be retained as 'retainMe' after upsert of existing record set");
    });

    getRecordsFromStorage(ITEM_STORAGE_PATH,null).getJsonArray("items").stream().forEach(item -> {
      testContext.assertEquals(((JsonObject)item).getString("barcode"), "updated",
          "The barcode of all items should be updated to 'updated' after upsert of existing record set with holdings and items");
      testContext.assertEquals(((JsonObject)item).getJsonArray("yearCaption").getString(0), "retainMe",
          "The yearCaption of all items should be retained as 'retainMe' after upsert of existing record set with holdings and items");
    });

    getRecordsFromStorage(HOLDINGS_STORAGE_PATH,null).getJsonArray("holdingsRecords").stream().forEach(holdingsRecord -> {
      testContext.assertEquals(((JsonObject)holdingsRecord).getString("acquisitionFormat"), "updated",
          "The acquisitionFormat of all holdings records should be updated to 'updated' after upsert of existing record set with holdings and items");
      testContext.assertEquals(((JsonObject)holdingsRecord).getString("shelvingTitle"), "retainMe",
          "The shelvingTitle of all holdings records should be retained as 'retainMe' after upsert of existing record set with holdings and items");
    });
  }

  @Test
  public void upsertByHridWillRetainExistingItemByPatternMatching(TestContext testContext) {
    String instanceHrid = "1";
    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance()
                .setTitle("Initial InputInstance")
                .setInstanceTypeId("12345")
                .setHrid(instanceHrid)
                .setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1")
                .setAcquisitionFormat("original").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2")
                .setAcquisitionFormat("original").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    JsonObject holdingsRecords = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "hrid==\"HOL-001\"");
    String holdingsRecordId = holdingsRecords.getJsonArray("holdingsRecords").getJsonObject(0).getString("id");
    fakeFolioApis.itemStorage.insert(new InputItem()
        .setHoldingsRecordId(holdingsRecordId)
        .setStatus(STATUS_UNKNOWN)
        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
        .setHrid("1234")
        .setBarcode("ext-create"));

    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance()
                .setTitle("Initial InputInstance")
                .setInstanceTypeId("12345")
                .setHrid(instanceHrid)
                .setSource("updated")
                .setEdition("updated").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1")
                .setAcquisitionFormat("updated")
                .setShelvingTitle("updated").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated")
                        .setYearCaption("updated").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2")
                .setAcquisitionFormat("updated")
                .setShelvingTitle("updated").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated")
                        .setYearCaption("updated").getJson()))))
        .put(PROCESSING, new InputProcessingInstructions()
            .setItemRecordRetentionCriterion("hrid", "\\d+").getJson())); // all digits

    int count1234withMatch = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"1234\"").getJsonArray("items").size();
    int countITEM002 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"ITEM-002\"").getJsonArray("items").size();
    testContext.assertEquals(count1234withMatch, 1, "Item '1234' should still exist with matching criteria");
    testContext.assertEquals(countITEM002, 0, "Item 'ITEM-002' should be gone");

    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance()
                .setTitle("Initial InputInstance")
                .setInstanceTypeId("12345")
                .setHrid(instanceHrid)
                .setSource("updated")
                .setEdition("updated").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2")
                .setAcquisitionFormat("updated")
                .setShelvingTitle("updated").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated")
                        .setYearCaption("updated").getJson()))))
        .put(PROCESSING, new InputProcessingInstructions()
            .setItemRecordRetentionCriterion("hrid", "\\d+").getJson())); // all digits

    int secondCount1234withMatch = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"1234\"").getJsonArray("items").size();
    int countHoldingsHOL001 = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "hrid==\"HOL-001\"").getJsonArray("holdingsRecords").size();
    testContext.assertEquals(secondCount1234withMatch, 1, "Item '1234' should still exist with matching criteria, even though holdings record was up for deletion");
    testContext.assertEquals(countHoldingsHOL001, 1, "Omitted holdings record 'HOL-001' should still exist due to delete protected item '1234'");

    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance()
                .setTitle("Initial InputInstance")
                .setInstanceTypeId("12345")
                .setHrid(instanceHrid)
                .setSource("updated")
                .setEdition("updated").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2")
                .setAcquisitionFormat("updated")
                .setShelvingTitle("updated").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated")
                        .setYearCaption("updated").getJson()))))
        .put(PROCESSING, new InputProcessingInstructions()
            .setItemRecordRetentionCriterion("hrid", "\\D+").getJson())); // all non-digits

    int count1234withNonMatch = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"1234\"").getJsonArray("items").size();
    testContext.assertEquals(count1234withNonMatch, 0, "Item '1234' should be gone when criteria didn't match");
    int secondCountHoldingsHOL001 = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "hrid==\"HOL-001\"").getJsonArray("holdingsRecords").size();
    testContext.assertEquals(secondCountHoldingsHOL001, 0, "Holdings HOL-001 should be gone when omitted, sole item not protected by match");
  }

  @Test
  public void upsertByHridWillSetStatCodeForRetainedHoldingsItems(TestContext testContext) {
    String instanceHrid = "1";
    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance()
                .setTitle("Initial InputInstance")
                .setInstanceTypeId("12345")
                .setHrid(instanceHrid)
                .setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("5678").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1")
                .setAcquisitionFormat("original").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())
                    .add(new InputItem().setHrid("1234")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("1234").getJson())
                    .add(new InputItem().setHrid("ITM-004")
                        .setStatus(STATUS_CHECKED_OUT)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-004").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2")
                .setAcquisitionFormat("original").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setHrid("ITM-003").setBarcode("BC-003").getJson())))));

    JsonObject upsertBody = new JsonObject()
        .put("instance",
            new InputInstance()
                .setTitle("Initial InputInstance")
                .setInstanceTypeId("12345")
                .setHrid(instanceHrid)
                .setSource("updated")
                .setEdition("updated").getJson())
        .put("holdingsRecords", new JsonArray())
        .put(PROCESSING, new InputProcessingInstructions()
            .setItemRecordRetentionCriterion("hrid", "\\d+")
            .setItemStatisticalCoding(new JsonArray()
                .add(new JsonObject().put("if","deleteSkipped").put("becauseOf","ITEM_PATTERN_MATCH").put("setCode","123"))
                .add(new JsonObject().put("if","deleteSkipped").put("becauseOf","ITEM_STATUS").put("setCode","789")))
            .setHoldingsRecordRetentionCriterion("hrid", "\\d+" )
            .setHoldingsRecordStatisticalCoding(new JsonArray()
                .add(new JsonObject().put("if","deleteSkipped").put("becauseOf","ITEM_PATTERN_MATCH").put("setCode","456"))
                .add(new JsonObject().put("if","deleteSkipped").put("becauseOf","ITEM_STATUS").put("setCode","789"))
                .add(new JsonObject().put("if","deleteSkipped").put("becauseOf","HOLDINGS_RECORD_PATTERN_MATCH").put("setCode","1011"))).getJson());


    upsertByHrid(upsertBody);

    JsonObject item1234 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==1234").getJsonArray("items").getJsonObject(0);
    JsonObject itemITM004 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==ITM-004").getJsonArray("items").getJsonObject(0);
    JsonObject firstHoldingsRecord = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, null).getJsonArray("holdingsRecords").getJsonObject(0);
    testContext.assertTrue(item1234.getJsonArray("statisticalCodeIds").contains("123"),"Statistical code 123 is set on item for ITEM_PATTERN_MATCH");
    testContext.assertTrue(itemITM004.getJsonArray("statisticalCodeIds").contains("789"),"Statistical code 789 is set on item for ITEM_STATUS");
    testContext.assertTrue(firstHoldingsRecord.getJsonArray("statisticalCodeIds").contains("456"),"Statistical code 456 is set on holdings record for ITEM_PATTERN_MATCH");
    testContext.assertTrue(firstHoldingsRecord.getJsonArray("statisticalCodeIds").contains("789"),"Statistical code 789 is set on holdings record for ITEM_STATUS");
    testContext.assertTrue(firstHoldingsRecord.getJsonArray("statisticalCodeIds").contains("1011"),"Statistical code 1011 is set on holdings record for HOLDINGS_RECORD_PATTERN_MATCH");

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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").setStatus("On order").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").setStatus("Unknown").getJson())))
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

    upsertResponseJson = upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").setStatus("Available").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").setStatus("Available").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").getJson()))))
        .put(PROCESSING, new InputProcessingInstructions()
            .setItemStatusPolicy(ProcessingInstructionsUpsert.ITEM_STATUS_POLICY_OVERWRITE)
            .setListOfStatuses("On order").getJson()));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, UPDATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully updated " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, UPDATE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully updated " + upsertResponseJson.encodePrettily());

    JsonArray item001 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"ITM-001\"").getJsonArray("items");
    testContext.assertEquals(item001.getJsonObject(0).getJsonObject("status").getString("name"),
        "Available",
        "Status for item ITM-001 should have been updated from 'On order' to 'Available");

    JsonArray item002 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"ITM-002\"").getJsonArray("items");
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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").setStatus("On order").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").setStatus("Unknown").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").setStatus("Checked out").getJson())))));

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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").setStatus("Available").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").setStatus("Available").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").setStatus("Available").getJson()))))
        .put(PROCESSING, new InputProcessingInstructions()
            .setItemStatusPolicy(ProcessingInstructionsUpsert.ITEM_STATUS_POLICY_OVERWRITE)
            .setListOfStatuses("On order", "Unknown").getJson()));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, UPDATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully updated " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, UPDATE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully updated " + upsertResponseJson.encodePrettily());

    JsonArray item001 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"ITM-001\"").getJsonArray("items");
    testContext.assertEquals(item001.getJsonObject(0).getJsonObject("status").getString("name"),
        "Available",
        "Status for item ITM-001 should have been updated to 'Available' from 'On order'");

    JsonArray item002 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"ITM-002\"").getJsonArray("items");
    testContext.assertEquals(item002.getJsonObject(0).getJsonObject("status").getString("name"),
        "Available",
        "Status for item ITM-002 should have been updated to 'Available' from 'Unknown'");

    JsonArray item003 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"ITM-003\"").getJsonArray("items");
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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").setStatus("On order").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").setStatus("Unknown").getJson())))
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

    upsertResponseJson = upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").setStatus("Available").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").setStatus("Available").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").getJson()))))
        .put(PROCESSING, new InputProcessingInstructions()
            .setItemStatusPolicy(ProcessingInstructionsUpsert.ITEM_STATUS_POLICY_RETAIN).getJson()));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, UPDATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully updated " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, UPDATE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully updated " + upsertResponseJson.encodePrettily());

    JsonArray item001 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"ITM-001\"").getJsonArray("items");
    testContext.assertEquals(item001.getJsonObject(0).getJsonObject("status").getString("name"),
        "On order",
        "Status for item ITM-001 should have been retained as 'On order'");

    JsonArray item002 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"ITM-002\"").getJsonArray("items");
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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").setStatus("On order").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").setStatus("Unknown").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))))
        .put(PROCESSING, new JsonObject()));

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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").setStatus("Available").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").setStatus("Available").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("updated-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("updated").getJson()))))
        .put(PROCESSING, new InputProcessingInstructions()
            .setItemStatusPolicy(ProcessingInstructionsUpsert.ITEM_STATUS_POLICY_OVERWRITE).getJson()));

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, UPDATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully updated " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, UPDATE , COMPLETED), 3,
        "Upsert metrics response should report [3] items successfully updated " + upsertResponseJson.encodePrettily());

    JsonArray item001 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"ITM-001\"").getJsonArray("items");
    testContext.assertEquals(item001.getJsonObject(0).getJsonObject("status").getString("name"),
        "Available",
        "Status for item ITM-001 should have been overwritten to 'Avaliable' from 'On order'");

    JsonArray item002 = getRecordsFromStorage(ITEM_STORAGE_PATH, "hrid==\"ITM-002\"").getJsonArray("items");
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
                        .setBarcode("BC-003").getJson()))));

    JsonObject upsertResponseJson = upsertByHrid(inventoryRecordSet);
    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    // Leave out one holdings record
    inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))));

    upsertResponseJson =  upsertByHrid(inventoryRecordSet);

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, DELETE , COMPLETED), 1,
        "After upsert with one holdings record removed from set, metrics should report [1] holdings record successfully deleted " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, DELETE , COMPLETED), 2,
        "After upsert with one holdings record removed from set, metrics should report [2] items successfully deleted " + upsertResponseJson.encodePrettily());
    JsonObject holdingsAfterUpsertJson = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(holdingsAfterUpsertJson.getInteger("totalRecords"), 1,
        "After upsert with one holdings record removed from set, number of holdings records left for the Instance should be [1] " + holdingsAfterUpsertJson.encodePrettily());
    JsonObject itemsAfterUpsertJson = getRecordsFromStorage(ITEM_STORAGE_PATH, null);
    testContext.assertEquals(itemsAfterUpsertJson.getInteger("totalRecords"), 1,
        "After upsert with one holdings record removed from set, the total number of item records should be [1] " + itemsAfterUpsertJson.encodePrettily() );
  }

  @Test
  public void upsertByHridWillNotDeleteOmittedItemIfCurrentlyCirculating(TestContext testContext) {
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_CHECKED_OUT)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))));

    JsonObject upsertResponseJson = upsertByHrid(inventoryRecordSet);
    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    // Leave out one holdings record
    inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray())));

    upsertResponseJson =  upsertByHrid(inventoryRecordSet);

    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, DELETE, SKIPPED), 1,
        "After upsert with still circulating item omitted, metrics should report [1] item deletion skipped " + upsertResponseJson.encodePrettily());

    JsonObject holdingsAfterUpsertJson = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(holdingsAfterUpsertJson.getInteger("totalRecords"), 1,
        "After upsert with still circulating item omitted, number of holdings records left for the Instance should still be [1] " + holdingsAfterUpsertJson.encodePrettily());
    JsonObject itemsAfterUpsertJson = getRecordsFromStorage(ITEM_STORAGE_PATH, null);
    testContext.assertEquals(itemsAfterUpsertJson.getInteger("totalRecords"), 1,
        "After upsert with still circulating item removed, the total number of item records should still be [1] " + itemsAfterUpsertJson.encodePrettily() );
  }

  @Test
  public void upsertByHridWillNotDeleteOmittedHoldingsWithCirculatingItem(TestContext testContext) {
    String instanceHrid = "1";
    JsonObject inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_CHECKED_OUT)
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
                        .setBarcode("BC-003").getJson()))));

    JsonObject upsertResponseJson = upsertByHrid(inventoryRecordSet);
    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    // Leave out one holdings record
    inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))));

    upsertResponseJson =  upsertByHrid(inventoryRecordSet);

    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, DELETE, SKIPPED), 1,
        "After upsert with one holdings record removed from set but with a circulating item, metrics should report [1] holdings record deletion skipped " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, DELETE, COMPLETED), 1,
        "After upsert with one holdings record removed from set but with a circulating item, metrics should report [1] items successfully deleted " + upsertResponseJson.encodePrettily());
    testContext.assertEquals(getMetric(upsertResponseJson, ITEM, DELETE, SKIPPED), 1,
        "After upsert with one holdings record removed from set but with a circulating item, metrics should report [1] item deletion skipped " + upsertResponseJson.encodePrettily());

    JsonObject holdingsAfterUpsertJson = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(holdingsAfterUpsertJson.getInteger("totalRecords"), 2,
        "After upsert with one holdings record removed from set but with a circulating item, number of holdings records left for the Instance should still be [2] " + holdingsAfterUpsertJson.encodePrettily());
    JsonObject itemsAfterUpsertJson = getRecordsFromStorage(ITEM_STORAGE_PATH, null);
    testContext.assertEquals(itemsAfterUpsertJson.getInteger("totalRecords"), 2,
        "After upsert with one holdings record removed from set but with a circulating item, the total number of item records should be [2] " + itemsAfterUpsertJson.encodePrettily() );
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
                        .setBarcode("BC-003").getJson()))));

    JsonObject upsertResponseJson = upsertByHrid(inventoryRecordSet);
    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");
    JsonObject itemsAfterUpsert0Json = getRecordsFromStorage(ITEM_STORAGE_PATH, null);

    // Upsert should not delete holdings/items when there's no holdings array in the request document
    upsertByHrid(
        new JsonObject()
            .put("instance",
                new InputInstance().setTitle("Updated InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson()));

    JsonObject holdingsAfterUpsert1Json = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    getRecordsFromStorage(ITEM_STORAGE_PATH, null);

    // Upsert should delete any attached holdings/items when there's an empty holdings array in the request
    upsertByHrid(
        new JsonObject()
            .put("instance",
                new InputInstance().setTitle("2nd Updated InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
            .put("holdingsRecords", new JsonArray()));

    JsonObject holdingsAfterUpsert2Json = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    getRecordsFromStorage(ITEM_STORAGE_PATH, null);

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
    JsonObject relationshipsAfterUpsertJson = getRecordsFromStorage(FakeFolioApisForImporting.INSTANCE_RELATIONSHIP_STORAGE_PATH, null);
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

    JsonObject relationshipsAfterGrandParent = getRecordsFromStorage(FakeFolioApisForImporting.INSTANCE_RELATIONSHIP_STORAGE_PATH, null);

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
    JsonObject relationshipsAfterUpsertJson = getRecordsFromStorage(FakeFolioApisForImporting.INSTANCE_RELATIONSHIP_STORAGE_PATH, null);
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

    JsonObject relationshipsAfterGrandParent = getRecordsFromStorage(FakeFolioApisForImporting.INSTANCE_RELATIONSHIP_STORAGE_PATH, null);

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

    JsonObject titleSuccessions = getRecordsFromStorage(FakeFolioApisForImporting.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH,null);
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

    JsonObject titleSuccessions = getRecordsFromStorage(FakeFolioApisForImporting.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH,null);
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

    JsonObject titleSuccessions = getRecordsFromStorage(FakeFolioApisForImporting.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH,null);
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
    JsonObject instancesAfterUpsertJson = getRecordsFromStorage(INSTANCE_STORAGE_PATH, null);
    testContext.assertEquals(instancesAfterUpsertJson.getInteger("totalRecords"), 2,
        "After upsert with provisional instance the total number of instances should be [2] " + instancesAfterUpsertJson.encodePrettily() );
  }

  @Test
  public void upsertByHridWillCreateJustOneProvisionalInstanceIfTwoRelationsRequireTheSame (TestContext testContext) {
    String childHrid1 = "002-1";
    String childHrid2 = "002-2";
    String parentHrid = "001";

    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();

    batch.addRecordSet(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Child InputInstance 1").setInstanceTypeId("12345").setHrid(childHrid1).setSource("test").getJson())
        .put("instanceRelations", new JsonObject()
            .put("parentInstances", new JsonArray()
                .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parentHrid)
                    .setProvisionalInstance(
                        new InputInstance()
                            .setTitle("Provisional Instance")
                            .setSource("MARC")
                            .setInstanceTypeId("12345").getJson()).getJson()))));

    batch.addRecordSet(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Child InputInstance 2").setInstanceTypeId("12345").setHrid(childHrid2).setSource("test").getJson())
        .put("instanceRelations", new JsonObject()
            .put("parentInstances", new JsonArray()
                .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parentHrid)
                    .setProvisionalInstance(
                        new InputInstance()
                            .setTitle("Provisional Instance")
                            .setSource("MARC")
                            .setInstanceTypeId("12345").getJson()).getJson()))));

    assertEquals(200,batchUpsertByHrid(200, batch.getJson()).getStatusCode());

  }

  @Test
  public void upsertByHridWillNotCreateProvisionalInstanceIfTheRegularInstanceIsCreatedInBatch(TestContext testContext) {
    String childHrid1 = "002-1";
    String parentHrid = "001";

    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();

    batch.addRecordSet(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Child InputInstance 2").setInstanceTypeId("12345").setHrid(childHrid1).setSource("test").getJson())
        .put("instanceRelations", new JsonObject()
            .put("parentInstances", new JsonArray()
                .add(new InputInstanceRelationship().setInstanceIdentifierHrid(parentHrid)
                    .setProvisionalInstance(
                        new InputInstance()
                            .setTitle("Provisional Instance")
                            .setSource("MARC")
                            .setInstanceTypeId("12345").getJson()).getJson()))));

    batch.addRecordSet(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Parent InputInstance 1").setInstanceTypeId("12345").setHrid(parentHrid).setSource("test").getJson()));

    JsonObject response = new JsonObject(batchUpsertByHrid(200, batch.getJson()).asString());
    testContext.assertEquals(getMetric(response, INSTANCE, CREATE, COMPLETED), 2,
        "Upsert metrics response should report [2] instances successfully created " + response.encodePrettily());
    testContext.assertEquals(getMetric(response, INSTANCE_RELATIONSHIP, CREATE, COMPLETED), 1,
        "Upsert metrics should report [1] relationship successfully created");
    testContext.assertEquals(getMetric(response, PROVISIONAL_INSTANCE, CREATE, COMPLETED), -1,
        "Upsert metrics should not report any provisional instance created");

  }

  @Test
  public void upsertByHridWillGracefullyFailToCreateRelationWithoutProvisionalInstance (TestContext testContext) {
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
                        .setBarcode("BC-003").getJson()))))
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

    JsonObject storedHoldings = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
        "After upsert the number of holdings records for instance " + instanceId + " should be [2] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
        "After upsert the total number of items should be [3] " + storedItems.encodePrettily() );
    JsonObject storedRelations = getRecordsFromStorage(FakeFolioApisForImporting.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH, null);
    testContext.assertEquals(storedRelations.getInteger("totalRecords"), 1,
        "After upsert the total number of relations should be [1] " + storedRelations.encodePrettily() );

    JsonObject deleteSignal = new JsonObject()
        .put("hrid",instanceHrid);

    JsonObject deleteResponse = delete(INVENTORY_UPSERT_HRID_PATH,deleteSignal);
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records successfully deleted " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , COMPLETED), 3,
        "Delete metrics response should report [3] items successfully deleted " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, INSTANCE_TITLE_SUCCESSION, DELETE , COMPLETED), 1,
        "Delete metrics response should report [1] relation successfully deleted " + deleteResponse.encodePrettily());

    storedHoldings = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 0,
        "After delete the number of holdings records for instance " + instanceId + " should be [0] " + storedHoldings.encodePrettily() );
    storedItems = getRecordsFromStorage(ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 0,
        "After delete the total number of items should be [3] " + storedItems.encodePrettily() );
    storedRelations = getRecordsFromStorage(FakeFolioApisForImporting.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH, null);
    testContext.assertEquals(storedRelations.getInteger("totalRecords"), 0,
        "After delete the total number of relations should be [0] " + storedRelations.encodePrettily() );

  }

  @Test
  public void deleteByHridWillNotDeleteProtectedItems (TestContext testContext) {
    String instanceHrid = "IN-001";
    JsonObject upsertBody =  new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("EXT-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))));

    JsonObject upsertResponseJson = upsertByHrid(upsertBody);
    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");
    JsonObject deleteSignal = new JsonObject()
        .put("hrid", instanceHrid)
        .put("processing", new DeleteProcessingInstructions()
            .setItemBlockDeletionCriterion("hrid", "EXT.*")
            .getJson());

    JsonObject storedInstances = getRecordsFromStorage(INSTANCE_STORAGE_PATH,null);
    testContext.assertEquals(storedInstances.getInteger("totalRecords"), 1,
        "Before delete of instance with protected item the number of instance records with HRID " + instanceHrid + " should be [1] " + storedInstances.encodePrettily() );

    JsonObject deleteResponse = delete(INVENTORY_UPSERT_HRID_PATH, deleteSignal);
    // Checking metrics
    testContext.assertEquals(getMetric(deleteResponse, INSTANCE, DELETE, SKIPPED), 1,
        "Upsert metrics response should report [1] instance deletion skipped " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE , COMPLETED), 1,
        "Upsert metrics response should report [1] holdings record successfully deleted " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE , SKIPPED), 1,
        "Upsert metrics response should report [1] holdings records deletion skipped " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , COMPLETED), 2,
        "Delete metrics response should report [2] items successfully deleted " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , SKIPPED), 1,
        "Delete metrics response should report [1] item deletion skipped " + deleteResponse.encodePrettily());

    storedInstances = getRecordsFromStorage(INSTANCE_STORAGE_PATH,"hrid==\"" + instanceHrid +"\"");
    testContext.assertEquals(storedInstances.getInteger("totalRecords"), 1,
        "After delete of instance with protected item the number of instance records with HRID " + instanceHrid + " should be [1] " + storedInstances.encodePrettily() );
    JsonObject storedHoldings = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 1,
        "After delete of instance with protected item the number of holdings records for instance " + instanceHrid + " should be [1] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 1,
        "After delete the total number of items should be [2] " + storedItems.encodePrettily() );
  }

  @Test
  public void deleteByHridWillNotDeleteProtectedHoldingsRecords (TestContext testContext) {
    String instanceHrid = "IN-001";
    JsonObject upsertBody =  new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("EXT-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("EXT-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))));


    JsonObject upsertResponseJson = upsertByHrid(upsertBody);
    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");
    JsonObject deleteSignal = new JsonObject()
        .put("hrid", instanceHrid)
        .put("processing", new  DeleteProcessingInstructions()
            .setHoldingsBlockDeletionCriterion("hrid", "EXT.*")
            .getJson());

    JsonObject storedInstances = getRecordsFromStorage(INSTANCE_STORAGE_PATH,null);
    testContext.assertEquals(storedInstances.getInteger("totalRecords"), 1,
        "Before delete of instance with protected item the number of instance records with HRID " + instanceHrid + " should be [1] " + storedInstances.encodePrettily() );

    JsonObject deleteResponse = delete(INVENTORY_UPSERT_HRID_PATH, deleteSignal);
    // Checking metrics
    testContext.assertEquals(getMetric(deleteResponse, INSTANCE, DELETE, SKIPPED), 1,
        "Upsert metrics response should report [1] instance deletion skipped " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE , COMPLETED), 1,
        "Upsert metrics response should report [1] holdings record successfully deleted " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE , SKIPPED), 1,
        "Upsert metrics response should report [1] holdings records deletion skipped " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , COMPLETED), 3,
        "Delete metrics response should report [2] items successfully deleted " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , SKIPPED), 0,
        "Delete metrics response should report [1] item deletion skipped " + deleteResponse.encodePrettily());

    storedInstances = getRecordsFromStorage(INSTANCE_STORAGE_PATH,"hrid==\"" + instanceHrid +"\"");
    testContext.assertEquals(storedInstances.getInteger("totalRecords"), 1,
        "After delete of instance with protected item the number of instance records with HRID " + instanceHrid + " should be [1] " + storedInstances.encodePrettily() );
    JsonObject storedHoldings = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 1,
        "After delete of instance with protected item the number of holdings records for instance " + instanceHrid + " should be [1] " + storedHoldings.encodePrettily() );
    JsonObject storedItems = getRecordsFromStorage(ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 0,
        "After delete the total number of items should be [0] " + storedItems.encodePrettily() );

  }

  @Test
  public void deleteByHridWillNotDeleteInstanceReferencedByOrder (TestContext testContext) {
    String instanceHrid = "IN-001";
    JsonObject upsertBody =  new JsonObject()
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
                        .setBarcode("BC-003").getJson()))));
    JsonObject upsertResponseJson = upsertByHrid(upsertBody);
    String instanceId = upsertResponseJson.getJsonObject("instance").getString("id");

    JsonObject poLine = new JsonObject("{" +
        "\"purchaseOrderId\": \"3b198b70-cf8e-4075-9e93-ebf2c76e60c2\", " +
        "\"instanceId\": \"" + instanceId +  "\", " +
        "\"orderFormat\": \"Other\", " +
        "\"source\": \"User\", " +
        "\"titleOrPackage\": \"Initital InputInstance\" }");
    post(ORDER_LINES_STORAGE_PATH, poLine);
    JsonObject deleteSignal = new JsonObject().put("hrid", "IN-001");
    JsonObject deleteResponse = delete(INVENTORY_UPSERT_HRID_PATH, deleteSignal);

    testContext.assertEquals(getMetric(deleteResponse, INSTANCE, DELETE, SKIPPED), 1,
        "Upsert metrics response should report [1] instance deletion skipped " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdings records deletions completed " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , COMPLETED), 3,
        "Delete metrics response should report [3] item deletions completed " + deleteResponse.encodePrettily());

  }

  @Test
  public void deleteByHridWillNotDeleteInstanceWithCirculatingItems (TestContext testContext) {
    String instanceHrid = "IN-001";
    JsonObject upsertBody =  new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_CHECKED_OUT)
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
                        .setBarcode("BC-003").getJson()))));
    JsonObject upsertResponseJson = upsertByHrid(upsertBody);
    upsertResponseJson.getJsonObject("instance").getString("id");

    JsonObject deleteSignal = new JsonObject().put("hrid", "IN-001");
    JsonObject deleteResponse = delete(INVENTORY_UPSERT_HRID_PATH, deleteSignal);

    testContext.assertEquals(getMetric(deleteResponse, INSTANCE, DELETE, SKIPPED), 1,
        "Upsert metrics response should report [1] instance deletion skipped " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE, COMPLETED), 1,
        "Upsert metrics response should report [1] holdings records deletions completed " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, HOLDINGS_RECORD, DELETE, SKIPPED), 1,
        "Upsert metrics response should report [1] holdings records deletions completed " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , COMPLETED), 2,
        "Delete metrics response should report [3] item deletions completed " + deleteResponse.encodePrettily());
    testContext.assertEquals(getMetric(deleteResponse, ITEM, DELETE , SKIPPED), 1,
        "Delete metrics response should report [3] item deletions completed " + deleteResponse.encodePrettily());
  }

  @Test
  public void deleteByHridSetsStatCodeOnItemNotInstanceForItemStatus (TestContext testContext) {
    String instanceHrid = "IN-001";
    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_CHECKED_OUT)
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

    delete(INVENTORY_UPSERT_HRID_PATH,
        new JsonObject().put("hrid", "IN-001")
            .put(PROCESSING,
                new JsonObject().put("item",
                    new JsonObject().put("statisticalCoding",
                        new JsonArray().add(new JsonObject()
                            .put("if","deleteSkipped").put("becauseOf","ITEM_STATUS").put("setCode","123"))))));

    JsonObject items = getRecordsFromStorage(ITEM_STORAGE_PATH,null);
    testContext.assertTrue(items.getJsonArray("items").getJsonObject(0)
        .getJsonArray("statisticalCodeIds").contains("123"), "Instance has a statistical code '123' for delete skipped due to item status");
    JsonObject instances = getRecordsFromStorage(INSTANCE_STORAGE_PATH,null);
    testContext.assertTrue(!instances.getJsonArray("instances").getJsonObject(0)
        .containsKey("statisticalCodeIds"), "Instance has no statistical codes for delete skipped due to item status");
  }

  @Test
  public void deleteByHridSetsStatCodeOnInstanceForItemPatternMatch (TestContext testContext) {
    String instanceHrid = "IN-001";
    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_CHECKED_OUT)
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

    JsonObject deleteSignal =
        new JsonObject()
            .put("hrid", "IN-001")
            .put(PROCESSING,
                new JsonObject()
                    .put("instance",
                        new JsonObject().put(STATISTICAL_CODING,
                            new JsonArray()
                                .add(new JsonObject().put("if","deleteSkipped").put("becauseOf","ITEM_STATUS").put("setCode","456"))
                                .add(new JsonObject().put("if","deleteSkipped").put("becauseOf","ITEM_PATTERN_MATCH").put("setCode","789"))))
                    .put("item",
                        new JsonObject().put("blockDeletion",
                            new JsonObject().put("ifField","hrid").put("matchesPattern", "ITM.*"))));

    delete(INVENTORY_UPSERT_HRID_PATH, deleteSignal);
    JsonObject items = getRecordsFromStorage(ITEM_STORAGE_PATH,null);
    testContext.assertTrue(!items.getJsonArray("items").getJsonObject(0).containsKey("statisticalCodeIds"), "Item has no statistical codes");
    JsonObject instances = getRecordsFromStorage(INSTANCE_STORAGE_PATH,null);
    JsonArray statisticalCodes = instances.getJsonArray("instances").getJsonObject(0)
        .getJsonArray("statisticalCodeIds");
    testContext.assertTrue(statisticalCodes!=null && !statisticalCodes.isEmpty(), "The instance has statistical codes set");
    testContext.assertTrue(instances.getJsonArray("instances").getJsonObject(0)
        .getJsonArray("statisticalCodeIds").contains("456"), "Instance has a statistical code '456' for delete skipped due to item status");
    testContext.assertTrue(instances.getJsonArray("instances").getJsonObject(0)
        .getJsonArray("statisticalCodeIds").contains("789"), "Instance has a statistical code '789' for delete skipped due to item pattern match");
  }

  @Test
  public void deleteByHridSetsStatCodeOnInstanceForOrdersReference (TestContext testContext) {
    String instanceHrid = "IN-001";
    JsonObject upsertResponseJson = upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_CHECKED_OUT)
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

    JsonObject poLine = new JsonObject("{" +
        "\"purchaseOrderId\": \"3b198b70-cf8e-4075-9e93-ebf2c76e60c2\", " +
        "\"instanceId\": \"" + instanceId +  "\", " +
        "\"orderFormat\": \"Other\", " +
        "\"source\": \"User\", " +
        "\"titleOrPackage\": \"Initital InputInstance\" }");
    post(ORDER_LINES_STORAGE_PATH, poLine);

    JsonObject deleteSignal =
        new JsonObject()
            .put("hrid", "IN-001")
            .put(PROCESSING,
                new JsonObject()
                    .put("instance",
                        new JsonObject().put(STATISTICAL_CODING,
                            new JsonArray()
                                .add(new JsonObject().put("if","deleteSkipped").put("becauseOf","PO_LINE_REFERENCE").put("setCode","123"))))
                    .put("item",
                        new JsonObject().put("blockDeletion",
                            new JsonObject().put("ifField","hrid").put("matchesPattern", "ITM.*"))));

    delete(INVENTORY_UPSERT_HRID_PATH, deleteSignal);
    JsonObject items = getRecordsFromStorage(ITEM_STORAGE_PATH,null);
    testContext.assertTrue(!items.getJsonArray("items").getJsonObject(0).containsKey("statisticalCodeIds"), "Item has no statistical codes");
    JsonObject instances = getRecordsFromStorage(INSTANCE_STORAGE_PATH,null);

    JsonArray statisticalCodes = instances.getJsonArray("instances").getJsonObject(0)
        .getJsonArray("statisticalCodeIds");
    testContext.assertTrue(statisticalCodes!=null && !statisticalCodes.isEmpty(), "The instance has statistical codes set");
    testContext.assertTrue(instances.getJsonArray("instances").getJsonObject(0)
        .getJsonArray("statisticalCodeIds").contains("123"), "Instance has a statistical code '123' for delete skipped due to PO line reference");
    testContext.assertEquals(instances.getJsonArray("instances").getJsonObject(0)
        .getJsonArray("statisticalCodeIds").size(), 1, "Instance has just one statistical code");
  }

  @Test
  public void deleteByHridSetsStatCodeOnInstanceItemForItemStatus (TestContext testContext) {
    String instanceHrid = "IN-001";
    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_CHECKED_OUT)
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

    delete(INVENTORY_UPSERT_HRID_PATH,
        new JsonObject()
            .put("hrid", "IN-001")
            .put(PROCESSING,
                new JsonObject()
                    .put("item",
                        new JsonObject().put(STATISTICAL_CODING,
                            new JsonArray().add(new JsonObject()
                                .put("if","deleteSkipped").put("becauseOf","ITEM_STATUS").put("setCode","123"))))
                    .put("instance",
                        new JsonObject().put(STATISTICAL_CODING,
                            new JsonArray().add(new JsonObject()
                                .put("if","deleteSkipped").put("becauseOf","ITEM_STATUS").put("setCode","456"))))));

    JsonObject items = getRecordsFromStorage(ITEM_STORAGE_PATH,null);
    JsonArray statisticalCodes = items.getJsonArray("items").getJsonObject(0)
        .getJsonArray("statisticalCodeIds");
    testContext.assertTrue(statisticalCodes!=null && !statisticalCodes.isEmpty(), "The instance has statistical codes set");
    testContext.assertTrue(items.getJsonArray("items").getJsonObject(0)
        .getJsonArray("statisticalCodeIds").contains("123"), "Instance has a statistical code '123' for delete skipped due to item status");
    JsonObject instances = getRecordsFromStorage(INSTANCE_STORAGE_PATH,null);
    testContext.assertTrue(instances.getJsonArray("instances").getJsonObject(0)
        .getJsonArray("statisticalCodeIds").contains("456"), "Instance has a statistical code '456' for delete skipped due to item status");
  }



  @Test
  public void deleteSignalByHridForNonExistingInstanceWillReturn404 (TestContext testContext) {
    JsonObject deleteSignal = new JsonObject().put("hrid","DOES_NOT_EXIST");
    assertEquals(404, delete(404, INVENTORY_UPSERT_HRID_PATH,deleteSignal).getStatusCode());
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

    String instanceId1 = firstResponse.getJsonObject("instance").getString("id");
    JsonObject storedHoldings = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId1 + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
        "After upsert the number of holdings records for instance " + instanceId1 + " should be [2] " + storedHoldings.encodePrettily() );

    String instanceHrid2 = "2";
    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("InputInstance 2X").setInstanceTypeId("12345").setHrid(instanceHrid2).setSource("test").getJson())
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

    storedHoldings = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "instanceId==\"" + instanceId1 + "\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 0,
        "After move of holdings the number of holdings records for instance " + instanceId1 + " should be [0] " + storedHoldings.encodePrettily() );

    storedHoldings = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "hrid==\"HOL-001\" OR hrid==\"HOL-002\"");
    testContext.assertEquals(storedHoldings.getInteger("totalRecords"), 2,
        "After move of holdings they should still exist, count should be [2] " + storedHoldings.encodePrettily() );

    JsonObject thirdResponse = upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("InputInstance 1X").setInstanceTypeId("12345").setHrid(instanceHrid1).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-003").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-3").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson())))));

    testContext.assertEquals(getMetric(thirdResponse, HOLDINGS_RECORD, CREATE , COMPLETED), 1,
        "Third update should report [1] holdings record successfully created  " + thirdResponse.encodePrettily());
    testContext.assertEquals(getMetric(thirdResponse, ITEM, UPDATE , COMPLETED), 1,
        "Third update should report [1] item successfully updated (moved)   " + thirdResponse.encodePrettily());

    JsonObject storedItems = getRecordsFromStorage(ITEM_STORAGE_PATH, null);
    testContext.assertEquals(storedItems.getInteger("totalRecords"), 3,
        "After two moves of holdings/items there should still be [3] items total in storage " + storedItems.encodePrettily() );

    upsertByHrid(new JsonObject()
        .put("instance",
            new InputInstance().setTitle("InputInstance 2X").setInstanceTypeId("12345").setHrid(instanceHrid2).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))));

    JsonObject storedHoldings002 = getRecordsFromStorage(HOLDINGS_STORAGE_PATH, "hrid==\"HOL-002\"");
    JsonObject holdings002 = storedHoldings002.getJsonArray(RESULT_SET_HOLDINGS_RECORDS).getJsonObject(0);
    JsonObject storedItemsHol002 = getRecordsFromStorage(ITEM_STORAGE_PATH, "holdingsRecordId==\""+holdings002.getString("id")+"\"");
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
                        .setBarcode("BC-003").getJson()))))
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
    assertEquals(404, getJsonObjectById( FETCH_INVENTORY_RECORD_SETS_ID_PATH, "2", 404 ).getStatusCode());
  }



  @Test
  public void upsertByHridWithMissingInstanceHridWillBeRejected (TestContext testContext) {
    upsertByHrid(422, new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setHrid("ITM-002").setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson())))));

    assertEquals(422, upsertByHrid(422, new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("").setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))))).getStatusCode());

  }


  @Test
  public void upsertByHridWithMissingItemHridWillBeRejected (TestContext testContext) {
    String instanceHrid = "1";
    assertEquals(422, upsertByHrid(422, new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))))).getStatusCode());

  }

  @Test
  public void upsertByHridWithMissingHoldingsHridWillBeRejected (TestContext testContext) {
    String instanceHrid = "1";
    assertEquals(422, upsertByHrid(422, new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid(instanceHrid).setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson()))))).getStatusCode());

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
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())
                    .add(new InputItem().setHrid("ITM-002")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-002").getJson())))
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId("BAD_LOCATION").setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson())))));
    JsonObject upsertResponseJson = new JsonObject(upsertResponse.getBody().asString());
    testContext.assertTrue(upsertResponseJson.containsKey("errors"),
        "After upsert with holdings record with bad location id, the response should contain error reports");
    testContext.assertEquals(getMetric(upsertResponseJson, HOLDINGS_RECORD, CREATE , FAILED), 2,
        "Upsert metrics response should report [2] holdings records create failure for wrong location ID on one of them (whole batch fails) " + upsertResponseJson.encodePrettily());
  }

  @Test
  public void upsertWithRecurringInstanceHridWillSwitchToRecordByRecord (TestContext testContext) {
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    String hrid="001";
    batch.addRecordSet(new JsonObject()
        .put("instance",
            new InputInstance().setHrid(hrid)
                .setTitle("InputInstance v1").setInstanceTypeId("12345").setSource("test").getJson()));
    batch.addRecordSet(new JsonObject()
        .put("instance",
            new InputInstance().setHrid("001")
                .setTitle("InputInstance v2").setInstanceTypeId("12345").setSource("test").getJson()));

    Response upsertResponse = batchUpsertByHrid(200, batch.getJson());
    JsonObject responseJson = new JsonObject(upsertResponse.getBody().asString());
    testContext.assertEquals(getMetric(responseJson, INSTANCE, CREATE , COMPLETED), 1,
        "Upsert metrics response should report [1] instance create completed " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, INSTANCE, UPDATE , COMPLETED), 1,
        "Upsert metrics response should report [1] instance update completed " + responseJson.encodePrettily());
  }

  @Test
  public void upsertWithRecurringHoldingsHridWillSwitchToRecordByRecord (TestContext testContext) {
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    batch.addRecordSet(new JsonObject()
        .put("instance",
            new InputInstance().setHrid("001")
                .setTitle("InputInstance v1").setInstanceTypeId("12345").setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord()
                .setHrid("H001")
                .setPermanentLocationId(LOCATION_ID_1)
                .getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setHrid("I001")
                        .getJson())))));
    batch.addRecordSet(new JsonObject()
        .put("instance",
            new InputInstance().setHrid("002")
                .setTitle("InputInstance v2").setInstanceTypeId("12345").setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord()
                .setHrid("H001")
                .setPermanentLocationId(LOCATION_ID_1)
                .getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setHrid("I002")
                        .getJson())))));

    Response upsertResponse = batchUpsertByHrid(200, batch.getJson());
    JsonObject responseJson = new JsonObject(upsertResponse.getBody().asString());
    testContext.assertEquals(getMetric(responseJson, INSTANCE, CREATE , COMPLETED), 2,
        "Upsert metrics response should report [2] instance creates completed " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 1,
        "Upsert metrics response should report [1] holdingsRecord create completed " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, HOLDINGS_RECORD, UPDATE , COMPLETED), 1,
        "Upsert metrics response should report [1] holdingsRecord update completed " + responseJson.encodePrettily());

  }

  @Test
  public void upsertWithRecurringItemHridWillSwitchToRecordByRecord (TestContext testContext) {
    BatchOfInventoryRecordSets batch = new BatchOfInventoryRecordSets();
    batch.addRecordSet(new JsonObject()
        .put("instance",
            new InputInstance().setHrid("001")
                .setTitle("InputInstance v1").setInstanceTypeId("12345").setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord()
                .setHrid("H001")
                .setPermanentLocationId(LOCATION_ID_1)
                .getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setHrid("I001")
                        .getJson())))));
    batch.addRecordSet(new JsonObject()
        .put("instance",
            new InputInstance().setHrid("002")
                .setTitle("InputInstance v2").setInstanceTypeId("12345").setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord()
                .setHrid("H002")
                .setPermanentLocationId(LOCATION_ID_1)
                .getJson()
                .put("items", new JsonArray()
                    .add(new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setHrid("I001")
                        .getJson())))));

    Response upsertResponse = batchUpsertByHrid(200, batch.getJson());
    JsonObject responseJson = new JsonObject(upsertResponse.getBody().asString());
    testContext.assertEquals(getMetric(responseJson, INSTANCE, CREATE , COMPLETED), 2,
        "Upsert metrics response should report [2] instance creates completed " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, HOLDINGS_RECORD, CREATE , COMPLETED), 2,
        "Upsert metrics response should report [2] holdingsRecord creates completed " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, ITEM, CREATE , COMPLETED), 1,
        "Upsert metrics response should report [1] item update completed " + responseJson.encodePrettily());
    testContext.assertEquals(getMetric(responseJson, ITEM, UPDATE , COMPLETED), 1,
        "Upsert metrics response should report [1] item update completed " + responseJson.encodePrettily());

  }


  @Test
  public void upsertByHridWillReturnErrorResponseOnMissingInstanceInRequestBody (TestContext testContext) {
    assertEquals(400, upsertByHrid(400, new JsonObject().put("invalid", "No Instance here")).getStatusCode());
  }

  @Test
  public void testSendingNonJson (TestContext testContext) {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    RestAssured.given()
        .body("bad request body")
        .header("Content-type","application/json")
        .header(OKAPI_URL_HEADER)
        .header(OKAPI_TENANT_HEADER)
        .put(INVENTORY_UPSERT_HRID_PATH)
        .then()
        .log().ifValidationFails()
        .statusCode(400);

    RestAssured.given()
        .header("Content-type","application/json")
        .header(OKAPI_URL_HEADER)
        .header(OKAPI_TENANT_HEADER)
        .put(INVENTORY_UPSERT_HRID_PATH)
        .then()
        .log().ifValidationFails()
        .statusCode(400);

    RestAssured.given()
        .body(new JsonObject().toString())
        .header("Content-type","text/plain")
        .header(OKAPI_URL_HEADER)
        .header(OKAPI_TENANT_HEADER)
        .put(INVENTORY_UPSERT_HRID_PATH)
        .then()
        .log().ifValidationFails()
        .statusCode(400).extract().response();

    RestAssured.given()
        .body(new JsonObject().toString())
        .header("Content-type","text/plain")
        .header(OKAPI_URL_HEADER)
        .header(OKAPI_TENANT_HEADER)
        .delete(INVENTORY_UPSERT_HRID_PATH)
        .then()
        .log().ifValidationFails()
        .statusCode(400);
  }

  @Test
  public void testSendingNonInventoryRecordSetArrayToBatchApi (TestContext testContext) {
    assertEquals(400, batchUpsertByHrid(400,new JsonObject().put("unknownProperty", new JsonArray())).getStatusCode());
  }

  @Test
  public void testForcedItemCreateFailure (TestContext testContext) {
    fakeFolioApis.itemStorage.failOnCreate = true;
    Response response = upsertByHrid(207,new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
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

    JsonObject responseJson = new JsonObject(response.getBody().asString());
    testContext.assertEquals(getMetric(responseJson, ITEM, CREATE , FAILED), 3,
        "Upsert metrics response should report [3] item record create failures (forced) " + responseJson.encodePrettily());

  }

  @Test
  public void plainTextErrorFromStorageIsWrappedAsJson (TestContext testContext) {
    fakeFolioApis.itemStorage.failOnCreate = true;
    Response response = upsertByHrid(207,new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-001").getJson())))));

    JsonObject responseJson = new JsonObject(response.getBody().asString());
    try {
      testContext.assertTrue(responseJson.containsKey("errors")
          && !responseJson.getJsonArray("errors").isEmpty()
          && responseJson.getJsonArray("errors").getJsonObject(0).getJsonObject("message") != null);
    } catch (Exception e) {
      fail("Error message should be JsonObject, but:   " + e.getMessage());
    }
  }

  @Test
  public void testForcedHoldingsCreateFailure (TestContext testContext) {
    fakeFolioApis.holdingsStorage.failOnCreate = true;
    Response response = upsertByHrid(207,new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
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

    JsonObject responseJson = new JsonObject(response.getBody().asString());

    testContext.assertEquals(getMetric(responseJson, HOLDINGS_RECORD, CREATE , FAILED), 2,
        "Upsert metrics response should report [2] holdings record create failures (forced) " + responseJson.encodePrettily());

    testContext.assertEquals(getMetric(responseJson, ITEM, CREATE , SKIPPED), 3,
        "Upsert metrics response should report [3] item record creates skipped " + responseJson.encodePrettily());

  }

  @Test
  public void testForcedItemUpdateFailure (TestContext testContext) {
    fakeFolioApis.itemStorage.failOnUpdate = true;
    JsonObject inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
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
                        .setBarcode("BC-003").getJson()))));
    upsertByHrid (inventoryRecordSet);
    Response response = upsertByHrid(207,inventoryRecordSet);
    JsonObject responseJson = new JsonObject(response.getBody().asString());

    testContext.assertEquals(getMetric(responseJson, ITEM, UPDATE , FAILED), 3,
        "Upsert metrics response should report [3] item record update failures (forced) " + responseJson.encodePrettily());

  }

  @Test
  public void testForcedHoldingsUpdateFailure (TestContext testContext) {
    fakeFolioApis.holdingsStorage.failOnUpdate = true;
    JsonObject inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
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
                        .setBarcode("BC-003").getJson()))));
    upsertByHrid (inventoryRecordSet);
    Response response = upsertByHrid(207,inventoryRecordSet);

    JsonObject responseJson = new JsonObject(response.getBody().asString());

    testContext.assertEquals(getMetric(responseJson, HOLDINGS_RECORD, UPDATE , FAILED), 2,
        "Upsert metrics response should report [2] holdings record update failures (forced) " + responseJson.encodePrettily());

  }

  @Test
  public void testForcedItemDeleteFailure (TestContext testContext) {
    fakeFolioApis.itemStorage.failOnDelete = true;
    upsertByHrid (new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
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

    Response response = upsertByHrid(207,new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-001").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-1").getJson()
                .put("items", new JsonArray()
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

    JsonObject responseJson = new JsonObject(response.getBody().asString());

    testContext.assertEquals(getMetric(responseJson, ITEM, DELETE , FAILED), 1,
        "Upsert metrics response should report [1] item delete failure (forced) " + responseJson.encodePrettily());

  }

  @Test
  public void testForcedHoldingsDeleteFailure (TestContext testContext) {
    fakeFolioApis.holdingsStorage.failOnDelete = true;
    upsertByHrid (new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
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

    Response response = upsertByHrid(207,new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
        .put("holdingsRecords", new JsonArray()
            .add(new InputHoldingsRecord().setHrid("HOL-002").setPermanentLocationId(LOCATION_ID_1).setCallNumber("test-cn-2").getJson()
                .put("items", new JsonArray()
                    .add(new InputItem().setHrid("ITM-003")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("BC-003").getJson())))));

    JsonObject responseJson = new JsonObject(response.getBody().asString());

    testContext.assertEquals(getMetric(responseJson, HOLDINGS_RECORD, DELETE , FAILED), 1,
        "Upsert metrics response should report [1] holdings record delete failure (forced) " + responseJson.encodePrettily());

    testContext.assertEquals(getMetric(responseJson, ITEM, DELETE, COMPLETED), 2,
        "Upsert metrics response should report [2] items successfully deleted " + responseJson.encodePrettily());

  }

  @Test
  public void testForcedItemGetRecordsFailure (TestContext testContext) {
    fakeFolioApis.itemStorage.failOnGetRecords = true;
    JsonObject inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
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
                        .setBarcode("BC-003").getJson()))));
    assertEquals(500, upsertByHrid (500,inventoryRecordSet).getStatusCode());

  }

  @Test
  public void testForcedHoldingsGetRecordsFailure (TestContext testContext) {
    fakeFolioApis.holdingsStorage.failOnGetRecords = true;
    JsonObject inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
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
                        .setBarcode("BC-003").getJson()))));
    assertEquals(500,  upsertByHrid (500,inventoryRecordSet).getStatusCode());

  }

  @Test
  public void testForcedInstanceGetRecordsFailure (TestContext testContext) {
    fakeFolioApis.instanceStorage.failOnGetRecords = true;
    JsonObject inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Initial InputInstance").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
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
                        .setBarcode("BC-003").getJson()))));
    assertEquals(500, upsertByHrid (500,inventoryRecordSet).getStatusCode());

  }

  @Test
  public void testForcedInstanceRelationshipsGetRecordsFailure (TestContext testContext) {
    fakeFolioApis.instanceRelationshipStorage.failOnGetRecords = true;
    fakeFolioApis.precedingSucceedingStorage.failOnGetRecords = true;
    JsonObject inventoryRecordSet = new JsonObject()
        .put("instance",
            new InputInstance().setTitle("Test forcedInstanceRelationshipGetRecordsFailure").setInstanceTypeId("12345").setHrid("001").setSource("test").getJson())
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
                        .setBarcode("BC-003").getJson()))))
        .put("instanceRelations", new JsonObject()
            .put("parentInstances",new JsonArray())
            .put("childInstances", new JsonArray())
            .put("succeedingTitles", new JsonArray())
            .put("precedingTitles", new JsonArray()));
    upsertByHrid (inventoryRecordSet);
    assertEquals(500, upsertByHrid (500,inventoryRecordSet).getStatusCode());

  }

}
