package org.folio.inventoryupdate.importing.test.fakestorage.validators;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.importing.test.fakestorage.RecordStorage;
import org.folio.inventoryupdate.importing.test.InventoryUpdateTestBase;
import org.folio.inventoryupdate.importing.test.fakestorage.FakeFolioApisForImporting;
import org.folio.inventoryupdate.importing.test.fakestorage.entities.InputHoldingsRecord;
import org.folio.inventoryupdate.importing.test.fakestorage.entities.InputInstance;
import org.folio.inventoryupdate.importing.test.fakestorage.entities.InputInstanceRelationship;
import org.folio.inventoryupdate.importing.test.fakestorage.entities.InputInstanceTitleSuccession;

import static org.folio.inventoryupdate.importing.test.fakestorage.FakeFolioApisForImporting.*;


/**
 *
 * Validates that the fake storage API behaves as expected for testing the instance match service.
 */
public class StorageValidatorInstances  {

  protected void validateStorage(TestContext testContext) {

    validatePostAndGetById(testContext);
    validateGetByQueryAndPut(testContext);
    validateGetByIdList(testContext);
    validateCanDeleteInstanceById(testContext);
    cannotDeleteInstanceWithHoldings();
    cannotDeleteInstanceWithInstanceRelations();
    cannotDeleteInstanceWithTitleSuccession();
  }

  protected void validatePostAndGetById(TestContext testContext) {
    JsonObject responseOnPOST = FakeFolioApisForImporting.post(
            INSTANCE_STORAGE_PATH,
            new InputInstance().setTitle("New InputInstance").setInstanceTypeId("12345").setHrid("999999999").setSource("test").getJson());
    testContext.assertEquals(responseOnPOST.getString("title"), "New InputInstance");
    JsonObject responseOnGET = FakeFolioApisForImporting.getRecordById(INSTANCE_STORAGE_PATH, responseOnPOST.getString("id"));
    testContext.assertEquals(responseOnGET.getString("title"), "New InputInstance");
  }

  protected void validateGetByQueryAndPut(TestContext testContext) {
    JsonObject responseJson = FakeFolioApisForImporting.getRecordsByQuery(
            INSTANCE_STORAGE_PATH,
            "query="+ RecordStorage.encode("title==\"New InputInstance\""));
    testContext.assertEquals(
            responseJson.getInteger("totalRecords"), 1,"Number of " + RESULT_SET_INSTANCES + " expected: 1" );
    JsonObject existingRecord = responseJson.getJsonArray(RESULT_SET_INSTANCES).getJsonObject(0);
    existingRecord.put("instanceTypeId", "456");
    FakeFolioApisForImporting.put(INSTANCE_STORAGE_PATH, existingRecord);
    JsonObject inventoryRecord = FakeFolioApisForImporting.getRecordById(INSTANCE_STORAGE_PATH, existingRecord.getString("id"));
    testContext.assertEquals(inventoryRecord.getString("instanceTypeId"), "456");
  }

  protected void validateGetByIdList(TestContext testContext) {
    JsonObject responseJson = FakeFolioApisForImporting.getRecordsByQuery(
            INSTANCE_STORAGE_PATH,
            "query="+ RecordStorage.encode("(hrid==(\"10\" OR \"999999999\")"));
    testContext.assertEquals(
            responseJson.getInteger("totalRecords"), 1,"Number of " + RESULT_SET_INSTANCES + " expected: 1" );
  }

  protected void validateCanDeleteInstanceById (TestContext testContext) {
    JsonObject responseOnPOST = FakeFolioApisForImporting.post(
            INSTANCE_STORAGE_PATH,
            new InputInstance().setTitle("InputInstance to delete").setInstanceTypeId("12345").setSource("test").getJson());
    testContext.assertEquals(responseOnPOST.getString("title"), "InputInstance to delete");
    FakeFolioApisForImporting.delete(INSTANCE_STORAGE_PATH, responseOnPOST.getString("id"),200);
  }

  protected void cannotDeleteInstanceWithHoldings () {
    JsonObject responseOnPOST = FakeFolioApisForImporting.post(
            INSTANCE_STORAGE_PATH,
            new InputInstance().setTitle("InputInstance with holdings").setInstanceTypeId("12345").setSource("test").getJson(), 201);
    String instanceId = responseOnPOST.getString("id");
    FakeFolioApisForImporting.post(
            HOLDINGS_STORAGE_PATH,
            new InputHoldingsRecord().setCallNumber("Test holdings").setPermanentLocationId(InventoryUpdateTestBase.LOCATION_ID_1).setInstanceId(instanceId).getJson(), 201);
    FakeFolioApisForImporting.delete(INSTANCE_STORAGE_PATH, instanceId, 400);
  }

  protected void cannotDeleteInstanceWithInstanceRelations () {
    JsonObject responseOnPOSTChild = FakeFolioApisForImporting.post(
            INSTANCE_STORAGE_PATH,
            new InputInstance().setTitle("InputInstance with parent").setInstanceTypeId("12345").setSource("test").getJson(), 201);
    String childId = responseOnPOSTChild.getString("id");
    JsonObject responseOnPOSTParent = FakeFolioApisForImporting.post(
            INSTANCE_STORAGE_PATH,
            new InputInstance().setTitle("InputInstance with child").setInstanceTypeId("12345").setSource("test").getJson(), 201);
    String parentId = responseOnPOSTParent.getString("id");
    FakeFolioApisForImporting.post(
            INSTANCE_RELATIONSHIP_STORAGE_PATH,
            new InputInstanceRelationship()
                    .setSubInstanceId(childId)
                    .setSuperInstanceId(parentId).getJson(), 201);
    FakeFolioApisForImporting.delete(INSTANCE_STORAGE_PATH, parentId, 400);
  }

  protected void cannotDeleteInstanceWithTitleSuccession () {
    JsonObject responseOnPOSTSucceeding = FakeFolioApisForImporting.post(
            INSTANCE_STORAGE_PATH,
            new InputInstance().setTitle("Succeeding title").setInstanceTypeId("12345").setSource("test").getJson(), 201);
    String succeedingId = responseOnPOSTSucceeding.getString("id");
    JsonObject responseOnPOSTPreceding = FakeFolioApisForImporting.post(
            INSTANCE_STORAGE_PATH,
            new InputInstance().setTitle("Preceding title").setInstanceTypeId("12345").setSource("test").getJson(), 201);
    String precedingId = responseOnPOSTPreceding.getString("id");
    FakeFolioApisForImporting.post(
            PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH,
            new InputInstanceTitleSuccession()
                    .setSucceedingInstanceId(succeedingId)
                    .setPrecedingInstanceId(precedingId).getJson(), 201);
    FakeFolioApisForImporting.delete(INSTANCE_STORAGE_PATH, precedingId , 400);
  }

}
