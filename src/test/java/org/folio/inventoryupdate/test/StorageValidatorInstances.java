package org.folio.inventoryupdate.test;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage;
import org.folio.inventoryupdate.test.fakestorage.RecordStorage;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestHoldingsRecord;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestInstance;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestInstanceRelationship;
import org.junit.Test;

import static org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage.*;


/**
 *
 * Validates that the fake storage API behaves as expected for testing the instance match service.
 */
public class StorageValidatorInstances  {

  protected void validateStorage(TestContext testContext) {
    validatePostAndGetById(testContext);
    validateGetByQueryAndPut(testContext);
    validateCanDeleteInstanceById(testContext);
    cannotDeleteInstanceWithHoldings(testContext);
    cannotDeleteInstanceWithInstanceRelations(testContext);
  }

  protected void validatePostAndGetById(TestContext testContext) {
    JsonObject responseOnPOST = FakeInventoryStorage.post(
            INSTANCE_STORAGE_PATH,
            new TestInstance().setTitle("New TestInstance").setInstanceTypeId("12345").getJson());
    testContext.assertEquals(responseOnPOST.getString("title"), "New TestInstance");
    JsonObject responseOnGET = FakeInventoryStorage.getRecordById(INSTANCE_STORAGE_PATH, responseOnPOST.getString("id"));
    testContext.assertEquals(responseOnGET.getString("title"), "New TestInstance");
  }

  protected void validateGetByQueryAndPut(TestContext testContext) {
    JsonObject responseJson = FakeInventoryStorage.getRecordsByQuery(
            INSTANCE_STORAGE_PATH,
            "query="+ RecordStorage.encode("title==\"New TestInstance\""));
    testContext.assertEquals(
            responseJson.getInteger("totalRecords"), 1,"Number of " + RESULT_SET_INSTANCES + " expected: 1" );
    JsonObject existingRecord = responseJson.getJsonArray(RESULT_SET_INSTANCES).getJsonObject(0);
    existingRecord.put("instanceTypeId", "456");
    FakeInventoryStorage.put(INSTANCE_STORAGE_PATH, existingRecord);
    JsonObject record = FakeInventoryStorage.getRecordById(INSTANCE_STORAGE_PATH, existingRecord.getString("id"));
    testContext.assertEquals(record.getString("instanceTypeId"), "456");
  }

  protected void validateCanDeleteInstanceById (TestContext testContext) {
    JsonObject responseOnPOST = FakeInventoryStorage.post(
            INSTANCE_STORAGE_PATH,
            new TestInstance().setTitle("TestInstance to delete").setInstanceTypeId("12345").getJson());
    testContext.assertEquals(responseOnPOST.getString("title"), "TestInstance to delete");
    FakeInventoryStorage.delete(INSTANCE_STORAGE_PATH, responseOnPOST.getString("id"),200);
  }

  protected void cannotDeleteInstanceWithHoldings (TestContext testContext) {
    JsonObject responseOnPOST = FakeInventoryStorage.post(
            INSTANCE_STORAGE_PATH,
            new TestInstance().setTitle("TestInstance with holdings").setInstanceTypeId("12345").getJson(), 201);
    String instanceId = responseOnPOST.getString("id");
    JsonObject responseOnHoldingsPOST = FakeInventoryStorage.post(
            HOLDINGS_STORAGE_PATH,
            new TestHoldingsRecord().setCallNumber("Test holdings").setInstanceId(instanceId).getJson(), 201);
    FakeInventoryStorage.delete(INSTANCE_STORAGE_PATH, instanceId, 400);
  }

  protected void cannotDeleteInstanceWithInstanceRelations (TestContext testContext) {
    JsonObject responseOnPOSTChild = FakeInventoryStorage.post(
            INSTANCE_STORAGE_PATH,
            new TestInstance().setTitle("TestInstance with parent").setInstanceTypeId("12345").getJson(), 201);
    String childId = responseOnPOSTChild.getString("id");
    JsonObject responseOnPOSTParent = FakeInventoryStorage.post(
            INSTANCE_STORAGE_PATH,
            new TestInstance().setTitle("TestInstance with child").setInstanceTypeId("12345").getJson(), 201);
    String parentId = responseOnPOSTParent.getString("id");
    JsonObject responseOnPOSTRelation = FakeInventoryStorage.post(
            INSTANCE_RELATIONSHIP_STORAGE_PATH,
            new TestInstanceRelationship()
                    .setSubInstanceId(childId)
                    .setSuperInstanceId(parentId).getJson(), 201);
    FakeInventoryStorage.delete(INSTANCE_STORAGE_PATH, parentId, 400);

  }

}
