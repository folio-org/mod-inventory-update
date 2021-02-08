package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;

import static org.folio.inventoryupdate.test.FakeInventoryStorage.*;


/**
 *
 * Validates that the fake storage API behaves as expected for testing the instance match service.
 */
public class StorageValidatorInstances  {

  protected void validateStorage(TestContext testContext, FakeInventoryStorage fakeInventoryStorage) {
    validatePostAndGetById(testContext);
    validateGetByQueryAndPut(testContext);
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
            "query="+ RecordStorage.encode("title==\"New TestInstance\""));;
    testContext.assertEquals(
            responseJson.getInteger("totalRecords"), 1,"Number of " + RESULT_SET_INSTANCES + " expected: 1" );
    JsonObject existingRecord = responseJson.getJsonArray(RESULT_SET_INSTANCES).getJsonObject(0);
    existingRecord.put("instanceTypeId", "456");
    FakeInventoryStorage.put(INSTANCE_STORAGE_PATH, existingRecord);
    JsonObject record = FakeInventoryStorage.getRecordById(INSTANCE_STORAGE_PATH, existingRecord.getString("id"));
    testContext.assertEquals(record.getString("instanceTypeId"), "456");
  }


}
