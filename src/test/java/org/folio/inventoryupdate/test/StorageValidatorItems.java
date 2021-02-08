package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage;
import org.folio.inventoryupdate.test.fakestorage.RecordStorage;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestHoldingsRecord;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestInstance;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestItem;

import static org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage.*;

public class StorageValidatorItems {

    private String existingHoldingsRecordId;

    protected void validateStorage(TestContext testContext) {
        createInstanceAndHoldings(testContext);
        validatePostAndGetById(testContext);
        validateGetByQueryAndPut(testContext);
        validateCanDeleteItemById(testContext);
        validateCannotPostWithBadHoldingsRecordId(testContext);
    }
    protected void createInstanceAndHoldings(TestContext testContext) {
        JsonObject responseOnInstancePOST = FakeInventoryStorage.post(
                INSTANCE_STORAGE_PATH,
                new TestInstance().setTitle("Instance for Item test").setInstanceTypeId("123").getJson());
        String existingInstanceId = responseOnInstancePOST.getString("id");
        JsonObject responseOnHoldingsPOST = FakeInventoryStorage.post(
                HOLDINGS_STORAGE_PATH,
                new TestHoldingsRecord().setCallNumber("CN-FOR-ITEM-TEST").setInstanceId(existingInstanceId).getJson());
        existingHoldingsRecordId = responseOnHoldingsPOST.getString("id");
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeInventoryStorage.post(
                ITEM_STORAGE_PATH,
                new TestItem().setHoldingsRecordId(existingHoldingsRecordId).setBarcode("bc-001").getJson());
        testContext.assertEquals(responseOnPOST.getString("barcode"), "bc-001");
        JsonObject responseOnGET = FakeInventoryStorage.getRecordById(ITEM_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString("barcode"), "bc-001");
    }

    protected void validateGetByQueryAndPut(TestContext testContext) {
        JsonObject responseJson = FakeInventoryStorage.getRecordsByQuery(
                ITEM_STORAGE_PATH,
                "query="+ RecordStorage.encode("holdingsRecordId==\""+ existingHoldingsRecordId +"\""));;
        testContext.assertEquals(
                responseJson.getInteger("totalRecords"), 1,"Number of " + RESULT_SET_ITEMS + " expected: 1" );
        JsonObject existingRecord = responseJson.getJsonArray(RESULT_SET_ITEMS).getJsonObject(0);
        existingRecord.put("barcode", "bc-002");
        FakeInventoryStorage.put(ITEM_STORAGE_PATH, existingRecord);
        JsonObject record = FakeInventoryStorage.getRecordById(ITEM_STORAGE_PATH, existingRecord.getString("id"));
        testContext.assertEquals(record.getString("barcode"), "bc-002");
    }

    protected void validateCanDeleteItemById (TestContext testContext) {
        JsonObject responseOnPOST = FakeInventoryStorage.post(
                ITEM_STORAGE_PATH,
                new TestItem().setBarcode("TEST-BC").setHoldingsRecordId(existingHoldingsRecordId).getJson());
        testContext.assertEquals(responseOnPOST.getString("barcode"), "TEST-BC");
        FakeInventoryStorage.delete(ITEM_STORAGE_PATH, responseOnPOST.getString("id"),200);
    }

    protected void validateCannotPostWithBadHoldingsRecordId (TestContext testContext) {
        JsonObject responseOnPOST = FakeInventoryStorage.post(
                ITEM_STORAGE_PATH,
                new TestItem().setHoldingsRecordId("12345").setBarcode("bc-003").getJson(),
                400);
        JsonObject responseJson = FakeInventoryStorage.getRecordsByQuery(
                ITEM_STORAGE_PATH,
                "query="+ RecordStorage.encode("holdingsRecordId==\"12345\""));;
        testContext.assertEquals(
                responseJson.getInteger("totalRecords"), 0,"Number of " + RESULT_SET_ITEMS + " expected for bad holdingsRecord ID 12345: 0" );

    }

}
