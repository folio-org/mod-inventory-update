package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage;
import org.folio.inventoryupdate.test.fakestorage.RecordStorage;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputHoldingsRecord;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputInstance;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputLocation;

import static org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage.*;

public class StorageValidatorHoldingsRecords {

    private static String FIRST_CALL_NUMBER = "TEST_CN_1";
    private static String SECOND_CALL_NUMBER = "TEST_CN_2";
    private static String INSTANCE_TITLE = "New InputInstance";
    private static String NON_EXISTING_INSTANCE_ID = "456";
    private String existingInstanceId;

    protected void validateStorage(TestContext testContext) {
        createDependencies(testContext);
        validatePostAndGetById(testContext);
        validateGetByQueryAndPut(testContext);
        validateCannotPostWithBadInstanceId(testContext);
        validateCanDeleteHoldingsRecordById(testContext);
    }

    protected void createDependencies(TestContext testContext) {
        JsonObject responseOnPOST = FakeInventoryStorage.post(
                INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle(INSTANCE_TITLE).setInstanceTypeId("123").getJson());
        existingInstanceId = responseOnPOST.getString("id");
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeInventoryStorage.post(
                HOLDINGS_STORAGE_PATH,
                new InputHoldingsRecord().setInstanceId(existingInstanceId).setPermanentLocationId(InventoryUpdateTestSuite.LOCATION_ID).setCallNumber(FIRST_CALL_NUMBER).getJson());
        testContext.assertEquals(responseOnPOST.getString("callNumber"), FIRST_CALL_NUMBER);
        JsonObject responseOnGET = FakeInventoryStorage.getRecordById(HOLDINGS_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString("callNumber"), FIRST_CALL_NUMBER);
    }

    protected void validateGetByQueryAndPut(TestContext testContext) {
        JsonObject responseJson = FakeInventoryStorage.getRecordsByQuery(
                HOLDINGS_STORAGE_PATH,
                "query="+ RecordStorage.encode("instanceId==\""+ existingInstanceId +"\""));
        testContext.assertEquals(
                responseJson.getInteger("totalRecords"), 1,"Number of " + RESULT_SET_HOLDINGS_RECORDS + " expected: 1" );
        JsonObject existingRecord = responseJson.getJsonArray(RESULT_SET_HOLDINGS_RECORDS).getJsonObject(0);
        existingRecord.put("callNumber", SECOND_CALL_NUMBER);
        FakeInventoryStorage.put(HOLDINGS_STORAGE_PATH, existingRecord);
        JsonObject record = FakeInventoryStorage.getRecordById(HOLDINGS_STORAGE_PATH, existingRecord.getString("id"));
        testContext.assertEquals(record.getString("callNumber"), SECOND_CALL_NUMBER);
    }

    protected void validateCanDeleteHoldingsRecordById (TestContext testContext) {
        JsonObject responseOnPOST = FakeInventoryStorage.post(
                HOLDINGS_STORAGE_PATH,
                new InputHoldingsRecord().setPermanentLocationId(InventoryUpdateTestSuite.LOCATION_ID).setCallNumber("TEST-CN").setInstanceId(existingInstanceId).getJson());
        testContext.assertEquals(responseOnPOST.getString("callNumber"), "TEST-CN");
        FakeInventoryStorage.delete(HOLDINGS_STORAGE_PATH, responseOnPOST.getString("id"),200);
    }

    protected void validateCannotPostWithBadInstanceId (TestContext testContext) {
        JsonObject responseOnPOST = FakeInventoryStorage.post(
                HOLDINGS_STORAGE_PATH,
                new InputHoldingsRecord().setInstanceId(NON_EXISTING_INSTANCE_ID).setPermanentLocationId(InventoryUpdateTestSuite.LOCATION_ID).setCallNumber(FIRST_CALL_NUMBER).getJson(),
                400);
        JsonObject responseJson = FakeInventoryStorage.getRecordsByQuery(
                HOLDINGS_STORAGE_PATH,
                "query="+ RecordStorage.encode("instanceId==\""+ NON_EXISTING_INSTANCE_ID +"\""));;
        testContext.assertEquals(
                responseJson.getInteger("totalRecords"), 0,"Number of " + RESULT_SET_HOLDINGS_RECORDS + " expected for bad instance ID " + NON_EXISTING_INSTANCE_ID + ": 0" );

    }

    protected void validateCannotPostWithBadLocationId (TestContext testContext) {
        JsonObject responseOnPOST = FakeInventoryStorage.post(
                HOLDINGS_STORAGE_PATH,
                new InputHoldingsRecord().setInstanceId(existingInstanceId).setPermanentLocationId("BAD_LOCATION").setCallNumber(FIRST_CALL_NUMBER).getJson(),
                400);
        JsonObject responseJson = FakeInventoryStorage.getRecordsByQuery(
                HOLDINGS_STORAGE_PATH,
                "query="+ RecordStorage.encode("permanentLocationId==\""+ "BAD_LOCATION" +"\""));;
        testContext.assertEquals(
                responseJson.getInteger("totalRecords"), 0,"Number of " + RESULT_SET_HOLDINGS_RECORDS + " expected for bad location ID " + NON_EXISTING_INSTANCE_ID + ": 0" );

    }

}
