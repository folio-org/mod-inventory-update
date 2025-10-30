package org.folio.inventoryupdate.updating.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApis;
import org.folio.inventoryupdate.updating.test.fakestorage.RecordStorage;
import org.folio.inventoryupdate.updating.test.fakestorage.entitites.InputHoldingsRecord;
import org.folio.inventoryupdate.updating.test.fakestorage.entitites.InputInstance;

import static org.folio.inventoryupdate.updating.test.fakestorage.FakeFolioApis.*;

public class StorageValidatorHoldingsRecords {

    private static final String FIRST_CALL_NUMBER = "TEST_CN_1";
    private static final String SECOND_CALL_NUMBER = "TEST_CN_2";
    private static final String INSTANCE_TITLE = "New InputInstance";
    private static final String NON_EXISTING_INSTANCE_ID = "456";
    private String existingInstanceId;

    protected void validateStorage(TestContext testContext) {
        createDependencies();
        validatePostAndGetById(testContext);
        validateGetByQueryAndPut(testContext);
        validateCannotPostWithBadInstanceId(testContext);
        validateCanDeleteHoldingsRecordById(testContext);
        validateCannotPostWithBadLocationId( testContext );
    }

    protected void createDependencies() {
        JsonObject responseOnPOST = FakeFolioApis.post(
                INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle(INSTANCE_TITLE).setInstanceTypeId("123").setSource("test").getJson());
        existingInstanceId = responseOnPOST.getString("id");
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApis.post(
                HOLDINGS_STORAGE_PATH,
                new InputHoldingsRecord().setInstanceId(existingInstanceId).setPermanentLocationId(InventoryUpdateTestBase.LOCATION_ID_1).setCallNumber(FIRST_CALL_NUMBER).getJson());
        testContext.assertEquals(responseOnPOST.getString("callNumber"), FIRST_CALL_NUMBER);
        JsonObject responseOnGET = FakeFolioApis.getRecordById(HOLDINGS_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString("callNumber"), FIRST_CALL_NUMBER);
    }

    protected void validateGetByQueryAndPut(TestContext testContext) {
        JsonObject responseJson = FakeFolioApis.getRecordsByQuery(
                HOLDINGS_STORAGE_PATH,
                "query="+ RecordStorage.encode("instanceId==\""+ existingInstanceId +"\""));
        testContext.assertEquals(
                responseJson.getInteger("totalRecords"), 1,"Number of " + RESULT_SET_HOLDINGS_RECORDS + " expected: 1" );
        JsonObject existingRecord = responseJson.getJsonArray(RESULT_SET_HOLDINGS_RECORDS).getJsonObject(0);
        existingRecord.put("callNumber", SECOND_CALL_NUMBER);
        FakeFolioApis.put(HOLDINGS_STORAGE_PATH, existingRecord);
        JsonObject holdingsRecord = FakeFolioApis.getRecordById(HOLDINGS_STORAGE_PATH, existingRecord.getString("id"));
        testContext.assertEquals(holdingsRecord.getString("callNumber"), SECOND_CALL_NUMBER);
    }

    protected void validateCanDeleteHoldingsRecordById (TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApis.post(
                HOLDINGS_STORAGE_PATH,
                new InputHoldingsRecord().setPermanentLocationId(InventoryUpdateTestBase.LOCATION_ID_1).setCallNumber("TEST-CN").setInstanceId(existingInstanceId).getJson());
        testContext.assertEquals(responseOnPOST.getString("callNumber"), "TEST-CN");
        FakeFolioApis.delete(HOLDINGS_STORAGE_PATH, responseOnPOST.getString("id"),200);
    }

    protected void validateCannotPostWithBadInstanceId (TestContext testContext) {
        FakeFolioApis.post(
                HOLDINGS_STORAGE_PATH,
                new InputHoldingsRecord().setInstanceId(NON_EXISTING_INSTANCE_ID).setPermanentLocationId(InventoryUpdateTestBase.LOCATION_ID_1).setCallNumber(FIRST_CALL_NUMBER).getJson(),
                500);
        JsonObject responseJson = FakeFolioApis.getRecordsByQuery(
                HOLDINGS_STORAGE_PATH,
                "query="+ RecordStorage.encode("instanceId==\""+ NON_EXISTING_INSTANCE_ID +"\""));
        testContext.assertEquals(
                responseJson.getInteger("totalRecords"), 0,"Number of " + RESULT_SET_HOLDINGS_RECORDS + " expected for bad instance ID " + NON_EXISTING_INSTANCE_ID + ": 0" );

    }

    protected void validateCannotPostWithBadLocationId (TestContext testContext) {
        FakeFolioApis.post(
                HOLDINGS_STORAGE_PATH,
                new InputHoldingsRecord().setInstanceId(existingInstanceId).setPermanentLocationId("BAD_LOCATION").setCallNumber(FIRST_CALL_NUMBER).getJson(),
                500);
        JsonObject responseJson = FakeFolioApis.getRecordsByQuery(
                HOLDINGS_STORAGE_PATH,
                "query="+ RecordStorage.encode("permanentLocationId==\""+ "BAD_LOCATION" +"\""));
        testContext.assertEquals(
                responseJson.getInteger("totalRecords"), 0,"Number of " + RESULT_SET_HOLDINGS_RECORDS + " expected for bad location ID " + NON_EXISTING_INSTANCE_ID + ": 0" );

    }

}
