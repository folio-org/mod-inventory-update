package org.folio.inventoryupdate.importing.test.fakestorage.validators;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.importing.test.fakestorage.RecordStorage;
import org.folio.inventoryupdate.importing.test.InventoryUpdateTestBase;
import org.folio.inventoryupdate.importing.test.fakestorage.FakeFolioApisForImporting;
import org.folio.inventoryupdate.importing.test.fakestorage.entities.InputHoldingsRecord;
import org.folio.inventoryupdate.importing.test.fakestorage.entities.InputInstance;
import org.folio.inventoryupdate.importing.test.fakestorage.entities.InputItem;

import static org.folio.inventoryupdate.importing.test.InventoryUpdateTestBase.MATERIAL_TYPE_TEXT;
import static org.folio.inventoryupdate.importing.test.InventoryUpdateTestBase.STATUS_UNKNOWN;
import static org.folio.inventoryupdate.importing.test.fakestorage.FakeFolioApisForImporting.*;

public class StorageValidatorItems {

    private String existingHoldingsRecordId;

    protected void validateStorage(TestContext testContext) {
        createInstanceAndHoldings();
        validatePostAndGetById(testContext);
        validateGetByQueryAndPut(testContext);
        validateCanDeleteItemById(testContext);
        validateCannotPostWithBadHoldingsRecordId(testContext);
    }
    protected void createInstanceAndHoldings() {
        JsonObject responseOnInstancePOST = FakeFolioApisForImporting.post(
                INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Instance for Item test").setInstanceTypeId("123").setSource("test").getJson());
        String existingInstanceId = responseOnInstancePOST.getString("id");
        JsonObject responseOnHoldingsPOST = FakeFolioApisForImporting.post(
                HOLDINGS_STORAGE_PATH,
                new InputHoldingsRecord().setPermanentLocationId(InventoryUpdateTestBase.LOCATION_ID_1).setCallNumber("CN-FOR-ITEM-TEST").setInstanceId(existingInstanceId).getJson());
        existingHoldingsRecordId = responseOnHoldingsPOST.getString("id");
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApisForImporting.post(
                ITEM_STORAGE_PATH,
                new InputItem().setHoldingsRecordId(existingHoldingsRecordId).setBarcode("bc-001")
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .getJson());
        testContext.assertEquals(responseOnPOST.getString("barcode"), "bc-001");
        JsonObject responseOnGET = FakeFolioApisForImporting.getRecordById(ITEM_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString("barcode"), "bc-001");
    }

    protected void validateGetByQueryAndPut(TestContext testContext) {
        JsonObject responseJson = FakeFolioApisForImporting.getRecordsByQuery(
                ITEM_STORAGE_PATH,
                "query="+ RecordStorage.encode("holdingsRecordId==\""+ existingHoldingsRecordId +"\""));
        testContext.assertEquals(
                responseJson.getInteger("totalRecords"), 1,"Number of " + RESULT_SET_ITEMS + " expected: 1" );
        JsonObject existingRecord = responseJson.getJsonArray(RESULT_SET_ITEMS).getJsonObject(0);
        existingRecord.put("barcode", "bc-002");
        FakeFolioApisForImporting.put(ITEM_STORAGE_PATH, existingRecord);
        JsonObject item = FakeFolioApisForImporting.getRecordById(ITEM_STORAGE_PATH, existingRecord.getString("id"));
        testContext.assertEquals(item.getString("barcode"), "bc-002");
    }

    protected void validateCanDeleteItemById (TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApisForImporting.post(
                ITEM_STORAGE_PATH,
                new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setBarcode("TEST-BC").setHoldingsRecordId(existingHoldingsRecordId).getJson());
        testContext.assertEquals(responseOnPOST.getString("barcode"), "TEST-BC");
        FakeFolioApisForImporting.delete(ITEM_STORAGE_PATH, responseOnPOST.getString("id"),200);
    }

    protected void validateCannotPostWithBadHoldingsRecordId (TestContext testContext) {
        FakeFolioApisForImporting.post(
                ITEM_STORAGE_PATH,
                new InputItem()
                        .setStatus(STATUS_UNKNOWN)
                        .setMaterialTypeId(MATERIAL_TYPE_TEXT)
                        .setHoldingsRecordId("12345").setBarcode("bc-003").getJson(),
                500);
        JsonObject responseJson = FakeFolioApisForImporting.getRecordsByQuery(
                ITEM_STORAGE_PATH,
                "query="+ RecordStorage.encode("holdingsRecordId==\"12345\""));
        testContext.assertEquals(
                responseJson.getInteger("totalRecords"), 0,"Number of " + RESULT_SET_ITEMS + " expected for bad holdingsRecord ID 12345: 0" );

    }

}
