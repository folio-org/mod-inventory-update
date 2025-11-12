package org.folio.inventoryupdate.unittests.fakestorage.validators;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputInstance;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputInstanceTitleSuccession;

public class StorageValidatorPrecedingSucceeding  {
    private String succeedingInstanceId;
    private String precedingInstanceId;
    protected void validateStorage(TestContext testContext) {
        createTwoTitles();
        validatePostAndGetById(testContext);
    }

    protected void createTwoTitles () {
        JsonObject responseOnPOSTSucceeding = FakeFolioApisForImporting.post(
                FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Succeeding title").setInstanceTypeId("12345").setSource("test").getJson(), 201);
        succeedingInstanceId = responseOnPOSTSucceeding.getString("id");
        JsonObject responseOnPOSTPreceding = FakeFolioApisForImporting.post(
                FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Preceding title").setInstanceTypeId("12345").setSource("test").getJson(), 201);
        precedingInstanceId = responseOnPOSTPreceding.getString("id");
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApisForImporting.post(
                FakeFolioApisForImporting.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH,
                new InputInstanceTitleSuccession().setSucceedingInstanceId(succeedingInstanceId).setPrecedingInstanceId(precedingInstanceId).getJson());
        testContext.assertEquals(responseOnPOST.getString(InputInstanceTitleSuccession.SUCCEEDING_INSTANCE_ID), succeedingInstanceId);
        JsonObject responseOnGET = FakeFolioApisForImporting.getRecordById(FakeFolioApisForImporting.PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString(InputInstanceTitleSuccession.SUCCEEDING_INSTANCE_ID), succeedingInstanceId);
    }

}
