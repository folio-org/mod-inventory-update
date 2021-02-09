package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputInstance;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputInstanceTitleSuccession;
import static org.folio.inventoryupdate.test.fakestorage.entitites.InputInstanceTitleSuccession.*;
import static org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage.*;

public class StorageValidatorPrecedingSucceeding  {
    private String succeedingInstanceId;
    private String precedingInstanceId;
    protected void validateStorage(TestContext testContext) {
        createTwoTitles(testContext);
        validatePostAndGetById(testContext);
        //validateCanDeleteSuccessionById(testContext);
        //validateCannotPostWithBadInstanceId(testContext);
    }

    protected void createTwoTitles (TestContext testContext) {
        JsonObject responseOnPOSTSucceeding = FakeInventoryStorage.post(
                INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Succeeding title").setInstanceTypeId("12345").getJson(), 201);
        succeedingInstanceId = responseOnPOSTSucceeding.getString("id");
        JsonObject responseOnPOSTPreceding = FakeInventoryStorage.post(
                INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Preceding title").setInstanceTypeId("12345").getJson(), 201);
        precedingInstanceId = responseOnPOSTPreceding.getString("id");
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeInventoryStorage.post(
                PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH,
                new InputInstanceTitleSuccession().setSucceedingInstanceId(succeedingInstanceId).setPrecedingInstanceId(precedingInstanceId).getJson());
        testContext.assertEquals(responseOnPOST.getString(SUCCEEDING_INSTANCE_ID), succeedingInstanceId);
        JsonObject responseOnGET = FakeInventoryStorage.getRecordById(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString(SUCCEEDING_INSTANCE_ID), succeedingInstanceId);
    }

}
