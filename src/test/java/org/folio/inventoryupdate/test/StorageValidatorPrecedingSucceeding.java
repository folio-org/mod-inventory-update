package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.test.fakestorage.FakeFolioApis;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputInstance;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputInstanceTitleSuccession;
import static org.folio.inventoryupdate.test.fakestorage.entitites.InputInstanceTitleSuccession.*;
import static org.folio.inventoryupdate.test.fakestorage.FakeFolioApis.*;

public class StorageValidatorPrecedingSucceeding  {
    private String succeedingInstanceId;
    private String precedingInstanceId;
    protected void validateStorage(TestContext testContext) {
        createTwoTitles(testContext);
        validatePostAndGetById(testContext);
    }

    protected void createTwoTitles (TestContext testContext) {
        JsonObject responseOnPOSTSucceeding = FakeFolioApis.post(
                INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Succeeding title").setInstanceTypeId("12345").setSource("test").getJson(), 201);
        succeedingInstanceId = responseOnPOSTSucceeding.getString("id");
        JsonObject responseOnPOSTPreceding = FakeFolioApis.post(
                INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Preceding title").setInstanceTypeId("12345").setSource("test").getJson(), 201);
        precedingInstanceId = responseOnPOSTPreceding.getString("id");
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApis.post(
                PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH,
                new InputInstanceTitleSuccession().setSucceedingInstanceId(succeedingInstanceId).setPrecedingInstanceId(precedingInstanceId).getJson());
        testContext.assertEquals(responseOnPOST.getString(SUCCEEDING_INSTANCE_ID), succeedingInstanceId);
        JsonObject responseOnGET = FakeFolioApis.getRecordById(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString(SUCCEEDING_INSTANCE_ID), succeedingInstanceId);
    }

}
