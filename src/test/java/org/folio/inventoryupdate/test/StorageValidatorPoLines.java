package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.test.fakestorage.FakeFolioApis;

import static org.folio.inventoryupdate.test.fakestorage.FakeFolioApis.*;

public class StorageValidatorPoLines {
  protected void validateStorage(TestContext testContext) {
    validatePostAndGetById(testContext);
  }

  protected void validatePostAndGetById(TestContext testContext) {
    JsonObject responseOnPOST = FakeFolioApis.post(
        ORDER_LINES_STORAGE_PATH,
        new JsonObject("{\"purchaseOrderId\": \"3b198b70-cf8e-4075-9e93-ebf2c76e60c2\", " +
            "\"instanceId\": \"ff8702a1-c562-48f0-a3fe-00421ce3c6d3\", \"orderFormat\": \"Other\", \"source\": \"User\", \"titleOrPackage\": \"New InputInstance\" }"));
    testContext.assertEquals(responseOnPOST.getString("titleOrPackage"), "New InputInstance");
    JsonObject responseOnGET = FakeFolioApis.getRecordById(ORDER_LINES_STORAGE_PATH, responseOnPOST.getString("id"));
    testContext.assertEquals(responseOnGET.getString("titleOrPackage"), "New InputInstance");
  }

}
