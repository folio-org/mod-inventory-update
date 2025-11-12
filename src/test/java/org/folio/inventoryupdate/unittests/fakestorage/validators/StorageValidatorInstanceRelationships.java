package org.folio.inventoryupdate.unittests.fakestorage.validators;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputInstance;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputInstanceRelationship;

public class StorageValidatorInstanceRelationships {
    private String childInstanceId;
    private String parentInstanceId;
    protected void validateStorage(TestContext testContext) {
        createTwoTitles();
        validatePostAndGetById(testContext);
    }

    protected void createTwoTitles () {
        JsonObject responseOnPOSTChild = FakeFolioApisForImporting.post(
                FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Child Instance").setInstanceTypeId("12345").setSource("test").getJson(), 201);
        childInstanceId = responseOnPOSTChild.getString("id");
        JsonObject responseOnPOSTParent = FakeFolioApisForImporting.post(
                FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Parent Instance").setInstanceTypeId("12345").setSource("test").getJson(), 201);
        parentInstanceId = responseOnPOSTParent.getString("id");
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApisForImporting.post(
                FakeFolioApisForImporting.INSTANCE_RELATIONSHIP_STORAGE_PATH,
                new InputInstanceRelationship().setSubInstanceId(childInstanceId).setSuperInstanceId(parentInstanceId).getJson());
        testContext.assertEquals(responseOnPOST.getString(InputInstanceRelationship.SUB_INSTANCE_ID), childInstanceId);
        JsonObject responseOnGET = FakeFolioApisForImporting.getRecordById(FakeFolioApisForImporting.INSTANCE_RELATIONSHIP_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString(InputInstanceRelationship.SUPER_INSTANCE_ID), parentInstanceId);
    }

}
