package org.folio.inventoryupdate.importing.test.fakestorage.validators;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.importing.test.fakestorage.FakeFolioApisForImporting;
import org.folio.inventoryupdate.importing.test.fakestorage.entities.InputInstance;
import org.folio.inventoryupdate.importing.test.fakestorage.entities.InputInstanceRelationship;

import static org.folio.inventoryupdate.importing.test.fakestorage.FakeFolioApisForImporting.*;
import static org.folio.inventoryupdate.importing.test.fakestorage.entities.InputInstanceRelationship.*;

public class StorageValidatorInstanceRelationships {
    private String childInstanceId;
    private String parentInstanceId;
    protected void validateStorage(TestContext testContext) {
        createTwoTitles();
        validatePostAndGetById(testContext);
    }

    protected void createTwoTitles () {
        JsonObject responseOnPOSTChild = FakeFolioApisForImporting.post(
                INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Child Instance").setInstanceTypeId("12345").setSource("test").getJson(), 201);
        childInstanceId = responseOnPOSTChild.getString("id");
        JsonObject responseOnPOSTParent = FakeFolioApisForImporting.post(
                INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Parent Instance").setInstanceTypeId("12345").setSource("test").getJson(), 201);
        parentInstanceId = responseOnPOSTParent.getString("id");
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApisForImporting.post(
                INSTANCE_RELATIONSHIP_STORAGE_PATH,
                new InputInstanceRelationship().setSubInstanceId(childInstanceId).setSuperInstanceId(parentInstanceId).getJson());
        testContext.assertEquals(responseOnPOST.getString(SUB_INSTANCE_ID), childInstanceId);
        JsonObject responseOnGET = FakeFolioApisForImporting.getRecordById(INSTANCE_RELATIONSHIP_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString(SUPER_INSTANCE_ID), parentInstanceId);
    }

}
