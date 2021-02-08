package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestInstance;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestInstanceRelationship;

import static org.folio.inventoryupdate.test.fakestorage.FakeInventoryStorage.*;
import static org.folio.inventoryupdate.test.fakestorage.entitites.TestInstanceRelationship.*;

public class StorageValidatorInstanceRelationships {
    private String childInstanceId;
    private String parentInstanceId;
    protected void validateStorage(TestContext testContext) {
        createTwoTitles(testContext);
        validatePostAndGetById(testContext);
        //validateCanDeleteSuccessionById(testContext);
        //validateCannotPostWithBadInstanceId(testContext);
    }

    protected void createTwoTitles (TestContext testContext) {
        JsonObject responseOnPOSTChild = FakeInventoryStorage.post(
                INSTANCE_STORAGE_PATH,
                new TestInstance().setTitle("Child Instance").setInstanceTypeId("12345").getJson(), 201);
        childInstanceId = responseOnPOSTChild.getString("id");
        JsonObject responseOnPOSTParent = FakeInventoryStorage.post(
                INSTANCE_STORAGE_PATH,
                new TestInstance().setTitle("Parent Instance").setInstanceTypeId("12345").getJson(), 201);
        parentInstanceId = responseOnPOSTParent.getString("id");
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeInventoryStorage.post(
                INSTANCE_RELATIONSHIP_STORAGE_PATH,
                new TestInstanceRelationship().setSubInstanceId(childInstanceId).setSuperInstanceId(parentInstanceId).getJson());
        testContext.assertEquals(responseOnPOST.getString(SUB_INSTANCE_ID), childInstanceId);
        JsonObject responseOnGET = FakeInventoryStorage.getRecordById(INSTANCE_RELATIONSHIP_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString(SUPER_INSTANCE_ID), parentInstanceId);
    }

}
