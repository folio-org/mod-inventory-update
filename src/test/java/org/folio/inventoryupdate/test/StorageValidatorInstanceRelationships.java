package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.test.fakestorage.FakeFolioApis;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputInstance;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputInstanceRelationship;

import static org.folio.inventoryupdate.test.fakestorage.FakeFolioApis.*;
import static org.folio.inventoryupdate.test.fakestorage.entitites.InputInstanceRelationship.*;

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
        JsonObject responseOnPOSTChild = FakeFolioApis.post(
                INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Child Instance").setInstanceTypeId("12345").setSource("test").getJson(), 201);
        childInstanceId = responseOnPOSTChild.getString("id");
        JsonObject responseOnPOSTParent = FakeFolioApis.post(
                INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Parent Instance").setInstanceTypeId("12345").setSource("test").getJson(), 201);
        parentInstanceId = responseOnPOSTParent.getString("id");
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApis.post(
                INSTANCE_RELATIONSHIP_STORAGE_PATH,
                new InputInstanceRelationship().setSubInstanceId(childInstanceId).setSuperInstanceId(parentInstanceId).getJson());
        testContext.assertEquals(responseOnPOST.getString(SUB_INSTANCE_ID), childInstanceId);
        JsonObject responseOnGET = FakeFolioApis.getRecordById(INSTANCE_RELATIONSHIP_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString(SUPER_INSTANCE_ID), parentInstanceId);
    }

}
