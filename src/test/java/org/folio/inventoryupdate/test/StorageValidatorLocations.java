package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.test.fakestorage.FakeFolioApis;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputLocation;

import static org.folio.inventoryupdate.test.fakestorage.FakeFolioApis.*;

public class StorageValidatorLocations {
    protected void validateStorage(TestContext testContext) {

        validatePostAndGetById(testContext);
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApis.post(
                LOCATION_STORAGE_PATH,
                new InputLocation().setId("LOC3").setInstitutionId("INST3").setName("Test Location").getJson());
        testContext.assertEquals(responseOnPOST.getString("id"), "LOC3");
        JsonObject responseOnGET = FakeFolioApis.getRecordById(LOCATION_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString("institutionId"), "INST3");
    }
}
