package org.folio.inventoryupdate.unittests.fakestorage.validators;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputLocation;

public class StorageValidatorLocations {
    protected void validateStorage(TestContext testContext) {

        validatePostAndGetById(testContext);
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApisForImporting.post(
                FakeFolioApisForImporting.LOCATION_STORAGE_PATH,
                new InputLocation().setId("LOC3").setInstitutionId("INST3").setName("Test Location").getJson());
        testContext.assertEquals(responseOnPOST.getString("id"), "LOC3");
        JsonObject responseOnGET = FakeFolioApisForImporting.getRecordById(FakeFolioApisForImporting.LOCATION_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString("institutionId"), "INST3");
    }
}
