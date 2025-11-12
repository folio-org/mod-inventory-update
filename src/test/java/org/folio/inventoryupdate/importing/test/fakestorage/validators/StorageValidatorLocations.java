package org.folio.inventoryupdate.importing.test.fakestorage.validators;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.importing.test.fakestorage.FakeFolioApisForImporting;
import org.folio.inventoryupdate.importing.test.fakestorage.entities.InputLocation;

import static org.folio.inventoryupdate.importing.test.fakestorage.FakeFolioApisForImporting.*;

public class StorageValidatorLocations {
    protected void validateStorage(TestContext testContext) {

        validatePostAndGetById(testContext);
    }

    protected void validatePostAndGetById(TestContext testContext) {
        JsonObject responseOnPOST = FakeFolioApisForImporting.post(
                LOCATION_STORAGE_PATH,
                new InputLocation().setId("LOC3").setInstitutionId("INST3").setName("Test Location").getJson());
        testContext.assertEquals(responseOnPOST.getString("id"), "LOC3");
        JsonObject responseOnGET = FakeFolioApisForImporting.getRecordById(LOCATION_STORAGE_PATH, responseOnPOST.getString("id"));
        testContext.assertEquals(responseOnGET.getString("institutionId"), "INST3");
    }
}
