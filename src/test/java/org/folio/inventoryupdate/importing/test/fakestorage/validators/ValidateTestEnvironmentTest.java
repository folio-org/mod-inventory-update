package org.folio.inventoryupdate.importing.test.fakestorage.validators;

import io.restassured.RestAssured;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventoryupdate.importing.test.InventoryUpdateTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.folio.inventoryupdate.importing.test.fixtures.Service.BASE_URI_OKAPI;

@RunWith(VertxUnitRunner.class)
public class ValidateTestEnvironmentTest extends InventoryUpdateTestBase {
  @Test
  public void testFakeFolioApis(TestContext testContext) {
    RestAssured.baseURI = BASE_URI_OKAPI;
    new StorageValidatorLocations().validateStorage(testContext);
    new StorageValidatorInstances().validateStorage(testContext);
    new StorageValidatorHoldingsRecords().validateStorage(testContext);
    new StorageValidatorItems().validateStorage(testContext);
    new StorageValidatorPrecedingSucceeding().validateStorage(testContext);
    new StorageValidatorInstanceRelationships().validateStorage(testContext);
    new StorageValidatorQueries().validateQueries(testContext);
    new StorageValidatorPoLines().validateStorage(testContext);
  }

}
