package org.folio.inventoryupdate.unittests.fakestorage.validators;

import io.restassured.RestAssured;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventoryupdate.unittests.InventoryUpdateTestBase;
import org.folio.inventoryupdate.unittests.fixtures.Service;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ValidateTestEnvironmentTest extends InventoryUpdateTestBase {
  @Test
  public void testFakeFolioApis(TestContext testContext) {
    RestAssured.baseURI = Service.BASE_URI_OKAPI;
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
