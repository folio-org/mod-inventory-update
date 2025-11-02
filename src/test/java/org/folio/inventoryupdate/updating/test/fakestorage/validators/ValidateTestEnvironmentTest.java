package org.folio.inventoryupdate.updating.test.fakestorage.validators;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventoryupdate.updating.test.InventoryUpdateTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ValidateTestEnvironmentTest extends InventoryUpdateTestBase {
  @Test
  public void testFakeFolioApis(TestContext testContext) {
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
