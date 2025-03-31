package org.folio.inventoryupdate.test;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ValidateTestEnvironment extends InventoryUpdateTestBase {
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
