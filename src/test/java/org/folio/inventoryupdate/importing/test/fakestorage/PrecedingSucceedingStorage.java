package org.folio.inventoryupdate.importing.test.fakestorage;

import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.updating.test.fakestorage.entitites.InputInstanceTitleSuccession;

public class PrecedingSucceedingStorage extends RecordStorage {

  @Override
  public void updateRecord(RoutingContext routingContext) {
    // not needed
  }

  public String getResultSetName () {
    return PRECEDING_SUCCEEDING_TITLES;
  }

  @Override
  protected void declareDependencies() {
    fakeStorageForImporting.instanceStorage.acceptDependant(this, InputInstanceTitleSuccession.SUCCEEDING_INSTANCE_ID);
    fakeStorageForImporting.instanceStorage.acceptDependant(this, InputInstanceTitleSuccession.PRECEDING_INSTANCE_ID);
  }
}
