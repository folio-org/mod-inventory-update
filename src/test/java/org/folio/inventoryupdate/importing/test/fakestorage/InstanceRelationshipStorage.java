package org.folio.inventoryupdate.importing.test.fakestorage;

import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.importing.test.fakestorage.entities.InputInstanceRelationship;

public class InstanceRelationshipStorage extends RecordStorage{
  @Override
  public String getResultSetName() {
    return INSTANCE_RELATIONSHIPS;
  }

  @Override
  protected void declareDependencies() {
    fakeStorageForImporting.instanceStorage.acceptDependant(this, InputInstanceRelationship.SUB_INSTANCE_ID);
    fakeStorageForImporting.instanceStorage.acceptDependant(this, InputInstanceRelationship.SUPER_INSTANCE_ID);
  }

  @Override
  public void updateRecord(RoutingContext routingContext) {
    // not needed
  }
}
