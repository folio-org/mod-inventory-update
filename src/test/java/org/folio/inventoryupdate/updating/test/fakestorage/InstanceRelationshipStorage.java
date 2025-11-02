package org.folio.inventoryupdate.updating.test.fakestorage;

import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.updating.test.fakestorage.entitites.InputInstanceRelationship;


public class InstanceRelationshipStorage extends RecordStorage {

    @Override
    public String getResultSetName() {
        return INSTANCE_RELATIONSHIPS;
    }

    @Override
    protected void declareDependencies() {
        fakeStorageForUpserts.instanceStorage.acceptDependant(this, InputInstanceRelationship.SUB_INSTANCE_ID);
        fakeStorageForUpserts.instanceStorage.acceptDependant(this, InputInstanceRelationship.SUPER_INSTANCE_ID);
    }

    @Override
    public void updateRecord(RoutingContext routingContext) {
        // not needed
    }

}
