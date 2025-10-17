package org.folio.inventory.updating.test.fakestorage;

import io.vertx.ext.web.RoutingContext;
import org.folio.inventory.updating.test.fakestorage.entitites.InputInstanceRelationship;


public class InstanceRelationshipStorage extends RecordStorage {

    @Override
    public String getResultSetName() {
        return INSTANCE_RELATIONSHIPS;
    }

    @Override
    protected void declareDependencies() {
        fakeStorage.instanceStorage.acceptDependant(this, InputInstanceRelationship.SUB_INSTANCE_ID);
        fakeStorage.instanceStorage.acceptDependant(this, InputInstanceRelationship.SUPER_INSTANCE_ID);
    }

    @Override
    protected void declareMandatoryProperties() {

    }

    @Override
    public void updateRecord(RoutingContext routingContext) {
        // not needed
    }

}
