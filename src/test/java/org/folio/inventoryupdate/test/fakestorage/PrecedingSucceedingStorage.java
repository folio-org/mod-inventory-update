package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.test.fakestorage.entitites.InputInstanceTitleSuccession;

public class PrecedingSucceedingStorage extends RecordStorage {


    @Override
    protected void updateRecord(RoutingContext routingContext) {
        // not needed
    }

    public String getResultSetName () {
        return PRECEDING_SUCCEEDING_TITLES;
    }

    @Override
    protected void declareDependencies() {
        fakeStorage.instanceStorage.acceptDependant(this, InputInstanceTitleSuccession.SUCCEEDING_INSTANCE_ID);
        fakeStorage.instanceStorage.acceptDependant(this, InputInstanceTitleSuccession.PRECEDING_INSTANCE_ID);
    }

    @Override
    protected void declareMandatoryProperties() {

    }


}
