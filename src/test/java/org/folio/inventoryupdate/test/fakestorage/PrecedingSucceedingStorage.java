package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.test.fakestorage.entitites.TestInstanceTitleSuccession;

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
        fakeStorage.instanceStorage.acceptDependant(this, TestInstanceTitleSuccession.SUCCEEDING_INSTANCE_ID);
        fakeStorage.instanceStorage.acceptDependant(this, TestInstanceTitleSuccession.PRECEDING_INSTANCE_ID);
    }

    @Override
    public void createRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        TestInstanceTitleSuccession titleSuccession = new TestInstanceTitleSuccession(recordJson);
        int code = insert(new TestInstanceTitleSuccession(recordJson));
        respond(routingContext, recordJson, code);
    }


}
