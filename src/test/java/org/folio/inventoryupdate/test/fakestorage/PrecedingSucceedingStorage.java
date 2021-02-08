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
    public void createRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        fakeStorage.instanceStorage.getRecords();
        TestInstanceTitleSuccession titleSuccession = new TestInstanceTitleSuccession(recordJson);
        if (validateCreate(titleSuccession)) {
            int code = insert(new TestInstanceTitleSuccession(recordJson));
            respond(routingContext, recordJson, code);
        } else {
            respondWithMessage(routingContext, new Throwable("Could not create title succession because referenced TestInstance does not exists"));
        }
    }

    public boolean validateCreate (TestInstanceTitleSuccession titleSuccession) {
        if (fakeStorage.instanceStorage.records.get(titleSuccession.getPrecedingInstanceId()) != null
           && fakeStorage.instanceStorage.records.get(titleSuccession.getSucceedingInstanceId()) != null) {
            return true;
        } else {
            return false;
        }
    }

}
