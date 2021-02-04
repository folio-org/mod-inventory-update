package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class PrecedingSucceedingStorage extends RecordStorage {

    public String getResultSetName () {
        return PRECEDING_SUCCEEDING_TITLES;
    }
    public void createPrecedingSucceedingTitle(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        fakeStorage.instanceStorage.getRecords();
        InstanceTitleSuccession titleSuccession = new InstanceTitleSuccession(recordJson);
        if (validateCreate(titleSuccession)) {
            int code = insert(new InstanceTitleSuccession(recordJson));
            respond(routingContext, recordJson, code);
        } else {
            respondWithMessage(routingContext, new Throwable("Could not create title succession because referenced Instance does not exists"));
        }
    }

    public boolean validateCreate (InstanceTitleSuccession titleSuccession) {
        if (fakeStorage.instanceStorage.records.get(titleSuccession.getPrecedingInstanceId()) != null
           && fakeStorage.instanceStorage.records.get(titleSuccession.getSucceedingInstanceId()) != null) {
            return true;
        } else {
            return false;
        }
    }

}
