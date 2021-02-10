package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.ext.web.RoutingContext;

public class LocationStorage extends RecordStorage {
    @Override
    protected String getResultSetName() {
        return LOCATIONS;
    }

    @Override
    protected void declareDependencies() {
        // Locations has none in fake storage
    }

    @Override
    protected void createRecord(RoutingContext routingContext) {

    }

    @Override
    protected void updateRecord(RoutingContext routingContext) {

    }
}
