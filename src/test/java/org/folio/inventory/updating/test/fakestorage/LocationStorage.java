package org.folio.inventory.updating.test.fakestorage;

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
    protected void declareMandatoryProperties() {

    }

}
