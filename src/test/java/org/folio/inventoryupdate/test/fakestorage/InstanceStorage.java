package org.folio.inventoryupdate.test.fakestorage;

public class InstanceStorage extends RecordStorage {

    public String getResultSetName() {
        return INSTANCES;
    }

    @Override
    protected void declareDependencies() {
        // Instances has none in fake storage
    }

}
