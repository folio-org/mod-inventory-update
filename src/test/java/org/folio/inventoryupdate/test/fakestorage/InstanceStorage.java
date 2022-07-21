package org.folio.inventoryupdate.test.fakestorage;

public class InstanceStorage extends RecordStorage {

    public String getResultSetName() {
        return INSTANCES;
    }

    @Override
    protected void declareDependencies() {
        // Instances have none in fake storage
    }

    @Override
    protected void declareMandatoryProperties() {
        mandatoryProperties.add("source");
    }

}
