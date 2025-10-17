package org.folio.inventoryupdate.updating.test.fakestorage;

public class HoldingsStorage extends RecordStorage {

    @Override
    public String getResultSetName() {
        return HOLDINGS_RECORDS;
    }

    @Override
    protected void declareDependencies() {
        fakeStorage.instanceStorage.acceptDependant(this, "instanceId");
        fakeStorage.locationStorage.acceptDependant(this, "permanentLocationId");
    }

    @Override
    protected void declareMandatoryProperties() {
        mandatoryProperties.add("permanentLocationId");
    }

}
