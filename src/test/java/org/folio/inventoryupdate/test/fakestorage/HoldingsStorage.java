package org.folio.inventoryupdate.test.fakestorage;

public class HoldingsStorage extends RecordStorage {

    @Override
    public String getResultSetName() {
        return HOLDINGS_RECORDS;
    }

    @Override
    protected void declareDependencies() {
        fakeStorage.instanceStorage.acceptDependant(this, "instanceId", false);
        fakeStorage.locationStorage.acceptDependant(this, "permanentLocationId", false);
    }

    @Override
    protected void declareMandatoryProperties() {
        mandatoryProperties.add("permanentLocationId");
    }

}
