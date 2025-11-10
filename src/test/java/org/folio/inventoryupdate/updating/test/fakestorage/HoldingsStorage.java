package org.folio.inventoryupdate.updating.test.fakestorage;

public class HoldingsStorage extends RecordStorage {

    @Override
    public String getResultSetName() {
        return HOLDINGS_RECORDS;
    }

    @Override
    protected void declareDependencies() {
        fakeStorageForUpserts.instanceStorage.acceptDependant(this, "instanceId");
        fakeStorageForUpserts.locationStorage.acceptDependant(this, "permanentLocationId");
    }

    @Override
    protected void declareMandatoryProperties() {
        mandatoryProperties.add("permanentLocationId");
    }

}
