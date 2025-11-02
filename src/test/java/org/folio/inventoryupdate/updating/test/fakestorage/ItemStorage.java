package org.folio.inventoryupdate.updating.test.fakestorage;

import org.folio.inventoryupdate.updating.test.fakestorage.entitites.InputItem;

public class ItemStorage extends RecordStorage {

    @Override
    protected String getResultSetName() {
        return ITEMS;
    }

    @Override
    protected void declareDependencies() {
        fakeStorageForUpserts.holdingsStorage.acceptDependant(this, InputItem.HOLDINGS_RECORD_ID);
    }

    @Override
    protected void declareMandatoryProperties() {
        mandatoryProperties.add("status");
        mandatoryProperties.add("materialTypeId");
    }

}
