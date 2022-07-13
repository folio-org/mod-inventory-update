package org.folio.inventoryupdate.test.fakestorage;

import org.folio.inventoryupdate.test.fakestorage.entitites.InputItem;
import org.folio.inventoryupdate.test.fakestorage.entitites.InventoryRecord;

public class ItemStorage extends RecordStorage {

    @Override
    protected String getResultSetName() {
        return ITEMS;
    }

    @Override
    protected void declareDependencies() {
        fakeStorage.holdingsStorage.acceptDependant(this, InputItem.HOLDINGS_RECORD_ID);
    }

    @Override
    protected void declareMandatoryProperties() {
        mandatoryProperties.add("status");
        mandatoryProperties.add("materialTypeId");
    }

}
