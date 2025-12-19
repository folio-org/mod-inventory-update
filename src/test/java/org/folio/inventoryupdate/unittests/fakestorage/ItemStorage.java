package org.folio.inventoryupdate.unittests.fakestorage;


import org.folio.inventoryupdate.unittests.fakestorage.entities.InputItem;

public class ItemStorage extends RecordStorage {
  @Override
  protected String getResultSetName() {
    return ITEMS;
  }

  @Override
  protected void declareDependencies() {
    fakeStorageForImporting.holdingsStorage.acceptDependant(this, InputItem.HOLDINGS_RECORD_ID);
  }

  @Override
  protected void declareMandatoryProperties() {
    mandatoryProperties.add("status");
    mandatoryProperties.add("materialTypeId");
  }

}
