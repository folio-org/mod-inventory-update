package org.folio.inventoryupdate.importing.test.fakestorage;

public class HoldingsStorage extends RecordStorage{
  @Override
  public String getResultSetName() {
    return HOLDINGS_RECORDS;
  }

  @Override
  protected void declareDependencies() {
    fakeStorageForImporting.instanceStorage.acceptDependant(this, "instanceId");
    fakeStorageForImporting.locationStorage.acceptDependant(this, "permanentLocationId");
  }

  @Override
  protected void declareMandatoryProperties() {
    mandatoryProperties.add("permanentLocationId");
  }
}
