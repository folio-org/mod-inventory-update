package org.folio.inventoryupdate.unittests.fakestorage;

public class HoldingsStorage extends RecordStorage{
  @Override
  public String getResultSetName() {
    return HOLDINGS_RECORDS;
  }

  @Override
  protected void declareDependencies() {
    fakeStorageForImporting.instanceStorage.acceptDependant(this, "instanceId", false);
    fakeStorageForImporting.locationStorage.acceptDependant(this, "permanentLocationId", false);
  }

  @Override
  protected void declareMandatoryProperties() {
    mandatoryProperties.add("permanentLocationId");
  }
}
