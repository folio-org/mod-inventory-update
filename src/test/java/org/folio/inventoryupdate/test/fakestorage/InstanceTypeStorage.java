package org.folio.inventoryupdate.test.fakestorage;

public class InstanceTypeStorage extends RecordStorage {
  @Override
  protected String getResultSetName() {
    return INSTANCE_TYPES;
  }

  @Override
  protected void declareDependencies() {

  }

  @Override
  protected void declareMandatoryProperties() {

  }

}
