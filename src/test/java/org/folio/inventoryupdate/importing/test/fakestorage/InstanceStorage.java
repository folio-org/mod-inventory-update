package org.folio.inventoryupdate.importing.test.fakestorage;

public class InstanceStorage extends RecordStorage{
  public String getResultSetName() {
    return INSTANCES;
  }
  @Override
  protected void declareMandatoryProperties() {
    mandatoryProperties.add("source");
  }
  @Override
  protected void declareUniqueProperties() {
    uniqueProperties.add("hrid");
  }

}
