package org.folio.inventoryupdate.test.fakestorage;

public class MaterialTypeStorage extends RecordStorage {

  @Override
  protected String getResultSetName() {
    return MATERIAL_TYPES;
  }

  @Override
  protected void declareDependencies() {

  }

  @Override
  protected void declareMandatoryProperties() {

  }
}
