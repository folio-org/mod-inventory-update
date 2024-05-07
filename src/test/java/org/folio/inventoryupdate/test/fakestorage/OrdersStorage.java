package org.folio.inventoryupdate.test.fakestorage;

public class OrdersStorage extends RecordStorage {
  @Override
  protected String getResultSetName() {
    return PO_LINES;
  }

  @Override
  protected void declareDependencies() {

  }

  @Override
  protected void declareMandatoryProperties() {

  }
}
