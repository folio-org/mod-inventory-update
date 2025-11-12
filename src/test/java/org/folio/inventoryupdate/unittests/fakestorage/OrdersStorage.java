package org.folio.inventoryupdate.unittests.fakestorage;

public class OrdersStorage extends RecordStorage{
  @Override
  protected String getResultSetName() {
    return PO_LINES;
  }

}
