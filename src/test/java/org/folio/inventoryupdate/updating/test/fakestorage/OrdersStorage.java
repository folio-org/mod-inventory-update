package org.folio.inventoryupdate.updating.test.fakestorage;

public class OrdersStorage extends RecordStorage {
  @Override
  protected String getResultSetName() {
    return PO_LINES;
  }

}
