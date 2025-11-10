package org.folio.inventoryupdate.importing.test.fakestorage;


public class LocationStorage extends RecordStorage {
  @Override
  protected String getResultSetName() {
    return LOCATIONS;
  }
}
