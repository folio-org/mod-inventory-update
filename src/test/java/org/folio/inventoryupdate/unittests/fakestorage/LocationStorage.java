package org.folio.inventoryupdate.unittests.fakestorage;


public class LocationStorage extends RecordStorage {
  @Override
  protected String getResultSetName() {
    return LOCATIONS;
  }
}
