package org.folio.inventoryupdate.importing.service.delivery.fileimport;

public interface RecordProvider {
  void provideRecords() throws ProcessingException;
}
