package org.folio.inventoryupdate.importing.service.delivery.fileimport;

public interface RecordReceiver {
  void put(ProcessingRecord processingRecord);

  void endOfDocument();

  long getProcessingTime();

  int getRecordsProcessed();
}
