package org.folio.inventoryupdate.importing.service.fileimport;

public interface RecordReceiver {
    void put(ProcessingRecord processingRecord);

    void endOfDocument();
}
