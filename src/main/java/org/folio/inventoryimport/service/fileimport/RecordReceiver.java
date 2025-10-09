package org.folio.inventoryimport.service.fileimport;

public interface RecordReceiver {
    void put(ProcessingRecord record);

    void endOfDocument();
}
