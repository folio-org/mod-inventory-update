package org.folio.inventory.importing.service.fileimport;

public interface RecordReceiver {
    void put(ProcessingRecord record);

    void endOfDocument();
}
