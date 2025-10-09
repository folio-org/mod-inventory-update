package org.folio.inventoryimport.service.fileimport;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryimport.foliodata.InventoryUpdateClient;

import java.util.ArrayList;

public class BatchOfRecords {

    private final boolean lastBatchOfFile;
    private final ArrayList<ProcessingRecord> batch;

    private ProcessingRecord deletingRecord;
    private InventoryUpdateClient.UpdateResponse updateResponse;

    public BatchOfRecords(ArrayList<ProcessingRecord> processingRecords, boolean lastBatchOfFile) {
        if (!processingRecords.isEmpty() && processingRecords.getLast().isDeletion()) {
            deletingRecord = processingRecords.removeLast();
        }
        this.batch = processingRecords;
        this.lastBatchOfFile = lastBatchOfFile;
    }

    public boolean hasDeletingRecord () {
        return deletingRecord != null;
    }

    public ProcessingRecord getDeletingRecord() {
        return deletingRecord;
    }

    public boolean isLastBatchOfFile() {
        return lastBatchOfFile;
    }

    public int size() {
        return batch.size();
    }

    public JsonObject getUpsertRequestBody() {
        return new JsonObject().put("inventoryRecordSets", getRecordsAsJsonArray());
    }

    private JsonArray getRecordsAsJsonArray () {
        JsonArray inventoryRecordSets = new JsonArray();
        for (ProcessingRecord record : batch) {
            inventoryRecordSets.add(record.getRecordAsJson());
        }
        return inventoryRecordSets;
    }

    public void setResponse(InventoryUpdateClient.UpdateResponse updateResponse) {
        this.updateResponse = updateResponse;
    }

    public JsonArray getErrors() {
        return updateResponse.getErrors();
    }


    public ProcessingRecord get(int index) {
        return batch.get(index);
    }
 }
