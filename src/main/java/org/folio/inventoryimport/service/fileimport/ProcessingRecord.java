package org.folio.inventoryimport.service.fileimport;

import io.vertx.core.json.JsonObject;

public class ProcessingRecord {
    private final String original;
    private String record;
    private boolean isDeletion = false;

    public ProcessingRecord(String original) {
        this.original = original;
        record = original;
    }

    public void update(String record) {
        this.record = record;
    }

    public String getRecordAsString() {
        return record;
    }

    public JsonObject getRecordAsJson() {
        return new JsonObject(record);
    }

    public String getOriginalRecordAsString() {
        return original;
    }

    public void setBatchIndex(int index) {
        JsonObject json = new JsonObject(record);
        if (!json.containsKey("processing")) {
            json.put("processing", new JsonObject());
        }
        json.getJsonObject("processing").put("batchIndex", index);
        record = json.encode();
    }

    public boolean isDeletion() {
        return isDeletion;
    }

    public void setIsDeletion(boolean isDeletion) {
        this.isDeletion = isDeletion;
    }


}
