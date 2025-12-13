package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.json.JsonObject;

public class ProcessingRecord {
  private final String original;
  private String theRecord;
  private boolean isDeletion = false;

  public ProcessingRecord(String original) {
    this.original = original;
    theRecord = original;
  }

  public void update(String theRecord) {
    this.theRecord = theRecord;
  }

  public String getRecordAsString() {
    return theRecord;
  }

  public JsonObject getRecordAsJson() {
    return new JsonObject(theRecord);
  }

  public String getOriginalRecordAsString() {
    return original;
  }

  public void setBatchIndex(int index) {
    JsonObject json = new JsonObject(theRecord);
    if (!json.containsKey("processing")) {
      json.put("processing", new JsonObject());
    }
    json.getJsonObject("processing").put("batchIndex", index);
    theRecord = json.encode();
  }

  public boolean isDeletion() {
    return isDeletion;
  }

  public void setIsDeletion(boolean isDeletion) {
    this.isDeletion = isDeletion;
  }
}
