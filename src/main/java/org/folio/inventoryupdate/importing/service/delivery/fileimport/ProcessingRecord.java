package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.json.JsonObject;
import java.util.Map;

public class ProcessingRecord {
  private final String original;
  private String theRecord;
  private boolean isDeletion = false;
  private final Map<String, String> prefixMappings;

  public ProcessingRecord(String original, Map<String, String> prefixes) {
    this.original = original;
    theRecord = original;
    prefixMappings = prefixes;
  }

  public void update(String theRecord) {
    this.theRecord = theRecord;
  }

  public String getRecordAsString() {
    return theRecord;
  }
  public String getCollectionOfOneRecordAsString() {
    StringBuilder collection = new StringBuilder();
    collection.append("<collection ");
    for (Map.Entry<String, String> entry : prefixMappings.entrySet()) {
      if (entry.getKey().isEmpty()) {
        collection.append("xmlns").append("=\"").append(entry.getValue()).append("\"");
      } else {
        collection.append("xmlns:").append(entry.getKey()).append("=\"").append(entry.getValue()).append("\"");
      }
    }
    collection.append(">");
    return collection.append(theRecord).append("</collection>").toString();
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
