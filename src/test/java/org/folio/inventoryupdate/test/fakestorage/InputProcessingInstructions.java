package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.ProcessingInstructions;

public class InputProcessingInstructions {
  JsonObject processingInstructions = new JsonObject();

  public InputProcessingInstructions () {
  }

  private void setItemInstructions() {
    processingInstructions.put(ProcessingInstructions.ITEM_INSTRUCTIONS_KEY,new JsonObject());
  }

  public JsonObject getItemInstructions() {
    if (!processingInstructions.containsKey(ProcessingInstructions.ITEM_INSTRUCTIONS_KEY)) {
      setItemInstructions();
    }
    return processingInstructions.getJsonObject(ProcessingInstructions.ITEM_INSTRUCTIONS_KEY);
  }

  private void setItemStatusInstructions() {
    getItemInstructions().put(ProcessingInstructions.ITEM_STATUS_INSTRUCTION_KEY, new JsonObject());
  }

  public JsonObject getItemStatusInstructions() {
    if (!getItemInstructions().containsKey(ProcessingInstructions.ITEM_STATUS_INSTRUCTION_KEY)) {
      setItemStatusInstructions();
    }
    return processingInstructions
        .getJsonObject(ProcessingInstructions.ITEM_INSTRUCTIONS_KEY)
        .getJsonObject(ProcessingInstructions.ITEM_STATUS_INSTRUCTION_KEY);
  }

  public InputProcessingInstructions setItemStatusPolicy(String policy) {
      getItemStatusInstructions()
          .put(ProcessingInstructions.ITEM_STATUS_POLICY_KEY, policy);
    return this;
  }

  public InputProcessingInstructions setListOfStatuses(String ...statuses) {
    JsonArray previousStatuses = new JsonArray();
    for (String status : statuses) {
      JsonObject stat = new JsonObject();
      stat.put(ProcessingInstructions.ITEM_STATUS_NAME_KEY,status);
      previousStatuses.add(stat);
    }
    getItemStatusInstructions().put(ProcessingInstructions.ITEM_STATUS_POLICY_APPLIES_TO_KEY, previousStatuses);
    return this;
  }

  private void setHoldingsRecordInstructions() {
    processingInstructions.put(ProcessingInstructions.HOLDINGS_INSTRUCTIONS_KEY,new JsonObject());
  }

  public JsonObject getHoldingsRecordInstructions() {
    if (!processingInstructions.containsKey(ProcessingInstructions.HOLDINGS_INSTRUCTIONS_KEY)) {
      setHoldingsRecordInstructions();
    }
    return processingInstructions.getJsonObject(ProcessingInstructions.HOLDINGS_INSTRUCTIONS_KEY);
  }

  public InputProcessingInstructions setHoldingsRecordPropertiesToRetain(String ...propertyNames) {
    JsonArray propNames = new JsonArray();
    for (String prop : propertyNames) {
      propNames.add(prop);
    }
    getHoldingsRecordInstructions().put(ProcessingInstructions.HOLDINGS_RECORD_RETENTION_KEY, propNames);
    return this;

  }

  public InputProcessingInstructions setItemPropertiesToRetain(String ...propertyNames) {
    JsonArray propNames = new JsonArray();
    for (String prop : propertyNames) {
      propNames.add(prop);
    }
    getItemInstructions().put(ProcessingInstructions.ITEM_RETENTION_KEY, propNames);
    return this;
  }

  public JsonObject getJson() {
    return processingInstructions;
  }
}
