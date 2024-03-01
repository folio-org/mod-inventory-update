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

  public JsonObject getInstanceInstructions() {
    if (!processingInstructions.containsKey(ProcessingInstructions.INSTANCE_INSTRUCTIONS_KEY)) {
      setInstanceInstructions();
    }
    return processingInstructions.getJsonObject(ProcessingInstructions.INSTANCE_INSTRUCTIONS_KEY);
  }

  public void setInstanceInstructions() {
    processingInstructions.put(ProcessingInstructions.INSTANCE_INSTRUCTIONS_KEY, new JsonObject());
  }

  public InputProcessingInstructions setInstancePropertiesToRetain(String ...propertyNames) {
    JsonArray propNames = new JsonArray();
    for (String prop : propertyNames) {
      propNames.add(prop);
    }
    if (!getInstanceInstructions().containsKey(ProcessingInstructions.VALUE_RETENTION_KEY)) {
      getInstanceInstructions().put(ProcessingInstructions.VALUE_RETENTION_KEY, new JsonObject());
    }
    getInstanceInstructions().getJsonObject(ProcessingInstructions.VALUE_RETENTION_KEY)
        .put(ProcessingInstructions.SPECIFIC_PROPERTIES_RETENTION_KEY, propNames);
    return this;
  }

  public InputProcessingInstructions setHoldingsRecordPropertiesToRetain(String ...propertyNames) {
    JsonArray propNames = new JsonArray();
    for (String prop : propertyNames) {
      propNames.add(prop);
    }
    if (!getHoldingsRecordInstructions().containsKey(ProcessingInstructions.VALUE_RETENTION_KEY)) {
      getHoldingsRecordInstructions().put(ProcessingInstructions.VALUE_RETENTION_KEY, new JsonObject());
    }
    getHoldingsRecordInstructions().getJsonObject(ProcessingInstructions.VALUE_RETENTION_KEY)
        .put(ProcessingInstructions.SPECIFIC_PROPERTIES_RETENTION_KEY, propNames);
    return this;

  }

  public InputProcessingInstructions setRetainOmittedInstanceProperties (boolean on) {
    if (!getInstanceInstructions().containsKey(ProcessingInstructions.VALUE_RETENTION_KEY)) {
      getInstanceInstructions().put(ProcessingInstructions.VALUE_RETENTION_KEY, new JsonObject());
    }
    getInstanceInstructions().getJsonObject(ProcessingInstructions.VALUE_RETENTION_KEY)
        .put(ProcessingInstructions.OMITTED_PROPERTIES_RETENTION_KEY,on);
    return this;

  }

  public InputProcessingInstructions setRetainOmittedHoldingsRecordProperties (boolean on) {
    if (!getHoldingsRecordInstructions().containsKey(ProcessingInstructions.VALUE_RETENTION_KEY)) {
      getHoldingsRecordInstructions().put(ProcessingInstructions.VALUE_RETENTION_KEY, new JsonObject());
    }
    getHoldingsRecordInstructions().getJsonObject(ProcessingInstructions.VALUE_RETENTION_KEY)
        .put(ProcessingInstructions.OMITTED_PROPERTIES_RETENTION_KEY,on);
    return this;
  }

  public InputProcessingInstructions setRetainOmittedItemProperties (boolean on) {
    if (!getItemInstructions().containsKey(ProcessingInstructions.VALUE_RETENTION_KEY)) {
      getItemInstructions().put(ProcessingInstructions.VALUE_RETENTION_KEY, new JsonObject());
    }

    getItemInstructions().getJsonObject(ProcessingInstructions.VALUE_RETENTION_KEY)
        .put(ProcessingInstructions.OMITTED_PROPERTIES_RETENTION_KEY,on);
    return this;
  }

  public InputProcessingInstructions setItemPropertiesToRetain(String ...propertyNames) {
    JsonArray propNames = new JsonArray();
    for (String prop : propertyNames) {
      propNames.add(prop);
    }
    if (!getItemInstructions().containsKey(ProcessingInstructions.VALUE_RETENTION_KEY)) {
      getItemInstructions().put(ProcessingInstructions.VALUE_RETENTION_KEY, new JsonObject());
    }
    getItemInstructions().getJsonObject(ProcessingInstructions.VALUE_RETENTION_KEY)
        .put(ProcessingInstructions.SPECIFIC_PROPERTIES_RETENTION_KEY, propNames);
    return this;
  }

  public InputProcessingInstructions setItemRecordRetentionCriterion(String fieldName, String pattern) {
    if (!getItemInstructions().containsKey(ProcessingInstructions.RECORD_RETENTION_KEY)) {
      getItemInstructions().put(ProcessingInstructions.RECORD_RETENTION_KEY, new JsonObject());
    }
    getItemInstructions().getJsonObject(ProcessingInstructions.RECORD_RETENTION_KEY)
        .put(ProcessingInstructions.RECORD_RETENTION_CRITERION_FIELD, fieldName)
        .put(ProcessingInstructions.RECORD_RETENTION_CRITERION_PATTERN, pattern);
    return this;
  }

  public JsonObject getJson() {
    return processingInstructions;
  }
}
