package org.folio.inventoryupdate.unittests.fakestorage;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.updating.instructions.ProcessingInstructionsUpsert;

public class InputProcessingInstructions {
  JsonObject processingInstructions = new JsonObject();

  public InputProcessingInstructions() {
    // Noop constructor.
  }

  private void setItemInstructions() {
    processingInstructions.put(ProcessingInstructionsUpsert.ITEM_INSTRUCTIONS_KEY,new JsonObject());
  }

  public JsonObject getItemInstructions() {
    if (!processingInstructions.containsKey(ProcessingInstructionsUpsert.ITEM_INSTRUCTIONS_KEY)) {
      setItemInstructions();
    }
    return processingInstructions.getJsonObject(ProcessingInstructionsUpsert.ITEM_INSTRUCTIONS_KEY);
  }

  private void setItemStatusInstructions() {
    getItemInstructions().put(ProcessingInstructionsUpsert.ITEM_STATUS_INSTRUCTION_KEY, new JsonObject());
  }

  public JsonObject getItemStatusInstructions() {
    if (!getItemInstructions().containsKey(ProcessingInstructionsUpsert.ITEM_STATUS_INSTRUCTION_KEY)) {
      setItemStatusInstructions();
    }
    return processingInstructions
        .getJsonObject(ProcessingInstructionsUpsert.ITEM_INSTRUCTIONS_KEY)
        .getJsonObject(ProcessingInstructionsUpsert.ITEM_STATUS_INSTRUCTION_KEY);
  }

  public InputProcessingInstructions setItemStatusPolicy(String policy) {
      getItemStatusInstructions()
          .put(ProcessingInstructionsUpsert.ITEM_STATUS_POLICY_KEY, policy);
    return this;
  }

  public InputProcessingInstructions setListOfStatuses(String ...statuses) {
    JsonArray previousStatuses = new JsonArray();
    for (String status : statuses) {
      JsonObject stat = new JsonObject();
      stat.put(ProcessingInstructionsUpsert.ITEM_STATUS_NAME_KEY,status);
      previousStatuses.add(stat);
    }
    getItemStatusInstructions().put(ProcessingInstructionsUpsert.ITEM_STATUS_POLICY_APPLIES_TO_KEY, previousStatuses);
    return this;
  }

  private void setHoldingsRecordInstructions() {
    processingInstructions.put(ProcessingInstructionsUpsert.HOLDINGS_INSTRUCTIONS_KEY,new JsonObject());
  }

  public JsonObject getHoldingsRecordInstructions() {
    if (!processingInstructions.containsKey(ProcessingInstructionsUpsert.HOLDINGS_INSTRUCTIONS_KEY)) {
      setHoldingsRecordInstructions();
    }
    return processingInstructions.getJsonObject(ProcessingInstructionsUpsert.HOLDINGS_INSTRUCTIONS_KEY);
  }

  public JsonObject getInstanceInstructions() {
    if (!processingInstructions.containsKey(ProcessingInstructionsUpsert.INSTANCE_INSTRUCTIONS_KEY)) {
      setInstanceInstructions();
    }
    return processingInstructions.getJsonObject(ProcessingInstructionsUpsert.INSTANCE_INSTRUCTIONS_KEY);
  }

  public void setInstanceInstructions() {
    processingInstructions.put(ProcessingInstructionsUpsert.INSTANCE_INSTRUCTIONS_KEY, new JsonObject());
  }

  public InputProcessingInstructions setInstancePropertiesToRetain(String ...propertyNames) {
    JsonArray propNames = new JsonArray();
    for (String prop : propertyNames) {
      propNames.add(prop);
    }
    if (!getInstanceInstructions().containsKey(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)) {
      getInstanceInstructions().put(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY, new JsonObject());
    }
    getInstanceInstructions().getJsonObject(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)
        .put(ProcessingInstructionsUpsert.SPECIFIC_PROPERTIES_RETENTION_KEY, propNames);
    return this;
  }

  public InputProcessingInstructions setHoldingsRecordPropertiesToRetain(String ...propertyNames) {
    JsonArray propNames = new JsonArray();
    for (String prop : propertyNames) {
      propNames.add(prop);
    }
    if (!getHoldingsRecordInstructions().containsKey(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)) {
      getHoldingsRecordInstructions().put(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY, new JsonObject());
    }
    getHoldingsRecordInstructions().getJsonObject(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)
        .put(ProcessingInstructionsUpsert.SPECIFIC_PROPERTIES_RETENTION_KEY, propNames);
    return this;

  }

  public InputProcessingInstructions setRetainOmittedInstanceProperties (boolean on) {
    if (!getInstanceInstructions().containsKey(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)) {
      getInstanceInstructions().put(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY, new JsonObject());
    }
    getInstanceInstructions().getJsonObject(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)
        .put(ProcessingInstructionsUpsert.OMITTED_PROPERTIES_RETENTION_KEY,on);
    return this;

  }

  public InputProcessingInstructions setRetainOmittedHoldingsRecordProperties (boolean on) {
    if (!getHoldingsRecordInstructions().containsKey(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)) {
      getHoldingsRecordInstructions().put(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY, new JsonObject());
    }
    getHoldingsRecordInstructions().getJsonObject(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)
        .put(ProcessingInstructionsUpsert.OMITTED_PROPERTIES_RETENTION_KEY,on);
    return this;
  }

  public InputProcessingInstructions setRetainOmittedItemProperties (boolean on) {
    if (!getItemInstructions().containsKey(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)) {
      getItemInstructions().put(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY, new JsonObject());
    }

    getItemInstructions().getJsonObject(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)
        .put(ProcessingInstructionsUpsert.OMITTED_PROPERTIES_RETENTION_KEY,on);
    return this;
  }

  public InputProcessingInstructions setItemPropertiesToRetain(String ...propertyNames) {
    JsonArray propNames = new JsonArray();
    for (String prop : propertyNames) {
      propNames.add(prop);
    }
    if (!getItemInstructions().containsKey(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)) {
      getItemInstructions().put(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY, new JsonObject());
    }
    getItemInstructions().getJsonObject(ProcessingInstructionsUpsert.VALUE_RETENTION_KEY)
        .put(ProcessingInstructionsUpsert.SPECIFIC_PROPERTIES_RETENTION_KEY, propNames);
    return this;
  }

  public InputProcessingInstructions setItemRecordRetentionCriterion(String fieldName, String pattern) {
    if (!getItemInstructions().containsKey(ProcessingInstructionsUpsert.RECORD_RETENTION_KEY)) {
      getItemInstructions().put(ProcessingInstructionsUpsert.RECORD_RETENTION_KEY, new JsonObject());
    }
    getItemInstructions().getJsonObject(ProcessingInstructionsUpsert.RECORD_RETENTION_KEY)
        .put(ProcessingInstructionsUpsert.RECORD_RETENTION_CRITERION_FIELD, fieldName)
        .put(ProcessingInstructionsUpsert.RECORD_RETENTION_CRITERION_PATTERN, pattern);
    return this;
  }

  public InputProcessingInstructions setItemStatisticalCoding(JsonArray codings) {
    getItemInstructions().put("statisticalCoding", codings);
    return this;
  }

  public InputProcessingInstructions setHoldingsRecordRetentionCriterion(String fieldName, String pattern) {
    if (!getHoldingsRecordInstructions().containsKey(ProcessingInstructionsUpsert.RECORD_RETENTION_KEY)) {
      getHoldingsRecordInstructions().put(ProcessingInstructionsUpsert.RECORD_RETENTION_KEY, new JsonObject());
    }
    getHoldingsRecordInstructions().getJsonObject(ProcessingInstructionsUpsert.RECORD_RETENTION_KEY)
        .put(ProcessingInstructionsUpsert.RECORD_RETENTION_CRITERION_FIELD, fieldName)
        .put(ProcessingInstructionsUpsert.RECORD_RETENTION_CRITERION_PATTERN, pattern);
    return this;
  }

  public InputProcessingInstructions setHoldingsRecordStatisticalCoding(JsonArray codings) {
    getHoldingsRecordInstructions().put("statisticalCoding", codings);
    return this;

  }


  public JsonObject getJson() {
    return processingInstructions;
  }
}
