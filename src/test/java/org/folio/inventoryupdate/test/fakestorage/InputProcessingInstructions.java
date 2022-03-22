package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.ProcessingInstructions;

public class InputProcessingInstructions {
  JsonObject processingInstructions = new JsonObject();

  public InputProcessingInstructions () {
    setItemInstructions().setItemStatusInstructions();
  }

  public InputProcessingInstructions setItemInstructions() {
    processingInstructions.put(ProcessingInstructions.ITEM_INSTRUCTIONS_KEY,new JsonObject());
    return this;
  }

  public InputProcessingInstructions setItemStatusInstructions() {
    processingInstructions.getJsonObject(ProcessingInstructions.ITEM_INSTRUCTIONS_KEY)
            .put(ProcessingInstructions.ITEM_STATUS_INSTRUCTION_KEY, new JsonObject());
    return this;
  }

  public InputProcessingInstructions setItemStatusPolicy(String policy) {
      processingInstructions.getJsonObject(ProcessingInstructions.ITEM_INSTRUCTIONS_KEY)
              .getJsonObject(ProcessingInstructions.ITEM_STATUS_INSTRUCTION_KEY)
              .put(ProcessingInstructions.ITEM_STATUS_POLICY_KEY, policy);
    return this;
  }

  public InputProcessingInstructions setListOfStatuses(String ...statuses) {
    if (processingInstructions.containsKey(ProcessingInstructions.ITEM_INSTRUCTIONS_KEY) &&
        processingInstructions.getJsonObject(ProcessingInstructions.ITEM_INSTRUCTIONS_KEY).containsKey(ProcessingInstructions.ITEM_STATUS_INSTRUCTION_KEY)) {
      JsonArray previousStatuses = new JsonArray();
      for (String status : statuses) {
        JsonObject stat = new JsonObject();
        stat.put(ProcessingInstructions.ITEM_STATUS_NAME_KEY,status);
        previousStatuses.add(stat);
      }
      processingInstructions.getJsonObject(ProcessingInstructions.ITEM_INSTRUCTIONS_KEY)
              .getJsonObject(ProcessingInstructions.ITEM_STATUS_INSTRUCTION_KEY).put(ProcessingInstructions.ITEM_STATUS_POLICY_APPLIES_TO_KEY, previousStatuses);
    }
    return this;
  }

  public JsonObject getJson() {
    return processingInstructions;
  }
}
