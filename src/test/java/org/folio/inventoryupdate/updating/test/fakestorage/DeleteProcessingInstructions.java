package org.folio.inventoryupdate.updating.test.fakestorage;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.updating.instructions.ProcessingInstructionsDeletion;


public class DeleteProcessingInstructions {

  public static final String BLOCK_DELETION_KEY = "blockDeletion";
  public static final String RECORD_RETENTION_CRITERION_FIELD = "ifField";
  public static final String RECORD_RETENTION_CRITERION_PATTERN = "matchesPattern";

  JsonObject processingInstructions = new JsonObject();

  public DeleteProcessingInstructions() {
    // Noop constructor
  }

  private void setItemInstructions() {
    processingInstructions.put(ProcessingInstructionsDeletion.ITEM_INSTRUCTIONS_KEY, new JsonObject());
  }

  public JsonObject getItemInstructions() {
    if (!processingInstructions.containsKey(ProcessingInstructionsDeletion.ITEM_INSTRUCTIONS_KEY)) {
      setItemInstructions();
    }
    return processingInstructions.getJsonObject(ProcessingInstructionsDeletion.ITEM_INSTRUCTIONS_KEY);
  }

  private void setHoldingsInstructions() {
    processingInstructions.put(ProcessingInstructionsDeletion.HOLDINGS_INSTRUCTIONS_KEY,new JsonObject());
  }

  public JsonObject getHoldingsInstructions() {
    if (!processingInstructions.containsKey(ProcessingInstructionsDeletion.HOLDINGS_INSTRUCTIONS_KEY)) {
      setHoldingsInstructions();
    }
    return processingInstructions.getJsonObject(ProcessingInstructionsDeletion.HOLDINGS_INSTRUCTIONS_KEY);
  }


  public JsonObject getJson() {
    return processingInstructions;
  }

  public DeleteProcessingInstructions setItemBlockDeletionCriterion(String fieldName, String pattern) {
    if (!getItemInstructions().containsKey(BLOCK_DELETION_KEY)) {
      getItemInstructions().put(BLOCK_DELETION_KEY, new JsonObject());
    }
    getItemInstructions().getJsonObject(BLOCK_DELETION_KEY)
        .put(RECORD_RETENTION_CRITERION_FIELD, fieldName)
        .put(RECORD_RETENTION_CRITERION_PATTERN, pattern);
    return this;
  }

  public DeleteProcessingInstructions setHoldingsBlockDeletionCriterion(String fieldName, String pattern) {
    if (!getHoldingsInstructions().containsKey(BLOCK_DELETION_KEY)) {
      getHoldingsInstructions().put(BLOCK_DELETION_KEY, new JsonObject());
    }
    getHoldingsInstructions().getJsonObject(BLOCK_DELETION_KEY)
        .put(RECORD_RETENTION_CRITERION_FIELD, fieldName)
        .put(RECORD_RETENTION_CRITERION_PATTERN, pattern);
    return this;
  }

}
