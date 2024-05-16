package org.folio.inventoryupdate;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.InventoryRecord;

public class ProcessingInstructionsDeletion {
  JsonObject processing;
  public static final String INSTANCE_INSTRUCTIONS_KEY = "instance";
  public static final String HOLDINGS_INSTRUCTIONS_KEY = "holdingsRecord";
  public static final String ITEM_INSTRUCTIONS_KEY = "item";
  public static final String BLOCK_DELETION_KEY = "blockDeletion";
  public static final String RECORD_RETENTION_CRITERION_FIELD = "ifField";
  public static final String RECORD_RETENTION_CRITERION_PATTERN = "matchesPattern";

  private final InstanceInstructions instanceInstructions;
  private final ItemInstructions itemInstructions;
  private final HoldingsRecordInstructions holdingsRecordInstructions;

  public ProcessingInstructionsDeletion(JsonObject processing) {
    this.processing = processing;
    instanceInstructions = new InstanceInstructions(processing);
    holdingsRecordInstructions = new HoldingsRecordInstructions(processing);
    itemInstructions = new ItemInstructions(processing);
  }


  ProcessingInstructionsDeletion.InstanceInstructions forInstance() {
    return instanceInstructions;
  }

  ProcessingInstructionsDeletion.HoldingsRecordInstructions forHoldingsRecord() {
    return holdingsRecordInstructions;
  }

  ProcessingInstructionsDeletion.ItemInstructions forItem() {
    return itemInstructions;
  }

  public static class EntityInstructions {
    String key;
    ProcessingInstructionsDeletion.RecordRetention recordRetention;
    JsonObject processing;
    JsonObject entityInstructionsJson;

    public EntityInstructions(JsonObject processing, String entityInstructionsKey) {
      this.processing = processing;
      this.key = entityInstructionsKey;
      if (processing != null) {
        this.entityInstructionsJson = processing.getJsonObject(entityInstructionsKey);
        recordRetention = new ProcessingInstructionsDeletion.RecordRetention(entityInstructionsJson);
      } else {
        recordRetention = new ProcessingInstructionsDeletion.RecordRetention(null);
      }
    }

    public void setDeleteProtectionIfAny(InventoryRecord inventoryRecord) {
      recordRetention.setDeleteProtection(inventoryRecord);
    }
  }


  public static class RecordRetention {

    String field = "~";
    String pattern = "~";
    RecordRetention(JsonObject json) {
      if (json != null) {
        JsonObject recordRetention = json.getJsonObject(BLOCK_DELETION_KEY);
        if (recordRetention != null) {
          field = recordRetention.getString(RECORD_RETENTION_CRITERION_FIELD,"~");
          pattern = recordRetention.getString(RECORD_RETENTION_CRITERION_PATTERN, "~");
        }
      }
    }

    void setDeleteProtection(InventoryRecord inventoryRecord) {
      if (inventoryRecord.asJson().getString(field) != null
          && inventoryRecord.asJson().getString(field).matches(pattern)) {
         inventoryRecord.registerConstraint(InventoryRecord.DeletionConstraint.IS_DELETE_PROTECTED_PER_INSTRUCTIONS);
      }
    }
  }

  public static class InstanceInstructions extends ProcessingInstructionsDeletion.EntityInstructions {
    InstanceInstructions(JsonObject processing) {
      super(processing, INSTANCE_INSTRUCTIONS_KEY);
    }
  }

  public static class HoldingsRecordInstructions extends ProcessingInstructionsDeletion.EntityInstructions {
    HoldingsRecordInstructions(JsonObject processing) {
      super(processing, HOLDINGS_INSTRUCTIONS_KEY);
    }
  }

  public static class ItemInstructions extends ProcessingInstructionsDeletion.EntityInstructions {
    ItemInstructions(JsonObject processing) {
      super(processing, ITEM_INSTRUCTIONS_KEY);
    }
  }



}
