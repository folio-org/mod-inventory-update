package org.folio.inventoryupdate.instructions;

import io.vertx.core.json.JsonObject;

public class ProcessingInstructionsDeletion {
  JsonObject processing;
  public static final String INSTANCE_INSTRUCTIONS_KEY = "instance";
  public static final String HOLDINGS_INSTRUCTIONS_KEY = "holdingsRecord";
  public static final String ITEM_INSTRUCTIONS_KEY = "item";


  private final InstanceInstructions instanceInstructions;
  private final ItemInstructions itemInstructions;
  private final HoldingsRecordInstructions holdingsRecordInstructions;

  public ProcessingInstructionsDeletion(JsonObject processing) {
    this.processing = processing;
    instanceInstructions = new InstanceInstructions(processing);
    holdingsRecordInstructions = new HoldingsRecordInstructions(processing);
    itemInstructions = new ItemInstructions(processing);
  }

  public ProcessingInstructionsDeletion.InstanceInstructions forInstance() {
    return instanceInstructions;
  }

  public ProcessingInstructionsDeletion.HoldingsRecordInstructions forHoldingsRecord() {
    return holdingsRecordInstructions;
  }

  public ProcessingInstructionsDeletion.ItemInstructions forItem() {
    return itemInstructions;
  }

  public static class EntityInstructions {
    String key;

    private final RecordRetention recordRetention;

    public RecordRetention getRecordRetention() {
      return recordRetention;
    }

    private JsonObject entityInstructionsJson;

    private final StatisticalCoding statisticalCoding;

    public StatisticalCoding getStatisticalCoding() {
      return statisticalCoding;
    }


    public EntityInstructions(JsonObject processing, String entityInstructionsKey) {
      this.key = entityInstructionsKey;
      if (processing != null) {
        this.entityInstructionsJson = processing.getJsonObject(entityInstructionsKey);
        recordRetention = new RecordRetention(entityInstructionsJson);
      } else {
        recordRetention = new RecordRetention(null);
      }
      statisticalCoding = new StatisticalCoding(this.entityInstructionsJson);
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
