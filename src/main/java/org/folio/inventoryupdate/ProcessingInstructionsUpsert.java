package org.folio.inventoryupdate;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.InventoryRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Wraps JSON structures as specified by ../ramls/instructions/processing-upsert.json
 */
public class ProcessingInstructionsUpsert {
  JsonObject processing;
  public static final String INSTANCE_INSTRUCTIONS_KEY = "instance";
  public static final String HOLDINGS_INSTRUCTIONS_KEY = "holdingsRecord";
  public static final String ITEM_INSTRUCTIONS_KEY = "item";
  public static final String RECORD_RETENTION_KEY = "retainOmittedRecord";
  public static final String RECORD_RETENTION_CRITERION_FIELD = "ifField";
  public static final String RECORD_RETENTION_CRITERION_PATTERN = "matchesPattern";
  public static final String VALUE_RETENTION_KEY = "retainExistingValues";
  public static final String OMITTED_PROPERTIES_RETENTION_KEY = "forOmittedProperties";
  public static final String SPECIFIC_PROPERTIES_RETENTION_KEY = "forTheseProperties";
  public static final String ITEM_STATUS_INSTRUCTION_KEY = "status";
  public static final String ITEM_STATUS_POLICY_KEY = "policy";
  public static final String ITEM_STATUS_POLICY_RETAIN = "retain";
  public static final String ITEM_STATUS_POLICY_OVERWRITE = "overwrite";
  public static final String ITEM_STATUS_POLICY_APPLIES_TO_KEY = "ifStatusWas";
  public static final String ITEM_STATUS_NAME_KEY = "name";

  private final InstanceInstructions instanceInstructions;
  private final HoldingsRecordInstructions holdingsRecordInstructions;
  private final ItemInstructions itemInstructions;

  public ProcessingInstructionsUpsert(JsonObject processing) {
    this.processing = processing;
    instanceInstructions = new InstanceInstructions(processing);
    holdingsRecordInstructions = new HoldingsRecordInstructions(processing);
    itemInstructions = new ItemInstructions(processing);
  }

  InstanceInstructions forInstance() {
    return instanceInstructions;
  }

  HoldingsRecordInstructions forHoldingsRecord() {
    return holdingsRecordInstructions;
  }

  ItemInstructions forItem() {
    return itemInstructions;
  }

  public static class EntityInstructions {
    String key;
    ValueRetention valueRetention;
    RecordRetention recordRetention;
    JsonObject processing;
    JsonObject entityInstructionsJson;

    EntityInstructions(JsonObject processing, String entityInstructionsKey) {
      this.processing = processing;
      this.key = entityInstructionsKey;
      if (processing != null) {
        this.entityInstructionsJson = processing.getJsonObject(entityInstructionsKey);
        valueRetention = new ValueRetention(entityInstructionsJson);
        recordRetention = new RecordRetention(entityInstructionsJson);
      } else {
        valueRetention = new ValueRetention(null);
        recordRetention = new RecordRetention(null);
      }
    }

    public boolean hasInstructions() {
      return processing != null && processing.containsKey(key);
    }

    public boolean retainOmittedProperties() {
      return valueRetention.forOmittedProperties.equals("true");
    }

    public List<String> retainTheseProperties() {
      return valueRetention.forSpecificProperties;
    }

    public boolean retainOmittedRecord(InventoryRecord inventoryRecord) {
      return recordRetention.retain(inventoryRecord);
    }

  }

  static class ValueRetention {
    String forOmittedProperties = "false";
    List<String> forSpecificProperties = new ArrayList<>();

    ValueRetention(JsonObject json) {
      if (json != null) {
        JsonObject valueRetention = json.getJsonObject(VALUE_RETENTION_KEY);
        if (valueRetention != null) {
          if (valueRetention.containsKey(OMITTED_PROPERTIES_RETENTION_KEY)) {
            forOmittedProperties = valueRetention.getString(OMITTED_PROPERTIES_RETENTION_KEY).toLowerCase();
          }
          if (valueRetention.containsKey(SPECIFIC_PROPERTIES_RETENTION_KEY)) {
            forSpecificProperties = valueRetention.getJsonArray(SPECIFIC_PROPERTIES_RETENTION_KEY)
                .stream().map(Object::toString).toList();
          }
        }
      }
    }
  }

  public static class RecordRetention {

    String field = "~";
    String pattern = "~";
    RecordRetention(JsonObject json) {
      if (json != null) {
        JsonObject recordRetention = json.getJsonObject(RECORD_RETENTION_KEY);
        if (recordRetention != null) {
          field = recordRetention.getString(RECORD_RETENTION_CRITERION_FIELD,"~");
          pattern = recordRetention.getString(RECORD_RETENTION_CRITERION_PATTERN, "~");
        }
      }
    }

    boolean retain (InventoryRecord inventoryRecord) {
      return inventoryRecord.asJson().getString(field) != null
          && inventoryRecord.asJson().getString(field).matches(pattern);
    }

  }

  static class InstanceInstructions extends EntityInstructions  {
    InstanceInstructions(JsonObject processing) {
      super(processing, INSTANCE_INSTRUCTIONS_KEY);
    }
  }

  static class HoldingsRecordInstructions extends EntityInstructions {
    HoldingsRecordInstructions(JsonObject processing) {
      super(processing, HOLDINGS_INSTRUCTIONS_KEY);
    }
  }

  public static class ItemInstructions extends EntityInstructions {
    ItemInstructions(JsonObject processing) {
      super(processing, ITEM_INSTRUCTIONS_KEY);
    }

    public boolean retainThisStatus(String statusName) {
      if (itemStatusPolicyIsRetain()) {
        if (hasListOfItemStatuses()) {
          return getListOfStatuses().contains(statusName);
        } else {
          return true;
        }
      } else if (itemStatusPolicyIsOverwrite()) {
        if (hasListOfItemStatuses()) {
          return !getListOfStatuses().contains(statusName);
        } else {
          return false;
        }
      }
      return false;
    }

    private JsonObject getItemStatusInstructions() {
      if (hasInstructions() && entityInstructionsJson.containsKey(ITEM_STATUS_INSTRUCTION_KEY)) {
        return entityInstructionsJson.getJsonObject(ITEM_STATUS_INSTRUCTION_KEY);
      } else {
        return new JsonObject();
      }
    }

    private boolean itemStatusPolicyIsOverwrite() {
      return ITEM_STATUS_POLICY_OVERWRITE.equalsIgnoreCase(getItemStatusUpdatePolicy());
    }

    private boolean itemStatusPolicyIsRetain() {
      return ITEM_STATUS_POLICY_RETAIN.equalsIgnoreCase(getItemStatusUpdatePolicy());
    }

    private boolean hasListOfItemStatuses() {
      return getItemStatusInstructions().containsKey(ITEM_STATUS_POLICY_APPLIES_TO_KEY);
    }

    private String getItemStatusUpdatePolicy() {
      return getItemStatusInstructions().getString(ITEM_STATUS_POLICY_KEY);
    }

    private List<String> getListOfStatuses () {
      List<String> statuses = new ArrayList<>();
      if (!getItemStatusInstructions().isEmpty()) {
        JsonArray itemStatuses = getItemStatusInstructions()
            .getJsonArray(ITEM_STATUS_POLICY_APPLIES_TO_KEY);
        for (Object o : itemStatuses) {
          statuses.add(((JsonObject) o).getString(ITEM_STATUS_NAME_KEY));
        }
      }
      return statuses;
    }

  }

}
