package org.folio.inventoryupdate;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Wraps JSON structure like this:
 * <pre>
 * "processing": {
 *   "item": {
 *     "status": {
 *       "policy": "retain" or "overwrite",
 *       "ifStatusIs": [
 *         {"name": "Available"}, {"name": "Checked out"}, ... etc
 *       ]
 *     }
 *   }
 * }
 * </pre>
 */
public class ProcessingInstructions {
  JsonObject processing;
  public static final String ITEM_INSTRUCTIONS_KEY = "item";
  public static final String ITEM_STATUS_INSTRUCTION_KEY = "status";
  public static final String ITEM_STATUS_POLICY_KEY = "policy";
  public static final String ITEM_STATUS_POLICY_RETAIN = "retain";
  public static final String ITEM_STATUS_POLICY_OVERWRITE = "overwrite";
  public static final String ITEM_STATUS_POLICY_APPLIES_TO_KEY = "ifStatusIs";
  public static final String ITEM_STATUS_NAME_KEY = "name";

  public ProcessingInstructions (JsonObject processing) {
    this.processing = processing;
  }

  private JsonObject getItemStatusInstructions() {
    if (processing != null
            && processing.containsKey(ITEM_INSTRUCTIONS_KEY)
            && processing.getJsonObject(ITEM_INSTRUCTIONS_KEY).containsKey(
            ITEM_STATUS_INSTRUCTION_KEY)) {
      return processing.getJsonObject(ITEM_INSTRUCTIONS_KEY)
              .getJsonObject(ITEM_STATUS_INSTRUCTION_KEY);
    } else {
      return null;
    }
  }

  private boolean hasItemStatusInstructions() {
    return ( getItemStatusInstructions() != null);
  }

  private boolean itemStatusPolicyIsOverwrite() {
    return ITEM_STATUS_POLICY_OVERWRITE.equalsIgnoreCase(getItemStatusUpdatePolicy());
  }

  private boolean itemStatusPolicyIsRetain() {
    return ITEM_STATUS_POLICY_RETAIN.equalsIgnoreCase(getItemStatusUpdatePolicy());
  }

  private boolean hasListOfItemStatuses() {
    if (hasItemStatusInstructions()) {
      return getItemStatusInstructions().containsKey(ITEM_STATUS_POLICY_APPLIES_TO_KEY);
    } else {
      return false;
    }
  }

  private String getItemStatusUpdatePolicy() {
    if (hasItemStatusInstructions()) {
      return getItemStatusInstructions().getString(ITEM_STATUS_POLICY_KEY);
   }
    return null;
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

  private List<String> getListOfStatuses () {
    List<String> statuses = new ArrayList<>();
    if (hasItemStatusInstructions()) {
      JsonArray itemStatuses = getItemStatusInstructions()
              .getJsonArray(ITEM_STATUS_POLICY_APPLIES_TO_KEY);
      for (Object o : itemStatuses) {
        statuses.add(((JsonObject) o).getString(ITEM_STATUS_NAME_KEY));
      }
    }
    return statuses;
  }
}
