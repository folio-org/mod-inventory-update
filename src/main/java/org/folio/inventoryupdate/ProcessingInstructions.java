package org.folio.inventoryupdate;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class ProcessingInstructions {
  JsonObject processing;
  public static final String ITEM_STATUS_RETAIN = "retain";
  public static final String ITEM_STATUS_OVERWRITE = "overwrite";
  public static final String ITEM_STATUS_OVERWRITE_STATUSES_IN_LIST = "overwrite-statuses-in-list";
  public static final String ITEM_STATUS_RETAIN_STATUSES_IN_LIST = "retain-statuses-in-list";
  public static final String ITEM_UPDATES_KEY = "itemUpdates";
  public static final String ITEM_UPDATES_STATUS_KEY = "itemStatus";
  public static final String ITEM_UPDATES_STATUS_INSTRUCTION_KEY = "instruction";
  public static final String ITEM_UPDATES_STATUS_PREVIOUS_STATUSES_KEY = "previousStatuses";
  public static final String ITEM_UPDATES_STATUS_PREVIOUS_STATUSES_NAME_KEY = "name";

  public ProcessingInstructions (JsonObject processing) {
    this.processing = processing;
  }

  private JsonObject getItemStatusProcessing () {
    if (processing != null
            && processing.containsKey(ITEM_UPDATES_KEY)
            && processing.getJsonObject(ITEM_UPDATES_KEY).containsKey(ITEM_UPDATES_STATUS_KEY)) {
      return processing.getJsonObject(ITEM_UPDATES_KEY)
              .getJsonObject(ITEM_UPDATES_STATUS_KEY);
    } else {
      return null;
    }
  }

  private boolean hasItemStatusProcessing () {
    return (getItemStatusProcessing() != null);
  }

  private boolean itemStatusInstructionIsOverwrite() {
    return ITEM_STATUS_OVERWRITE.equalsIgnoreCase(getItemStatusUpdateInstruction());
  }

  private boolean itemStatusInstructionIsRetain() {
    return ITEM_STATUS_RETAIN.equalsIgnoreCase(getItemStatusUpdateInstruction());
  }

  private boolean itemStatusInstructionIsOnlyUpdateThese() {
    return ITEM_STATUS_OVERWRITE_STATUSES_IN_LIST
            .equalsIgnoreCase(getItemStatusUpdateInstruction());
  }

  private boolean itemStatusInstructionIsDoNotUpdateThese() {
    return ITEM_STATUS_RETAIN_STATUSES_IN_LIST
            .equalsIgnoreCase(getItemStatusUpdateInstruction());
  }

  private String getItemStatusUpdateInstruction () {
    if (hasItemStatusProcessing()) {
      return getItemStatusProcessing().getString(ITEM_UPDATES_STATUS_INSTRUCTION_KEY);
   }
    return null;
  }

  public boolean retainThisStatus(String statusName) {
    if (itemStatusInstructionIsRetain()) {
      return true;
    } else if (itemStatusInstructionIsOverwrite()) {
      return false;
    } else if (itemStatusInstructionIsOnlyUpdateThese()) {
      return !getListOfStatuses().contains(statusName);
    } else if (itemStatusInstructionIsDoNotUpdateThese()) {
      return getListOfStatuses().contains(statusName);
    } else {
      return false;
    }
  }

  private List<String> getListOfStatuses () {
    List<String> statuses = new ArrayList<>();
    if (hasItemStatusProcessing()) {
      JsonArray itemStatuses = getItemStatusProcessing()
              .getJsonArray(ITEM_UPDATES_STATUS_PREVIOUS_STATUSES_KEY);
      for (Object o : itemStatuses) {
        statuses.add(((JsonObject) o).getString(ITEM_UPDATES_STATUS_PREVIOUS_STATUSES_NAME_KEY));
      }
    }
    return statuses;
  }
}
