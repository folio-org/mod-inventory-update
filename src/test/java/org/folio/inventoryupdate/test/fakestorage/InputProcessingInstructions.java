package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class InputProcessingInstructions {
  JsonObject processingInstructions = new JsonObject();

  public InputProcessingInstructions () {
    setItemUpdates().setItemStatus();
  }

  public InputProcessingInstructions setItemUpdates () {
    processingInstructions.put("itemUpdates",new JsonObject());
    return this;
  }

  public InputProcessingInstructions setItemStatus () {
    if (processingInstructions.containsKey("itemUpdates")) {
      processingInstructions.getJsonObject("itemUpdates").put("itemStatus", new JsonObject());
    }
    return this;
  }

  public InputProcessingInstructions setItemStatusUpdateInstruction (String instruction) {
    if (processingInstructions.containsKey("itemUpdates") &&
            processingInstructions.getJsonObject("itemUpdates").containsKey("itemStatus")) {
      processingInstructions.getJsonObject("itemUpdates")
              .getJsonObject("itemStatus").put("instruction", instruction);
    }
    return this;
  }

  public InputProcessingInstructions setListOfPreviousStatuses (String ...statuses) {
    if (processingInstructions.containsKey("itemUpdates") &&
        processingInstructions.getJsonObject("itemUpdates").containsKey("itemStatus")) {
      JsonArray previousStatuses = new JsonArray();
      for (String status : statuses) {
        JsonObject stat = new JsonObject();
        stat.put("name",status);
        previousStatuses.add(stat);
      }
      processingInstructions.getJsonObject("itemUpdates")
              .getJsonObject("itemStatus").put("previousStatuses", previousStatuses);
    }
    return this;
  }

  public JsonObject getJson() {
    return processingInstructions;
  }
}
