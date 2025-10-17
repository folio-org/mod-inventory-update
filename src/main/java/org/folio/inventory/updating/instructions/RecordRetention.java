package org.folio.inventory.updating.instructions;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.updating.entities.InventoryRecord;

public class RecordRetention {
  public static final String BLOCK_DELETION_KEY = "blockDeletion";
  public static final String RETAIN_OMITTED_RECORD_KEY = "retainOmittedRecord";
  public static final String RECORD_RETENTION_CRITERION_FIELD = "ifField";
  public static final String RECORD_RETENTION_CRITERION_PATTERN = "matchesPattern";

  String field = "~";
  String pattern = "~";

  RecordRetention(JsonObject json) {
    if (json != null) {
      JsonObject recordRetention = null;
      if (json.containsKey(BLOCK_DELETION_KEY)) {
        recordRetention = json.getJsonObject(BLOCK_DELETION_KEY);
      } else if (json.containsKey(RETAIN_OMITTED_RECORD_KEY)) {
        recordRetention = json.getJsonObject(RETAIN_OMITTED_RECORD_KEY);
      }
      if (recordRetention != null) {
        field = recordRetention.getString(RECORD_RETENTION_CRITERION_FIELD,"~");
        pattern = recordRetention.getString(RECORD_RETENTION_CRITERION_PATTERN, "~");
      }
    }
  }

  public boolean isDeleteProtectedByPatternMatch(InventoryRecord inventoryRecord) {
    return (inventoryRecord.asJson().getString(field) != null
        && inventoryRecord.asJson().getString(field).matches(pattern));
  }
}
