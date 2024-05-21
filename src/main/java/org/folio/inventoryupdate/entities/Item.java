package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.instructions.ProcessingInstructionsUpsert;

import java.util.Arrays;
import java.util.List;

public class Item extends InventoryRecord {

  public static final String HOLDINGS_RECORD_ID = "holdingsRecordId";
  private static final List<String> statusesIndicatingCirculatingItem =
      Arrays.asList("Awaiting delivery",
        "Awaiting pickup",
        "Checked out",
        "Aged to lost",
        "Claimed returned",
        "Declared lost",
        "Paged",
        "In transit");


  public Item(JsonObject item, JsonObject originJson) {
    this.jsonRecord = item;
    entityType = Entity.ITEM;
    this.originJson = originJson;
  }

  public Item(JsonObject item) {
    this.jsonRecord = item;
    entityType = Entity.ITEM;
  }

  public String getHoldingsRecordId() {
    return jsonRecord.getString(HOLDINGS_RECORD_ID);
  }

  public boolean hasHoldingsRecordId() {
    return (getHoldingsRecordId() != null && !getHoldingsRecordId().isEmpty());
  }

  public void setHoldingsRecordId(String uuid) {
    jsonRecord.put(HOLDINGS_RECORD_ID, uuid);
  }

  public void skipDependants() {
    // has none
  }

  public void setStatus(String statusName) {
    jsonRecord.getJsonObject("status").put("name", statusName);
  }

  public String getStatusName() {
    return jsonRecord.getJsonObject("status").getString("name");
  }

  @Override
  public void applyOverlays(InventoryRecord existingRecord, ProcessingInstructionsUpsert.EntityInstructions instr) {
    Item existingItem = (Item) existingRecord;
    ProcessingInstructionsUpsert.ItemInstructions itemInstr = (ProcessingInstructionsUpsert.ItemInstructions) instr;
    if (itemInstr.retainThisStatus(((Item) existingRecord).getStatusName())) {
      setStatus(existingItem.getStatusName());
    }
    super.applyOverlays(existingItem,instr);
  }

  @Override
  public void prepareCheckedDeletion() {
    setTransition(Transaction.DELETE);
    if (recordRetention.isDeleteProtectedByPatternMatch(this)) {
      handleDeleteProtection(DeletionConstraint.ITEM_PATTERN_MATCH);
    }
    if (isCirculating()) {
      handleDeleteProtection(DeletionConstraint.ITEM_STATUS);
    }
  }

  public boolean isCirculating() {
    return statusesIndicatingCirculatingItem.contains(getStatusName());
  }

 }
