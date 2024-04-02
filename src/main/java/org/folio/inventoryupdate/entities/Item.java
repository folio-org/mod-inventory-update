package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.ProcessingInstructionsUpsert;

public class Item extends InventoryRecord {

  public static final String HOLDINGS_RECORD_ID = "holdingsRecordId";

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
}
