package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.instructions.ProcessingInstructionsUpsert;
import org.folio.inventoryupdate.referencemapping.AlternateFKValues;
import org.folio.inventoryupdate.referencemapping.ForeignKey;
import org.folio.inventoryupdate.referencemapping.ReferenceApi;

import java.util.ArrayList;
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


  private static final ForeignKey ITEM_NOTE_TYPE = new ForeignKey("itemNoteTypeId", "notes", ReferenceApi.ITEM_NOTE_TYPES);
  private static final ForeignKey ITEM_DAMAGED_STATUS = new ForeignKey("itemDamagedStatusId", "",ReferenceApi.ITEM_DAMAGED_STATUSES);
  private static final ForeignKey PERMANENT_LOAN_TYPE = new ForeignKey("permanentLoanTypeId", "", ReferenceApi.LOAN_TYPES);
  private static final ForeignKey TEMPORARY_LOAN_TYPE = new ForeignKey("temporaryLoanTypeId", "", ReferenceApi.LOAN_TYPES);
  private static final ForeignKey MATERIAL_TYPE = new ForeignKey("materialTypeId", "", ReferenceApi.MATERIAL_TYPES);
  private static final ForeignKey STATISTICAL_CODE = new ForeignKey("", "statisticalCodeIds", ReferenceApi.STATISTICAL_CODES);

  public List<AlternateFKValues> getAlternateFKValues() {
    List<AlternateFKValues> list = new ArrayList<>();
    // Find alternate identifies embedded in arrays of objects
    list.add(new AlternateFKValues(ITEM_NOTE_TYPE.referencedApi(), getAltIdsFromArrayOfObjects(ITEM_NOTE_TYPE.foreignKeyEmbeddedIn(), ITEM_NOTE_TYPE.foreignKeyName())));
    // Find alternate identifiers in arrays of strings
    list.add(new AlternateFKValues(STATISTICAL_CODE.referencedApi(), getAltIdsFromArrayOfStrings(STATISTICAL_CODE.foreignKeyEmbeddedIn())));
    // Find alternate identifiers in top level string properties
    for (ForeignKey fk : Arrays.asList(ITEM_DAMAGED_STATUS, MATERIAL_TYPE, PERMANENT_LOAN_TYPE, TEMPORARY_LOAN_TYPE)) {
      if (isNoUUID(asJson().getString(fk.foreignKeyName()))) {
        list.add(new AlternateFKValues(fk.referencedApi(), asJson().getString(fk.foreignKeyName())));
      }
    }
    return list;
  }

  @Override
  public List<ForeignKey> getForeignKeys() {
    return Arrays.asList(ITEM_NOTE_TYPE, ITEM_DAMAGED_STATUS, PERMANENT_LOAN_TYPE, TEMPORARY_LOAN_TYPE, MATERIAL_TYPE, STATISTICAL_CODE);
  }

}
