package org.folio.inventoryupdate.entities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.referencemapping.AlternateFKValues;
import org.folio.inventoryupdate.referencemapping.ReferenceApi;
import org.folio.inventoryupdate.referencemapping.ForeignKey;

public class HoldingsRecord extends InventoryRecord {

  public static final String INSTANCE_ID = "instanceId";
  public static final String PERMANENT_LOCATION_ID = "permanentLocationId";
  List<Item> items = new ArrayList<>();

  public HoldingsRecord(JsonObject holdingsRecord) {
    this.jsonRecord = holdingsRecord;
    entityType = Entity.HOLDINGS_RECORD;
  }

  public HoldingsRecord(JsonObject holdingsRecord, JsonObject originJson) {
    this.jsonRecord = holdingsRecord;
    entityType = Entity.HOLDINGS_RECORD;
    this.originJson = originJson;
  }

  @Override
  public void setUUID(String uuid) {
    super.setUUID(uuid);
    setItemsHoldingsRecordId(uuid);
  }

  @Override
  public String generateUUID() {
    String uuid = super.generateUUID();
    setItemsHoldingsRecordId(uuid);
    return uuid;
  }

  public void setItemsHoldingsRecordId(String uuid) {
    for (Item item : items) {
      item.setHoldingsRecordId(uuid);
    }
  }

  public String getInstanceId() {
    return jsonRecord.getString(INSTANCE_ID);
  }

  public boolean hasInstanceId() {
    return (getInstanceId() != null && !getInstanceId().isEmpty());
  }

  public void setInstanceId(String uuid) {
    jsonRecord.put(INSTANCE_ID, uuid);
  }

  public void addItem(Item item) {
    items.add(item);
    if (hasUUID() && !item.hasHoldingsRecordId()) {
      item.setHoldingsRecordId(getUUID());
    }
  }

  public List<Item> getItems() {
    return items;
  }

  public String getPermanentLocationId() {
    return jsonRecord.getString(PERMANENT_LOCATION_ID);
  }

  public String getInstitutionId(Map<String, String> institutionsMap) {
    return institutionsMap.get(getPermanentLocationId());
  }

  public void skipDependants() {
    for (Item rec : items) {
      rec.skip();
    }
  }

  /**
   * Some GET properties from /holdings-storage/holdings cannot be PUT
   *
   * @param jsonRecord The record from Inventory storage
   */
  @Override
  public void removeGetPropertiesDisallowedInPut(JsonObject jsonRecord) {
    jsonRecord.remove("permanentLocation");
    jsonRecord.remove("illPolicy");
    jsonRecord.remove("holdingsItems");
    jsonRecord.remove("bareHoldingsItems");
    jsonRecord.remove("holdingsInstance");
  }

  @Override
  public void prepareCheckedDeletion() {
    setTransition(Transaction.DELETE);
    if (recordRetention.isDeleteProtectedByPatternMatch(this)) {
      handleDeleteProtection(DeletionConstraint.HOLDINGS_RECORD_PATTERN_MATCH);
    }
  }

  // Alternate foreign key resolution (supporting use of codes or names instead of UUIDs in input)
  private static final ForeignKey HOLDINGS_NOTE_TYPE = new ForeignKey("holdingsNoteTypeId", "notes", ReferenceApi.HOLDINGS_NOTE_TYPES);
  private static final ForeignKey HOLDINGS_SOURCE = new ForeignKey("sourceId", "", ReferenceApi.HOLDINGS_SOURCES);
  private static final ForeignKey HOLDINGS_TYPE = new ForeignKey("holdingsTypeId", "", ReferenceApi.HOLDINGS_TYPES);
  private static final ForeignKey CALL_NUMBER_TYPE = new ForeignKey("callNumberTypeId", "", ReferenceApi.CALL_NUMBER_TYPES);
  private static final ForeignKey ILL_POLICY = new ForeignKey("illPolicyId", "", ReferenceApi.ILL_POLICIES);
  private static final ForeignKey STATISTICAL_CODE = new ForeignKey("", "statisticalCodeIds", ReferenceApi.STATISTICAL_CODES);

  public List<AlternateFKValues> findAlternateFKValues() {
    List<AlternateFKValues> list = new ArrayList<>();
    // Find alternate identifies embedded in arrays of objects
    list.add(new AlternateFKValues(HOLDINGS_NOTE_TYPE.referencedApi(), getAltIdsFromArrayOfObjects(HOLDINGS_NOTE_TYPE.foreignKeyEmbeddedIn(), HOLDINGS_NOTE_TYPE.foreignKeyName())));
    // Find alternate identifiers in arrays of strings
    list.add(new AlternateFKValues(STATISTICAL_CODE.referencedApi(), getAltIdsFromArrayOfStrings(STATISTICAL_CODE.foreignKeyEmbeddedIn())));
    for (ForeignKey rd : Arrays.asList(CALL_NUMBER_TYPE, HOLDINGS_SOURCE, ILL_POLICY, HOLDINGS_TYPE)) {
      if (isNoUUID(asJson().getString(rd.foreignKeyName()))) {
        list.add(new AlternateFKValues(rd.referencedApi(), asJson().getString(rd.foreignKeyName())));
      }
    }
    return list;
  }

  @Override
  public List<ForeignKey> getForeignKeys() {
    return Arrays.asList(HOLDINGS_NOTE_TYPE, STATISTICAL_CODE, CALL_NUMBER_TYPE, HOLDINGS_SOURCE, ILL_POLICY, HOLDINGS_TYPE);
  }


}
