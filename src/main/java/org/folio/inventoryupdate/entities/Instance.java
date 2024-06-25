package org.folio.inventoryupdate.entities;

import java.util.*;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.MatchKey;
import org.folio.inventoryupdate.referencemapping.AlternateFKValues;
import org.folio.inventoryupdate.referencemapping.ReferenceApi;
import org.folio.inventoryupdate.referencemapping.ForeignKey;


public class Instance extends InventoryRecord {

  public static final String MATCH_KEY = "matchKey";
  List<HoldingsRecord> holdingsRecords = new ArrayList<>();
  boolean holdingsIgnored = false;

  public Instance(JsonObject instance) {
    jsonRecord = instance;
    entityType = Entity.INSTANCE;
  }

  public Instance(JsonObject instance, JsonObject originJson) {
    jsonRecord = new JsonObject(instance.encode());
    this.originJson = originJson;
    entityType = Entity.INSTANCE;
  }

  public void replaceJson(JsonObject instance) {
    jsonRecord = instance;
    for (HoldingsRecord holdingsRecord : holdingsRecords) {
      holdingsRecord.setInstanceId(getUUID());
    }
  }

  @Override
  public void setUUID(String uuid) {
    super.setUUID(uuid);
    setHoldingsRecordsInstanceId(uuid);
  }


  @Override
  public void prepareCheckedDeletion() {
  }

  @Override
  public String generateUUID() {
    String uuid = super.generateUUID();
    setHoldingsRecordsInstanceId(uuid);
    return uuid;
  }

  public String getMatchKey() {
    if (jsonRecord.getString(MATCH_KEY) == null) {
      jsonRecord.put(MATCH_KEY, new MatchKey(jsonRecord).getKey());
    } else if (jsonRecord.getValue(MATCH_KEY) instanceof JsonObject) {
      // Received multipart match key object, translating it to match-key string
      jsonRecord.put(MATCH_KEY, new MatchKey(jsonRecord).getKey());
    }
    return jsonRecord.getString(MATCH_KEY);
  }

  public void ignoreHoldings(boolean ignore) {
    holdingsIgnored = ignore;
  }

  public boolean ignoreHoldings() {
    return holdingsIgnored;
  }

  public void setHoldingsRecordsInstanceId(String uuid) {
    for (HoldingsRecord holdingsRecord : holdingsRecords) {
      holdingsRecord.setInstanceId(uuid);
    }
  }

  public void addHoldingsRecord(HoldingsRecord holdingsRecord) {
    if (hasUUID() && !holdingsRecord.hasInstanceId()) {
      holdingsRecord.setInstanceId(getUUID());
    }
    holdingsRecords.add(holdingsRecord);
  }

  public List<HoldingsRecord> getHoldingsRecords() {
    return holdingsRecords;
  }

  public HoldingsRecord getHoldingsRecordByHRID(String hrid) {
    for (int i = 0; i < holdingsRecords.size(); i++) {
      if (holdingsRecords.get(i).getHRID().equals(hrid)) {
        return holdingsRecords.get(i);
      }
    }
    return null;
  }

  public void skipDependants() {
    for (HoldingsRecord rec : holdingsRecords) {
      rec.skip();
      rec.skipDependants();
    }
  }

  // Alternate foreign key resolution (supporting use of codes or names instead of UUIDs in input)
  private static final ForeignKey ALTERNATIVE_TITLE_TYPE = new ForeignKey("alternativeTitleTypeId", "alternativeTitles", ReferenceApi.ALTERNATIVE_TITLE_TYPES);
  private static final ForeignKey CLASSIFICATION_TYPE = new ForeignKey("classificationTypeId", "classifications", ReferenceApi.CLASSIFICATION_TYPES);
  private static final ForeignKey CONTRIBUTOR_NAME_TYPE = new ForeignKey("contributorNameTypeId", "contributors", ReferenceApi.CONTRIBUTOR_NAME_TYPES);
  private static final ForeignKey CONTRIBUTOR_TYPE = new ForeignKey("contributorTypeId", "contributors", ReferenceApi.CONTRIBUTOR_TYPES);
  private static final ForeignKey ELECTRONIC_ACCESS_RELATIONSHIP = new ForeignKey("relationshipId", "electronicAccess", ReferenceApi.ELECTRONIC_ACCESS_RELATIONSHIPS);
  private static final ForeignKey IDENTIFIER_TYPE = new ForeignKey("identifierTypeId", "identifiers", ReferenceApi.IDENTIFIER_TYPES);
  private static final ForeignKey INSTANCE_FORMAT = new ForeignKey("", "instanceFormatIds", ReferenceApi.INSTANCE_FORMATS);
  private static final ForeignKey INSTANCE_NOTE_TYPE = new ForeignKey("instanceNoteTypeId", "notes", ReferenceApi.INSTANCE_NOTE_TYPES);
  private static final ForeignKey INSTANCE_STATUS = new ForeignKey("statusId", "", ReferenceApi.INSTANCE_STATUSES);
  private static final ForeignKey INSTANCE_TYPE = new ForeignKey("instanceTypeId", "", ReferenceApi.INSTANCE_TYPES);
  private static final ForeignKey MODE_OF_ISSUANCE = new ForeignKey("modeOfIssuanceId", "", ReferenceApi.MODES_OF_ISSUANCE);
  private static final ForeignKey NATURE_OF_CONTENT_TERM = new ForeignKey("", "natureOfContentTermIds", ReferenceApi.NATURE_OF_CONTENT_TERMS);
  private static final ForeignKey STATISTICAL_CODE = new ForeignKey("", "statisticalCodeIds", ReferenceApi.STATISTICAL_CODES);

  public List<AlternateFKValues> getAlternateFKValues() {
    List<AlternateFKValues> list = new ArrayList<>();
    // Find alternate identifies embedded in arrays of objects
    for (ForeignKey rd : Arrays.asList(
        ALTERNATIVE_TITLE_TYPE, CLASSIFICATION_TYPE, CONTRIBUTOR_TYPE, CONTRIBUTOR_NAME_TYPE, ELECTRONIC_ACCESS_RELATIONSHIP,
        IDENTIFIER_TYPE, INSTANCE_NOTE_TYPE)) {
      list.add(new AlternateFKValues(rd.referencedApi(),
          getAltIdsFromArrayOfObjects(rd.foreignKeyEmbeddedIn(), rd.foreignKeyName())));
    }
    for (ForeignKey rd : Arrays.asList(INSTANCE_FORMAT, NATURE_OF_CONTENT_TERM, STATISTICAL_CODE)) {
      list.add(new AlternateFKValues(rd.referencedApi(), getAltIdsFromArrayOfStrings(rd.foreignKeyEmbeddedIn())));
    }
    for (ForeignKey rd : Arrays.asList(INSTANCE_STATUS, INSTANCE_TYPE, MODE_OF_ISSUANCE)) {
      if (isNoUUID(asJson().getString(rd.foreignKeyName()))) {
        list.add(new AlternateFKValues(rd.referencedApi(), asJson().getString(rd.foreignKeyName())));
      }
    }
    return list;
  }

  @Override
  public List<ForeignKey> getForeignKeys() {
    return Arrays.asList(ALTERNATIVE_TITLE_TYPE, CLASSIFICATION_TYPE, CONTRIBUTOR_TYPE, CONTRIBUTOR_NAME_TYPE, ELECTRONIC_ACCESS_RELATIONSHIP,
        IDENTIFIER_TYPE, INSTANCE_NOTE_TYPE, INSTANCE_FORMAT, NATURE_OF_CONTENT_TERM, STATISTICAL_CODE,
        INSTANCE_STATUS, INSTANCE_TYPE, MODE_OF_ISSUANCE);
  }

}
