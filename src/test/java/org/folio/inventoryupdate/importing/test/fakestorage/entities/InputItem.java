package org.folio.inventoryupdate.importing.test.fakestorage.entities;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.importing.test.fakestorage.FolioApiRecord;

public class InputItem extends FolioApiRecord {
  public static final String HRID = "hrid";
  public static final String BARCODE = "barcode";
  public static final String HOLDINGS_RECORD_ID = "holdingsRecordId";

  public InputItem setHrid (String hrid) {
    recordJson.put(HRID,hrid);
    return this;
  }
  public InputItem setBarcode (String barcode) {
    recordJson.put(BARCODE, barcode);
    return this;
  }

  public InputItem setHoldingsRecordId (String holdingsRecordId) {
    recordJson.put(HOLDINGS_RECORD_ID, holdingsRecordId);
    return this;
  }

  public InputItem setStatus (String statusName) {
    JsonObject status = new JsonObject();
    status.put("name", statusName);
    recordJson.put("status", status);
    return this;
  }

  public InputItem setMaterialTypeId (String materialTypeId) {
    recordJson.put("materialTypeId", materialTypeId);
    return this;
  }

  public InputItem setYearCaption (String yearCaption) {
    JsonArray array = new JsonArray();
    array.add(yearCaption);
    recordJson.put("yearCaption", array);
    return this;
  }

}
