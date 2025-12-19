package org.folio.inventoryupdate.unittests.fakestorage.entities;

import org.folio.inventoryupdate.unittests.fakestorage.FolioApiRecord;

public class InputHoldingsRecord extends FolioApiRecord {

    public static final String HRID = "hrid";
    public static final String CALL_NUMBER = "callNumber";
    public static final String INSTANCE_ID = "instanceId";
    public static final String PERMANENT_LOCATION_ID = "permanentLocationId";

    public InputHoldingsRecord setInstanceId (String instanceId) {
        recordJson.put(INSTANCE_ID,instanceId);
        return this;
    }

    public InputHoldingsRecord setHrid (String hrid) {
        recordJson.put(HRID,hrid);
        return this;
    }

    public InputHoldingsRecord setPermanentLocationId (String permanentLocationId) {
        recordJson.put(PERMANENT_LOCATION_ID, permanentLocationId);
        return this;
    }

    public InputHoldingsRecord setCallNumber (String callNumber) {
        recordJson.put(CALL_NUMBER, callNumber);
        return this;
    }

    public InputHoldingsRecord setShelvingTitle (String shelvingTitle) {
        recordJson.put("shelvingTitle", shelvingTitle);
        return this;
    }

    public InputHoldingsRecord setAcquisitionFormat (String acquisitionFormat) {
        recordJson.put("acquisitionFormat", acquisitionFormat);
        return this;
    }

}
