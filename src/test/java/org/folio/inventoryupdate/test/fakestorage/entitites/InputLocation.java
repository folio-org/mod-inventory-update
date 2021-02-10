package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;

public class InputLocation extends InventoryRecord {

    public static String ID = "id";
    public static String NAME = "name";
    public static String CODE = "code";
    public static String INSTITUTION_ID = "institutionId";

    public InputLocation () {
        super();
    }

    public InputLocation(JsonObject record) {
        super(record);
    }

    public InputLocation setId (String id) {
        recordJson.put(ID, id);
        return this;
    }

    public InputLocation setName(String name) {
        recordJson.put(NAME, name);
        return this;
    }

    public InputLocation setCode(String code) {
        recordJson.put(CODE, code);
        return this;
    }

    public InputLocation setInstitutionId(String institutionId) {
        recordJson.put(INSTITUTION_ID, institutionId);
        return this;
    }

}
