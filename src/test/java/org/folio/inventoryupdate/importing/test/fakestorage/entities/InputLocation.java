package org.folio.inventoryupdate.importing.test.fakestorage.entities;

import org.folio.inventoryupdate.importing.test.fakestorage.FolioApiRecord;

public class InputLocation extends FolioApiRecord {

    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String INSTITUTION_ID = "institutionId";

    public InputLocation () {
        super();
    }

    @Override
    public InputLocation setId (String id) {
        recordJson.put(ID, id);
        return this;
    }

    public InputLocation setName(String name) {
        recordJson.put(NAME, name);
        return this;
    }

    public InputLocation setInstitutionId(String institutionId) {
        recordJson.put(INSTITUTION_ID, institutionId);
        return this;
    }

}
