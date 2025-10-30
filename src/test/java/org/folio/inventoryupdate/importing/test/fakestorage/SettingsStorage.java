package org.folio.inventoryupdate.importing.test.fakestorage;

import org.folio.inventoryupdate.importing.foliodata.SettingsClient;

public class SettingsStorage extends RecordStorage {
    public String getResultSetName() {
        return SettingsClient.RECORDS;
    }

    protected void declareUniqueProperties() {
        mandatoryProperties.add("id");
        mandatoryProperties.add("scope");
        mandatoryProperties.add("key");
        mandatoryProperties.add("value");
    }
}
