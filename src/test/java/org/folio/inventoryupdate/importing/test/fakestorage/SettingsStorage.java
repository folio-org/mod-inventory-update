package org.folio.inventoryupdate.importing.test.fakestorage;

import org.folio.inventoryupdate.importing.foliodata.SettingsClient;

public class SettingsStorage extends RecordStorage {
    public String getResultSetName() {
        return SettingsClient.RECORDS;
    }

    @Override
    protected void declareDependencies() {
        // Settings have none in fake storage
    }

    @Override
    protected void declareMandatoryProperties() {}

    protected void declareUniqueProperties() {
        mandatoryProperties.add("id");
        mandatoryProperties.add("scope");
        mandatoryProperties.add("key");
        mandatoryProperties.add("value");
    }


}
