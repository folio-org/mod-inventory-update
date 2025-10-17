package org.folio.inventoryupdate.importing.test.fakestorage;

import org.folio.inventoryupdate.importing.foliodata.ConfigurationsClient;

public class ConfigurationStorage extends RecordStorage {
    public String getResultSetName() {
        return ConfigurationsClient.RECORDS;
    }

    @Override
    protected void declareDependencies() {
        // Configurations have none in fake storage
    }

    @Override
    protected void declareMandatoryProperties() {}


}
