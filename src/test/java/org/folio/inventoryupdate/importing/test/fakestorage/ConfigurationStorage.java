package org.folio.inventoryupdate.importing.test.fakestorage;

import org.folio.inventoryupdate.importing.foliodata.ConfigurationsClient;

public class ConfigurationStorage extends RecordStorage {
    public String getResultSetName() {
        return ConfigurationsClient.RECORDS;
    }

}
