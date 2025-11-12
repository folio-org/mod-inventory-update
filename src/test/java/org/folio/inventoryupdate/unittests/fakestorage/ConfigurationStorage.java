package org.folio.inventoryupdate.unittests.fakestorage;

import org.folio.inventoryupdate.importing.foliodata.ConfigurationsClient;

public class ConfigurationStorage extends RecordStorage {
    public String getResultSetName() {
        return ConfigurationsClient.RECORDS;
    }

}
