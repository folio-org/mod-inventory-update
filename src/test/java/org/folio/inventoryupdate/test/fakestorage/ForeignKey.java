package org.folio.inventoryupdate.test.fakestorage;

public class ForeignKey {

    private RecordStorage dependentStorage;
    private RecordStorage masterStorage;
    private String foreignKeyPropertyName;

    public ForeignKey (RecordStorage dependentStorage, String foreignKeyName, RecordStorage masterStorage) {
        this.dependentStorage = dependentStorage;
        this.masterStorage = masterStorage;
        foreignKeyPropertyName = foreignKeyName;
    }

    public RecordStorage getDependentStorage() {
        return dependentStorage;
    }

    public RecordStorage getMasterStorage() {
        return masterStorage;
    }

    public String getFkPropertyName () {
        return foreignKeyPropertyName;
    }
}
