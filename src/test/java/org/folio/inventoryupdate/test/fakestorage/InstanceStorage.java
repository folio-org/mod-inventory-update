package org.folio.inventoryupdate.test.fakestorage;

import org.folio.inventoryupdate.test.fakestorage.entitites.InputInstance;

public class InstanceStorage extends RecordStorage {

    public String getResultSetName() {
        return INSTANCES;
    }

    @Override
    protected void declareDependencies() {
      fakeStorage.instanceTypeStorage.acceptDependant(this, InputInstance.INSTANCE_TYPE_ID);
    }

    @Override
    protected void declareMandatoryProperties() {
        mandatoryProperties.add("source");
    }

    protected void declareUniqueProperties() {
      uniqueProperties.add("hrid");
    }

}
