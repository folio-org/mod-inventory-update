package org.folio.inventorymatch;

import org.folio.okapi.common.OkapiClient;

import io.vertx.core.Future;

public class UpdateSharedInventoryByMatchkey extends UpdatePlan {

    public UpdateSharedInventoryByMatchkey(InventoryRecordSet incomingSet, InventoryRecordSet existingSet,
            OkapiClient okapiClient) {
        super(incomingSet, existingSet, okapiClient);
        // TODO Auto-generated constructor stub
    }

    @Override
    public Future<Void> planInventoryUpdates(OkapiClient client) {
        // TODO Auto-generated method stub
        return null;
    }
    
}