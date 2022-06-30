package org.folio.inventoryupdate;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.InventoryRecord;
import org.folio.inventoryupdate.entities.InventoryRecordSet;
import org.folio.inventoryupdate.entities.RecordIdentifiers;
import org.folio.okapi.common.OkapiClient;

public class ShiftingMatchKeyManager
{
    InventoryQuery shiftingMatchKeyQuery;
    RecordIdentifiers recordIdentifiers;
    InventoryRecordSet updatingRecordSet;
    InventoryRecordSet secondaryExistingRecordSet;
    boolean doCheck;
    boolean canCheck;
    protected final Logger logger = LoggerFactory.getLogger("inventory-update");


    public ShiftingMatchKeyManager( InventoryRecordSet updatingRecordSet, InventoryRecordSet secondaryExistingRecordSet, boolean doCheck ) {
        this.doCheck = doCheck;
        if (doCheck) {
            this.secondaryExistingRecordSet = secondaryExistingRecordSet;
            this.updatingRecordSet = updatingRecordSet;
            if (updatingRecordSet.canLookForRecordsWithPreviousMatchKey()) {
                canCheck = true;
                shiftingMatchKeyQuery = new QueryShiftingMatchKey(
                        updatingRecordSet.getLocalIdentifier(),
                        updatingRecordSet.getLocalIdentifierTypeId(),
                        updatingRecordSet.getInstance().getMatchKey());
            }
            recordIdentifiers = RecordIdentifiers.identifiersWithLocalIdentifier( null, updatingRecordSet.getLocalIdentifierTypeId(), updatingRecordSet.getLocalIdentifier() );
        }
    }


    public Future<Void> handleUpdateOfInstanceWithPreviousMatchKeyIfAny (OkapiClient client) {
        Promise<Void> promise = Promise.promise();
        if (secondaryExistingRecordSet != null)
        {
            InventoryStorage.putInventoryRecord( client, secondaryExistingRecordSet.getInstance() ).onComplete( put -> {
                if ( put.succeeded() )
                {
                    promise.complete();
                } else {
                    promise.fail( put.cause().getMessage() );
                }
            } );
        } else {
            promise.complete();
        }
        return promise.future();
    }
}
