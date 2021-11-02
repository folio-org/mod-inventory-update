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
                shiftingMatchKeyQuery = new ShiftingMatchKeyQuery(
                        updatingRecordSet.getLocalIdentifier(),
                        updatingRecordSet.getLocalIdentifierTypeId(),
                        updatingRecordSet.getInstance().getMatchKey());
            }
            recordIdentifiers = RecordIdentifiers.identifiersWithLocalIdentifier( null, updatingRecordSet.getLocalIdentifierTypeId(), updatingRecordSet.getLocalIdentifier() );
        }
    }

    public Future<InventoryRecordSet> findPreviousMatchKeyByRecordIdentifier( OkapiClient okapiClient) {
        Promise<InventoryRecordSet> promise = Promise.promise();
        if (doCheck)
        {
            if ( canCheck )
            {
                InventoryStorage.lookupSingleInventoryRecordSet( okapiClient, shiftingMatchKeyQuery ).onComplete(
                        recordSet -> {
                            if (recordSet.succeeded()) {
                                JsonObject existingInventoryRecordSetJson = recordSet.result();
                                if (existingInventoryRecordSetJson != null) {
                                    secondaryExistingRecordSet = new InventoryRecordSet( existingInventoryRecordSetJson );
                                    UpdatePlanSharedInventory.removeIdentifierFromInstanceForInstitution(
                                            recordIdentifiers, secondaryExistingRecordSet.getInstance().asJson() );
                                    secondaryExistingRecordSet.getInstance().setTransition( InventoryRecord.Transaction.UPDATE );
                                    promise.complete(secondaryExistingRecordSet);
                                } else {
                                    promise.complete( null );
                                }
                            } else {
                                promise.fail( "Error looking up existing record set: " + recordSet.cause().getMessage() );
                            }
                        } );
            } else {
                if ( updatingRecordSet != null)
                {
                    logger.info(
                            "Incoming set does not provide all required record identifiers for looking up previous matches. " +
                                    "Provided processing info was: " +
                                    ( updatingRecordSet.getProcessingInfoAsJson() != null ?
                                            updatingRecordSet.getProcessingInfoAsJson().encode()
                                            : " None, no 'processing' info element provided " ) );
                }
                promise.complete( null );
            }
        } else {
            promise.complete(null);
        }
        return promise.future();
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
                    promise.fail( "Error updating Instance with a previous match key: " );
                }
            } );
        } else {
            promise.complete();
        }
        return promise.future();
    }
}
