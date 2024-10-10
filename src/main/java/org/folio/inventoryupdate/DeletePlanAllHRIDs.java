package org.folio.inventoryupdate;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.folio.inventoryupdate.entities.HoldingsRecord;
import org.folio.inventoryupdate.entities.Instance;
import org.folio.inventoryupdate.entities.InventoryRecord;
import org.folio.inventoryupdate.entities.Item;
import org.folio.inventoryupdate.remotereferences.OrdersStorage;
import org.folio.inventoryupdate.instructions.ProcessingInstructionsDeletion;
import org.folio.okapi.common.OkapiClient;

import static org.folio.inventoryupdate.entities.InventoryRecord.Transaction.DELETE;

public class DeletePlanAllHRIDs extends DeletePlan {

  /**
   * Constructs deletion plane
   * @param existingInstanceQuery The query by which to find the instance to delete
   */
  private DeletePlanAllHRIDs (InventoryQuery existingInstanceQuery) {
    super(existingInstanceQuery);
  }

  public static DeletePlanAllHRIDs getDeletionPlan(InventoryQuery existingInstanceQuery) {
    return  new DeletePlanAllHRIDs(existingInstanceQuery);
  }

  public Future<Void> planInventoryDelete(OkapiClient okapiClient, ProcessingInstructionsDeletion deleteInstructions) {
    Promise<Void> promisedPlan = Promise.promise();
    lookupExistingRecordSet(okapiClient, instanceQuery).onComplete(lookup -> {
      if (lookup.succeeded()) {
        this.existingSet = lookup.result();
        if (foundExistingRecordSet()) {
          getExistingRecordSet().setDeleteInstructions(deleteInstructions);
          setDeleteConstraintIfReferencedByAcquisitions(okapiClient, getExistingInstance())
              .onComplete(result-> {
                planInventoryRecordsDeletes();
                promisedPlan.complete();
              });
        } else {
          promisedPlan.fail("Instance to delete not found");
        }
      } else {
        promisedPlan.fail(lookup.cause().getMessage());
      }
    });
    return promisedPlan.future();
  }

  public static Future<Void> setDeleteConstraintIfReferencedByAcquisitions(OkapiClient okapiClient, Instance existingInstance) {
    return OrdersStorage.lookupPurchaseOrderLinesByInstanceId(okapiClient, existingInstance.getUUID())
        .onComplete(poLinesLookup -> {
          if (!poLinesLookup.result().isEmpty()) {
            existingInstance.handleDeleteProtection(InventoryRecord.DeletionConstraint.PO_LINE_REFERENCE);
          }
        }).mapEmpty();
  }


  private void planInventoryRecordsDeletes () {
    getExistingInstance().setTransition(DELETE);
    for (HoldingsRecord holdings : getExistingInstance().getHoldingsRecords()) {
      holdings.prepareCheckedDeletion();
      for (InventoryRecord.DeletionConstraint holdingsConstraint : holdings.getDeleteConstraints()) {
        getExistingInstance().handleDeleteProtection(holdingsConstraint);
      }
      for (Item item : holdings.getItems()) {
        item.prepareCheckedDeletion();
        for (InventoryRecord.DeletionConstraint itemConstraint : item.getDeleteConstraints()) {
          holdings.handleDeleteProtection(itemConstraint);
          getExistingInstance().handleDeleteProtection(itemConstraint);
        }
      }
    }
    getExistingRecordSet().prepareInstanceRelationsForDeleteOrSkip();
  }

  @Override
  public Future<Void> doInventoryDelete(OkapiClient okapiClient) {
    Promise<Void> promise = Promise.promise();
    handleSingleSetDelete(okapiClient).onComplete(deletes -> {
      if (deletes.succeeded()) {
        promise.complete();
      } else {
        promise.fail(deletes.cause().getMessage());
      }
    });
    return promise.future();
  }

}
