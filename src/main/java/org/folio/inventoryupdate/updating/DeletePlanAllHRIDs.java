package org.folio.inventoryupdate.updating;

import io.vertx.core.Future;
import io.vertx.core.VertxException;
import org.folio.inventoryupdate.updating.entities.HoldingsRecord;
import org.folio.inventoryupdate.updating.entities.Instance;
import org.folio.inventoryupdate.updating.entities.InventoryRecord;
import org.folio.inventoryupdate.updating.entities.Item;
import org.folio.inventoryupdate.updating.foreignconstraints.OrdersStorage;
import org.folio.inventoryupdate.updating.instructions.ProcessingInstructionsDeletion;
import org.folio.okapi.common.OkapiClient;

import static org.folio.inventoryupdate.updating.entities.InventoryRecord.Transaction.DELETE;

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
    return lookupExistingRecordSet(okapiClient, instanceQuery)
        .compose(lookup -> {
          this.existingSet = lookup;
          if (!foundExistingRecordSet()) {
            throw VertxException.noStackTrace("Instance to delete not found");
          }
          getExistingRecordSet().setDeleteInstructions(deleteInstructions);
          return setDeleteConstraintIfReferencedByAcquisitions(okapiClient, getExistingInstance());
        })
        .onComplete(x -> planInventoryRecordsDeletes());
  }

  public static Future<Void> setDeleteConstraintIfReferencedByAcquisitions(OkapiClient okapiClient, Instance existingInstance) {
    return OrdersStorage.lookupPurchaseOrderLines(okapiClient, existingInstance.getUUID())
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
    return handleSingleSetDelete(okapiClient);
  }

}
