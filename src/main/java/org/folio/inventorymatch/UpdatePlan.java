package org.folio.inventorymatch;

import java.util.List;

import org.folio.inventorymatch.InventoryRecordSet.HoldingsRecord;
import org.folio.inventorymatch.InventoryRecordSet.Item;
import org.folio.inventorymatch.InventoryRecordSet.Transition;

import io.vertx.core.json.JsonObject;

public abstract class UpdatePlan {

    protected InventoryRecordSet incomingSet;
    protected InventoryRecordSet existingSet;

    public UpdatePlan (JsonObject incomingInventoryRecordSet, JsonObject existingInventoryRecordSet) {
        incomingSet = new InventoryRecordSet(incomingInventoryRecordSet);
        if (existingInventoryRecordSet != null) {
            existingSet = new InventoryRecordSet(existingInventoryRecordSet);
        }
        planInventoryUpdates();
    }

    public abstract void planInventoryUpdates ();

    public InventoryRecordSet getIncomingRecordSet () {
        return incomingSet;
    }

    public InventoryRecordSet getExistingRecordSet () {
        return existingSet;
    }

    public List<Item> itemsToDelete () {
        return existingSet.getItemsByTransitionType(Transition.DELETING);
    }

    public List<HoldingsRecord> holdingsToDelete () {
        return existingSet.getHoldingsRecordsByTransitionType(Transition.DELETING);
    }


    public List<HoldingsRecord> holdingsToCreate () {
        return incomingSet.getHoldingsRecordsByTransitionType(Transition.CREATING);
    }

    public List<Item> itemsToCreate () {
        return incomingSet.getItemsByTransitionType(Transition.CREATING);
    }

    public List<HoldingsRecord> holdingsToUpdate () {
        return incomingSet.getHoldingsRecordsByTransitionType(Transition.UPDATING);
    }

    public List<Item> itemsToUpdate () {
        return incomingSet.getItemsByTransitionType(Transition.UPDATING);
    }

    public boolean isInstanceUpdating () {
        return incomingSet.getInstance().getTransition() == Transition.UPDATING;
    }

    public boolean isInstanceCreating () {
        return incomingSet.getInstance().getTransition() == Transition.CREATING;
    }

    public boolean isInstanceDeleting () {
        return incomingSet.getInstance().getTransition() == Transition.DELETING;
    }

}