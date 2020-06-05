package org.folio.inventorymatch;

import java.util.List;

import org.folio.inventorymatch.InventoryRecordSet.HoldingsRecord;
import org.folio.inventorymatch.InventoryRecordSet.Instance;
import org.folio.inventorymatch.InventoryRecordSet.Item;
import org.folio.inventorymatch.InventoryRecordSet.Transition;
import io.vertx.core.json.JsonObject;

public class UpdatePlainInventoryByHRIDs extends UpdatePlan {

    public UpdatePlainInventoryByHRIDs (JsonObject incomingInventoryRecordSet, JsonObject existingInventoryRecordSet) {
        super(incomingInventoryRecordSet, existingInventoryRecordSet);
    }

    public void planInventoryUpdates () {
        Instance existingInstance = existingSet.getInstance();
        Instance incomingInstance = incomingSet.getInstance();
        if (existingInstance == null) {
            // Nothing to transfer, optimistically generate UUIDs
            String generatedInstanceId = incomingInstance.generateUUID();
            incomingInstance.setTransition(Transition.CREATING);
            List<HoldingsRecord> holdingsRecords = incomingInstance.getHoldingsRecords();
            for (int i=0; i< holdingsRecords.size(); i++) {
                HoldingsRecord holdingsRecord = holdingsRecords.get(i);
                String generatedHoldingsRecordId = holdingsRecord.generateUUID();
                holdingsRecord.setInstanceId(generatedInstanceId);
                holdingsRecord.setTransition(Transition.CREATING);
                List<Item> items = holdingsRecord.getItems();
                for (int j=0; j< items.size(); j++) {
                    Item item = items.get(j);
                    item.setHoldingsRecordId(generatedHoldingsRecordId);
                    item.generateUUID();
                    item.setTransition(Transition.CREATING);
                }
            }
        } else {
            incomingInstance.setTransition(Transition.UPDATING);
            // There is an existing instance with same HRID as the incoming instance
            List<HoldingsRecord> existingHoldingsRecords = existingInstance.getHoldingsRecords();
            for (int h=0; h<existingHoldingsRecords.size(); h++) {
                HoldingsRecord existingHoldingsRecord = existingHoldingsRecords.get(h);
                HoldingsRecord incomingHoldingsRecord = incomingSet.getHoldingsRecordByHRID(existingHoldingsRecord.getHRID());
                if (incomingHoldingsRecord == null) {
                    existingHoldingsRecord.setTransition(Transition.DELETING);
                    List<Item> existingItems = existingHoldingsRecord.getItems();
                    for (int i=0; i<existingItems.size(); i++) {
                        Item existingItem = existingItems.get(i);
                        Item incomingItem = incomingSet.getItemByHRID(existingItem.getHRID());
                        if (incomingItem == null) {
                            existingItem.setTransition(Transition.DELETING);
                        } else {
                            // TODO: item still exists but not on this holdings record HRID, where then?
                            incomingItem.setUUID(existingItem.getUUID());
                        }
                    }
                } else {
                    // There is an existing holdings record with the same HRID, on the same Instance
                    incomingHoldingsRecord.setUUID(existingHoldingsRecord.getUUID());
                    incomingHoldingsRecord.setInstanceId(existingInstance.getUUID());
                    incomingHoldingsRecord.setTransition(Transition.UPDATING);
                    List<Item> existingItems = existingHoldingsRecord.getItems();
                    for (int i=0; i<existingItems.size(); i++) {
                        Item existingItem = existingItems.get(i);
                        Item incomingItem = incomingHoldingsRecord.getItemByHRID(existingItem.getHRID());
                        if (incomingItem == null) {
                            existingItem.setTransition(Transition.DELETING);
                        } else {
                            // There is an existing item with the same HRID on the same holdings record
                            incomingItem.setUUID(existingItem.getUUID());
                            incomingItem.setHoldingsRecordId(incomingHoldingsRecord.getUUID());
                            incomingItem.setTransition(Transition.UPDATING);
                        }
                    }
                }
            }
            // Are there additional incoming holdings or items that we could not match by HRID in existing set:
            // Must have been new or moved here, will optimistically create
            List<HoldingsRecord> incomingHoldingsRecords = incomingInstance.getHoldingsRecords();
            for (int k=0; k< incomingHoldingsRecords.size(); k++) {
                HoldingsRecord incomingHoldingsRecord = incomingHoldingsRecords.get(k);
                if (incomingHoldingsRecord.stateUnknown()) {
                    incomingHoldingsRecord.setInstanceId(incomingInstance.getUUID());
                    if (! incomingHoldingsRecord.hasUUID()) {
                        incomingHoldingsRecord.generateUUID();
                        incomingHoldingsRecord.setTransition(Transition.CREATING);
                    }
                }
                List<Item> incomingItems = incomingHoldingsRecord.getItems();
                for (int l=0; l< incomingItems.size(); l++) {
                    Item incomingItem = incomingItems.get(l);
                    if (incomingItem.stateUnknown()) {
                        incomingItem.setHoldingsRecordId(incomingHoldingsRecord.getUUID());
                        if (! incomingItem.hasUUID()) {
                            incomingItem.generateUUID();
                            incomingItem.setTransition(Transition.CREATING);
                        }
                    }
                }
            }
        }
    }
}