package org.folio.inventoryupdate.entities;

public class PairedRecordSets {
  private InventoryRecordSet existingRecordSet;
  private InventoryRecordSet incomingRecordSet;

  public void setIncomingRecordSet (InventoryRecordSet incomingRecordSet) {
    this.incomingRecordSet = incomingRecordSet;
  }

  public void setExistingRecordSet (InventoryRecordSet existingRecordSet) {
    this.existingRecordSet = existingRecordSet;
  }

  public InventoryRecordSet getIncomingRecordSet () {
    return incomingRecordSet;
  }

  public InventoryRecordSet getExistingRecordSet () {
    return existingRecordSet;
  }

  public boolean hasIncomingRecordSet () {
    return incomingRecordSet != null;
  }

  public boolean hasExistingRecordSet () {
    return existingRecordSet != null;
  }
}
