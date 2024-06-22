package org.folio.inventoryupdate.test.fakestorage.entitites;

public class InputMaterialType extends InventoryRecord {
  public static String ID = "id";
  public static String NAME = "name";
  public InputMaterialType () {
    super();
  }

  public InputMaterialType setId (String id) {
    recordJson.put(ID, id);
    return this;
  }

  public InputMaterialType setName(String name) {
    recordJson.put(NAME, name);
    return this;
  }

}
