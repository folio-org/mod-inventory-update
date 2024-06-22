package org.folio.inventoryupdate.test.fakestorage.entitites;

import org.folio.inventoryupdate.referencemapping.ForeignKey;

import java.util.List;

public class InputInstanceType extends InventoryRecord {
  public static String ID = "id";
  public static String NAME = "name";
  public static String CODE = "code";
  public InputInstanceType () {
    super();
  }

  public InputInstanceType setId (String id) {
    recordJson.put(ID, id);
    return this;
  }

  public InputInstanceType setCode(String code) {
    recordJson.put(CODE, code);
    return this;
  }

  public InputInstanceType setName(String name) {
    recordJson.put(NAME, name);
    return this;
  }

}
