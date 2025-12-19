package org.folio.inventoryupdate.unittests.fakestorage;

import io.vertx.core.json.JsonObject;


import java.util.UUID;

public abstract class FakeRecord {

  public static final String ID = "id";
  public static final String VERSION = "_version";
  protected JsonObject recordJson;


  public String getStringValue (String propertyName) {
    return recordJson.containsKey(propertyName) ? recordJson.getValue(propertyName).toString() : null;
  }

  public JsonObject getJson() {
    return recordJson;
  }

  public FakeRecord setId (String id) {
    recordJson.put(ID, id);
    return this;
  }

  public boolean hasId() {
    return recordJson.getString(ID) != null;
  }

  public void generateId () {
    recordJson.put(ID, UUID.randomUUID().toString());
  }

  public String getId () {
    return recordJson.getString(ID);
  }

  public void setFirstVersion () {
    recordJson.put(VERSION,1);
  }

  public Integer getVersion () {
    return recordJson.containsKey( VERSION ) ? recordJson.getInteger( VERSION ) : 0;
  }

  public void setVersion (Integer version) {
    recordJson.put( VERSION,  version);
  }

  public abstract boolean match(String query);
}
