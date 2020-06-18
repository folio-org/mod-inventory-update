package org.folio.inventoryupdate.test;

import io.vertx.core.json.JsonObject;

public class Instance {
  private String id;
  private String title;
  private String hrid;
  private String instanceTypeId;
  private String matchKey;
  private JsonObject instanceJson = new JsonObject();

  public Instance () {
  }

  public Instance (JsonObject instanceJson) {
    this.instanceJson = instanceJson;
  }

  public Instance setId (String id) {
    this.id = id;
    instanceJson.put("id", id);
    return this;
  }

  public String getId () {
    return id;
  }

  public Instance setTitle (String title) {
    this.title = title;
    instanceJson.put("title", title);
    return this;
  }

  public String getTitle () {
    return title;
  }

  public Instance setInstanceTypeId (String instanceTypeId) {
    this.instanceTypeId = instanceTypeId;
    instanceJson.put("instanceTypeId", instanceTypeId);
    return this;
  }

  public String getInstanceTypeId() {
    return instanceTypeId;
  }

  public Instance setMatchKey (String matchKey) {
    this.matchKey = matchKey;
    instanceJson.put("matchKey", this.matchKey);
    System.out.println(instanceJson.encodePrettily());
    return this;
  }

  public String getMatchKey() {
    return matchKey;
  }

  public Instance setHrid (String hrid) {
    this.hrid = hrid;
    instanceJson.put("hrid", hrid);
    return this;
  }

  public String getHrid () {
    return hrid;
  }

  public JsonObject getJson() {
    return instanceJson;
  }

  public boolean match(String query) {
    String trimmed = query.replace("(","").replace(")", "");
    String[] queryParts = trimmed.split("==");
    System.out.println("query: " +query);
    System.out.println("queryParts[0]: " + queryParts[0]);
    String key = queryParts[0];
    String value = queryParts.length > 1 ?  queryParts[1].replace("\"", "") : "";
    System.out.println("key: "+key);
    System.out.println("value: "+value);
    System.out.println("instance.getString(key): " + instanceJson.getString(key));
    return (instanceJson.getString(key) != null && instanceJson.getString(key).equals(value));
  }
}
