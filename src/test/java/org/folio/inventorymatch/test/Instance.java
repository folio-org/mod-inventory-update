package org.folio.inventorymatch.test;

import io.vertx.core.json.JsonObject;

public class Instance {
  private String id;
  private String title;
  private String instanceTypeId;
  private String indexTitle;
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

  public Instance setIndexTitle (String matchKey) {
    this.indexTitle = matchKey;
    instanceJson.put("indexTitle", this.indexTitle);
    return this;
  }

  public String getIndexTitle() {
    return indexTitle;
  }

  public JsonObject getJson() {
    return instanceJson;
  }

  public boolean match(String query) {
    String trimmed = query.replace("(","").replace(")", "");
    String[] queryParts = trimmed.split("=");
    String key = queryParts[0];
    String value = queryParts[1].replace("\"", "");
    System.out.println("key: "+key);
    System.out.println("value: "+value);
    System.out.println("instance.getString(key): " + instanceJson.getString(key));
    return (instanceJson.getString(key) != null && instanceJson.getString(key).equals(value));
  }
}
