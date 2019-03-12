package org.folio.inventorymatch.test.samples;

import io.vertx.core.json.JsonObject;

public class Instance {
  private String title;
  private String instanceTypeId;
  private JsonObject instanceJson = new JsonObject();

  public Instance () {
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

  public JsonObject getJson() {
    return instanceJson;
  }
}
