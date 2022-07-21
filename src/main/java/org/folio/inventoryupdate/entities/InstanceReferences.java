package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class InstanceReferences {

  // JSON property keys
  public static final String INSTANCE_RELATIONS = "instanceRelations";
  public static final String EXISTING_PARENT_CHILD_RELATIONS = "existingParentChildRelations";
  public static final String PARENT_INSTANCES = "parentInstances";
  public static final String CHILD_INSTANCES = "childInstances";
  public static final String SUCCEEDING_TITLES = "succeedingTitles";
  public static final String PRECEDING_TITLES = "precedingTitles";
  public static final String EXISTING_PRECEDING_SUCCEEDING_TITLES = "existingPrecedingSucceedingTitles";
  JsonObject incomingInstanceRelations = null;
  List<InstanceReference> references = new ArrayList<>();
  JsonObject originJson;

  public InstanceReferences(JsonObject instanceRelations, JsonObject originJson) {
    if (instanceRelations != null) {
      incomingInstanceRelations = instanceRelations;
      registerIncomingInstanceReferences(instanceRelations, originJson);
    }
    this.originJson = originJson;
  }


  public void registerIncomingInstanceReferences (JsonObject instanceRelationsJson, JsonObject originJson) {
    if (instanceRelationsJson.containsKey(PARENT_INSTANCES)) {
      for (Object o : instanceRelationsJson.getJsonArray(PARENT_INSTANCES)) {
        references.add(new InstanceReference((JsonObject) o,
                InstanceToInstanceRelation.InstanceRelationsClass.TO_PARENT, originJson));
      }
    }
    if (instanceRelationsJson.containsKey(CHILD_INSTANCES)) {
      for (Object o : instanceRelationsJson.getJsonArray(CHILD_INSTANCES)) {
        references.add(new InstanceReference((JsonObject) o,
                InstanceToInstanceRelation.InstanceRelationsClass.TO_CHILD, originJson));
      }
    }
    if (instanceRelationsJson.containsKey(SUCCEEDING_TITLES)) {
      for (Object o : instanceRelationsJson.getJsonArray(SUCCEEDING_TITLES)) {
        references.add(new InstanceReference((JsonObject) o,
                InstanceToInstanceRelation.InstanceRelationsClass.TO_SUCCEEDING, originJson));
      }
    }
    if (instanceRelationsJson.containsKey(PRECEDING_TITLES)) {
      for (Object o : instanceRelationsJson.getJsonArray(PRECEDING_TITLES)) {
        references.add(new InstanceReference((JsonObject) o,
                InstanceToInstanceRelation.InstanceRelationsClass.TO_PRECEDING, originJson));
      }
    }
  }

}
