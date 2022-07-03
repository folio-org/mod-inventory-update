package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

import static org.folio.inventoryupdate.entities.InstanceRelations.*;

public class InstanceReferences {

  JsonObject incomingInstanceRelations = null;
  List<InstanceReference> references = new ArrayList<>();

  public InstanceReferences(JsonObject instanceRelations) {
    if (instanceRelations != null) {
      incomingInstanceRelations = instanceRelations;
      registerIncomingInstanceReferences(instanceRelations);
    }
  }


  public void registerIncomingInstanceReferences (JsonObject instanceRelationsJson) {
    if (instanceRelationsJson.containsKey(PARENT_INSTANCES)) {
      for (Object o : instanceRelationsJson.getJsonArray(PARENT_INSTANCES)) {
        references.add(new InstanceReference((JsonObject) o,
                InstanceToInstanceRelation.InstanceRelationsClass.TO_PARENT));
      }
    }
    if (instanceRelationsJson.containsKey(CHILD_INSTANCES)) {
      for (Object o : instanceRelationsJson.getJsonArray(CHILD_INSTANCES)) {
        references.add(new InstanceReference((JsonObject) o,
                InstanceToInstanceRelation.InstanceRelationsClass.TO_CHILD));
      }
    }
    if (instanceRelationsJson.containsKey(SUCCEEDING_TITLES)) {
      for (Object o : instanceRelationsJson.getJsonArray(SUCCEEDING_TITLES)) {
        references.add(new InstanceReference((JsonObject) o,
                InstanceToInstanceRelation.InstanceRelationsClass.TO_SUCCEEDING));
      }
    }
    if (instanceRelationsJson.containsKey(PRECEDING_TITLES)) {
      for (Object o : instanceRelationsJson.getJsonArray(PRECEDING_TITLES)) {
        references.add(new InstanceReference((JsonObject) o,
                InstanceToInstanceRelation.InstanceRelationsClass.TO_PRECEDING));
      }
    }
  }

}
