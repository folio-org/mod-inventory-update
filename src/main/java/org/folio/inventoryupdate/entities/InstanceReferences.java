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

  public boolean hasRelationsArrays () {
    if (incomingInstanceRelations != null) {
      return (incomingInstanceRelations.containsKey(PARENT_INSTANCES) ||
              incomingInstanceRelations.containsKey(CHILD_INSTANCES) ||
              incomingInstanceRelations.containsKey(PRECEDING_TITLES) ||
              incomingInstanceRelations.containsKey(SUCCEEDING_TITLES));
    } else {
      return false;
    }
  }

  public List<InstanceReference> getReferencesToParentInstances () {
    List<InstanceReference> list = new ArrayList<>();
    if (incomingInstanceRelations.containsKey(PARENT_INSTANCES)) {
      for (InstanceReference reference : references) {
        if (reference.referencesParentInstance()) {
          list.add(reference);
        }
      }
      return list;
    } else {
      return null;
    }
  }

  public List<InstanceReference> getReferencesToChildInstances () {
    List<InstanceReference> list = new ArrayList<>();
    if (incomingInstanceRelations.containsKey(CHILD_INSTANCES)) {
      for (InstanceReference reference : references) {
        if (reference.referencesChildInstance()) {
          list.add(reference);
        }
      }
      return list;
    } else {
      return null;
    }
  }

  public List<InstanceReference> getReferencesToSucceedingTitles () {
    List<InstanceReference> list = new ArrayList<>();
    if (incomingInstanceRelations.containsKey(SUCCEEDING_TITLES)) {
      for (InstanceReference reference : references) {
        if (reference.referencesSucceedingInstance()) {
          list.add(reference);
        }
      }
      return list;
    } else {
      return null;
    }
  }

  public List<InstanceReference> getReferencesToPrecedingTitles () {
    List<InstanceReference> list = new ArrayList<>();
    if (incomingInstanceRelations.containsKey(PRECEDING_TITLES)) {
      for (InstanceReference reference : references) {
        if (reference.referencesPrecedingInstance()) {
          list.add(reference);
        }
      }
      return list;
    } else {
      return null;
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
