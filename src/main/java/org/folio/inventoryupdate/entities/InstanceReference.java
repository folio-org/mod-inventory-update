package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.QueryByUUID;

import java.util.UUID;

import static org.folio.inventoryupdate.entities.InstanceRelations.INSTANCE_IDENTIFIER;
import static org.folio.inventoryupdate.entities.InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID;
import static org.folio.inventoryupdate.entities.InstanceToInstanceRelation.PROVISIONAL_INSTANCE;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.HRID_IDENTIFIER_KEY;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.UUID_IDENTIFIER_KEY;

public class InstanceReference {

  String fromInstanceId = null;
  String referencedInstanceId;
  JsonObject instanceReferenceJson;
  InstanceToInstanceRelation.InstanceRelationsClass typeOfRelation;

  public InstanceReference (JsonObject referenceJson, InstanceToInstanceRelation.InstanceRelationsClass typeOfRelation ) {
    instanceReferenceJson = referenceJson;
    this.typeOfRelation = typeOfRelation;
  }

  public boolean hasReferenceHrid () {
    return instanceReferenceJson.containsKey(INSTANCE_IDENTIFIER)
            && instanceReferenceJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey(HRID_IDENTIFIER_KEY);
  }

  public boolean hasReferenceUuid () {
    return (instanceReferenceJson.containsKey(INSTANCE_IDENTIFIER)
            && instanceReferenceJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey(UUID_IDENTIFIER_KEY));
  }

  public String getReferenceHrid () {
    return instanceReferenceJson.getJsonObject(INSTANCE_IDENTIFIER).getString(HRID_IDENTIFIER_KEY);
  }

  public String getReferenceUuid ()  {
    return instanceReferenceJson.getJsonObject(INSTANCE_IDENTIFIER).getString(UUID_IDENTIFIER_KEY);
  }

  public void setFromInstanceId(String uuid) {
    this.fromInstanceId = uuid;
  }

  private String getRelationshipTypeId () {
    return instanceReferenceJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID);
  }

  protected boolean referencesParentInstance () {
    return (typeOfRelation == InstanceToInstanceRelation.InstanceRelationsClass.TO_PARENT);
  }

  protected boolean referencesChildInstance () {
    return (typeOfRelation == InstanceToInstanceRelation.InstanceRelationsClass.TO_CHILD);
  }

  protected boolean referencesSucceedingInstance () {
    return (typeOfRelation == InstanceToInstanceRelation.InstanceRelationsClass.TO_SUCCEEDING);
  }

  protected boolean referencesPrecedingInstance () {
    return (typeOfRelation == InstanceToInstanceRelation.InstanceRelationsClass.TO_PRECEDING);
  }
  public void setReferencedInstanceId(String uuid) {
    this.referencedInstanceId = uuid;
  }

  private boolean hasProvisionalInstance () {
    return instanceReferenceJson.containsKey(PROVISIONAL_INSTANCE);
  }

  private boolean provisionalInstanceIsValid() {
    return hasProvisionalInstance()
            && validateProvisionalInstanceProperties(
                    instanceReferenceJson.getJsonObject(PROVISIONAL_INSTANCE));
  }

  private JsonObject getProvisionalInstanceJson () {
    return instanceReferenceJson.containsKey(PROVISIONAL_INSTANCE) ?
            instanceReferenceJson.getJsonObject(PROVISIONAL_INSTANCE) :
            new JsonObject();
  }

  private static boolean validateProvisionalInstanceProperties (JsonObject provisionalInstanceProperties) {
    if (provisionalInstanceProperties == null) {
      return false;
    } else {
      if (provisionalInstanceProperties.getString( InstanceRelations.TITLE) != null
              && provisionalInstanceProperties.getString( InstanceRelations.SOURCE) != null
              && provisionalInstanceProperties.getString( InstanceRelations.INSTANCE_TYPE_ID) != null) {
        return true;
      } else {
        return false;
      }
    }
  }

  private Instance getProvisionalInstance () {
    JsonObject json = new JsonObject(getProvisionalInstanceJson().toString());
    if (! json.containsKey( HRID_IDENTIFIER_KEY ) && hasReferenceHrid()) {
      json.put( HRID_IDENTIFIER_KEY, getReferenceHrid());
    }
    if (! json.containsKey("id")) {
      json.put("id", UUID.randomUUID().toString());
    }
    return new Instance(json);

  }

  public InstanceToInstanceRelation getInstanceToInstanceRelation () {
    InstanceToInstanceRelation relation = null;
    String toInstanceId;
    Instance provisionalInstance = null;

    if (referencedInstanceId != null) { // Found existing Instance for HRID to link to
      toInstanceId = referencedInstanceId;
    } else { // Create provisional Instance to link to
      provisionalInstance = getProvisionalInstance();
      toInstanceId = provisionalInstance.getUUID();
    }
    if (typeOfRelation == InstanceToInstanceRelation.InstanceRelationsClass.TO_PARENT) {
      relation = InstanceRelationship.makeParentRelationship(fromInstanceId, toInstanceId,
              getRelationshipTypeId());
    } else if (typeOfRelation == InstanceToInstanceRelation.InstanceRelationsClass.TO_CHILD) {
      relation = InstanceRelationship.makeChildRelationship(fromInstanceId, toInstanceId,
              getRelationshipTypeId());
    } else if (typeOfRelation == InstanceToInstanceRelation.InstanceRelationsClass.TO_SUCCEEDING) {
      relation = InstanceTitleSuccession.makeRelationToSucceeding(fromInstanceId, toInstanceId);
    } else if (typeOfRelation == InstanceToInstanceRelation.InstanceRelationsClass.TO_PRECEDING) {
      relation = InstanceTitleSuccession.makeRelationToPreceding(fromInstanceId, toInstanceId);
    }
    if (referencedInstanceId == null && relation != null) {
      relation.requiresProvisionalInstanceToBeCreated(true);
      if (!provisionalInstanceIsValid()) {
        provisionalInstance.fail();
        relation.fail();
      }
      relation.setProvisionalInstance(provisionalInstance);
    }
    return relation;
  }

}
