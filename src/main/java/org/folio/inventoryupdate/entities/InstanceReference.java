package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.ErrorReport;

import java.util.UUID;

import static org.folio.inventoryupdate.entities.InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID;
import static org.folio.inventoryupdate.entities.InstanceToInstanceRelation.PROVISIONAL_INSTANCE;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.HRID_IDENTIFIER_KEY;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.UUID_IDENTIFIER_KEY;

public class InstanceReference {

  public static final String INSTANCE_IDENTIFIER = "instanceIdentifier";
  public static final String ID = "id";
  public static final String INSTANCE_TYPE_ID = "instanceTypeId";
  public static final String TITLE = "title";
  public static final String SOURCE = "source";
  String fromInstanceId = null;
  String referencedInstanceId;
  JsonObject instanceReferenceJson;
  InstanceToInstanceRelation.InstanceRelationsClass typeOfRelation;
  JsonObject originJson;

  public InstanceReference (JsonObject referenceJson, InstanceToInstanceRelation.InstanceRelationsClass typeOfRelation, JsonObject originJson ) {
    instanceReferenceJson = referenceJson;
    this.typeOfRelation = typeOfRelation;
    this.originJson = originJson;
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
      return provisionalInstanceProperties.getString(TITLE) != null
              && provisionalInstanceProperties.getString(SOURCE) != null
              && provisionalInstanceProperties.getString(INSTANCE_TYPE_ID) != null;
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
      if ((relation.getHRID() != null) || (provisionalInstance != null && provisionalInstance.getHRID() != null)) {
        // Silently omit relation / provisional if no HRID provided, otherwise:
        relation.requiresProvisionalInstanceToBeCreated(true);
        if (!provisionalInstanceIsValid() && provisionalInstance!=null) {
          provisionalInstance.fail();
          provisionalInstance.logError(
                  "Provided data not sufficient for creating provisional Instance",
                  422,
                  ErrorReport.ErrorCategory.STORAGE,relation.originJson);
          relation.fail();
          relation.logError(
                  "Cannot create relation; Instance not found and miss data for provisional instance",
                  422,
                  ErrorReport.ErrorCategory.STORAGE, relation.originJson);
        }
        relation.setProvisionalInstance(provisionalInstance);
      }
    }
    return relation;
  }

}
