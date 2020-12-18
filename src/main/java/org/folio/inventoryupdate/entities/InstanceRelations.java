package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class InstanceRelations extends JsonRepresentation {

    public static final String INSTANCE_RELATIONS = "instanceRelations";
    public static final String EXISTING_RELATIONS = "existingRelations";
    public static final String PARENT_INSTANCES = "parentInstances";
    public static final String CHILD_INSTANCES = "childInstances";
    public static final String INSTANCE_IDENTIFIER = "instanceIdentifier";

    private InventoryRecordSet inventoryRecordSet = null;
    private List<InstanceRelationship> parentRelations = new ArrayList<>();
    private List<InstanceRelationship> childRelations = new ArrayList<>();

    public InstanceRelations (String instanceId, JsonObject instanceRelations) {
        if (instanceRelations.containsKey(EXISTING_RELATIONS)) {
            JsonArray existingRelations = instanceRelations.getJsonArray(EXISTING_RELATIONS);
            for (Object o : existingRelations) {
                InstanceRelationship relationship = InstanceRelationship.makeRelationshipFromExisting(instanceId, (JsonObject) o);
                if (relationship.isRelationToChild()) {
                    childRelations.add(relationship);
                } else {
                    parentRelations.add(relationship);
                }
            }
        } else {
            if (instanceRelations.containsKey(PARENT_INSTANCES)) {
                for (Object o : instanceRelations.getJsonArray(PARENT_INSTANCES)) {
                    JsonObject relationshipJson = (JsonObject) o;
                    if (relationshipJson.containsKey(INSTANCE_IDENTIFIER) &&  relationshipJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey("hrid")) {
                        String hrid = relationshipJson.getJsonObject(INSTANCE_IDENTIFIER).getString("hrid");
                        InstanceRelationship relationship = InstanceRelationship.makeParentRelationshipWithHRID(
                                instanceId,
                                hrid,
                                relationshipJson.getString(InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID)
                        );
                        parentRelations.add(relationship);
                    }
                }
            }
            if (instanceRelations.containsKey(CHILD_INSTANCES)) {
                for (Object o : instanceRelations.getJsonArray(CHILD_INSTANCES)) {
                    JsonObject relationshipJson = (JsonObject) o;
                    if (relationshipJson.containsKey(INSTANCE_IDENTIFIER) &&  relationshipJson.getJsonObject(INSTANCE_IDENTIFIER).containsKey("hrid")) {
                        String hrid = relationshipJson.getJsonObject(INSTANCE_IDENTIFIER).getString("hrid");
                        InstanceRelationship relationship = InstanceRelationship.makeChildRelationshipWithHRID(
                                instanceId,
                                hrid,
                                relationshipJson.getString(InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID)
                        );
                        childRelations.add(relationship);
                    }
                }
            }
        }
    }

    @Override
    public JsonObject asJson() {
        return null;
    }

    @Override
    public boolean hasErrors() {
        return false;
    }

    @Override
    public JsonArray getErrors() {
        return null;
    }
}
