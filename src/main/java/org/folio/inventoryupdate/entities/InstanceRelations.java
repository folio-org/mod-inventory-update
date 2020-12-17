package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class InstanceRelations extends JsonRepresentation {

    private static final String INSTANCE_RELATIONS = "instanceRelations";
    private static final String PARENT_INSTANCES = "parentInstances";
    private static final String CHILD_INSTANCES = "childInstances";
    private InventoryRecordSet inventoryRecordSet = null;
    private List<InstanceRelationship> parentRelations = new ArrayList<>();
    private List<InstanceRelationship> childRelations = new ArrayList<>();

    /*
    public InstanceRelations(InventoryRecordSet inventoryRecordSet) {
        if (inventoryRecordSet != null) {
            this.inventoryRecordSet = inventoryRecordSet;
            if (this.inventoryRecordSet.sourceJson != null) {
                if (this.inventoryRecordSet.sourceJson.containsKey(INSTANCE_RELATIONS)) {
                    registerInstanceRelationships(this.inventoryRecordSet.sourceJson.getJsonObject(INSTANCE_RELATIONS));
                }
            }
        }
    }

    private void registerInstanceRelationships (JsonObject instanceRelations) {
        if (instanceRelations != null) {
            if (instanceRelations.containsKey(PARENT_INSTANCES)) {
                for (Object o : instanceRelations.getJsonArray(PARENT_INSTANCES)) {
                    JsonObject parentRelation = (JsonObject) o;
                    InstanceRelationship relation = new InstanceRelationship(parentRelation);
                    parentRelations.add(relation);
                }
            }
            if (instanceRelations.containsKey(CHILD_INSTANCES)) {
                for (Object o : instanceRelations.getJsonArray(CHILD_INSTANCES)) {
                    JsonObject childRelation = (JsonObject) o;
                    InstanceRelationship relation = new InstanceRelationship(childRelation);
                    childRelations.add(relation);
                }
            }
        }
    }
*/
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
