package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

import java.util.UUID;

import static org.folio.inventoryupdate.entities.InventoryRecordSet.HRID;

public abstract class InstanceToInstanceRelation extends InventoryRecord {
    public static final String PROVISIONAL_INSTANCE = "provisionalInstance";
    private boolean needsProvisionalInstance = false;
    protected Instance provisionalInstance = null;
    protected enum TypeOfRelation {
        TO_PARENT,
        TO_CHILD,
        TO_PRECEDING,
        TO_SUCCEEDING
    }

    public void requiresProvisionalInstanceToBeCreated(boolean yes) {
        needsProvisionalInstance = yes;
    }

    public boolean requiresProvisionalInstanceToBeCreated () {
        return needsProvisionalInstance;
    }

    public void setProvisionalInstance (Instance provisionalInstance) {
        this.provisionalInstance = provisionalInstance;
    }

    public boolean hasPreparedProvisionalInstance () {
        return provisionalInstance != null && !provisionalInstance.failed();
    }

    public Instance getProvisionalInstance () {
        return provisionalInstance;
    }

    /**
     * Create a temporary Instance to create a relationship to.
     * @param hrid human readable ID of the temporary Instance to create
     * @param provisionalInstanceJson other properties of the Instance to create
     * @return Instance POJO
     */
    //TODO: move to controller?
    protected static Instance prepareProvisionalInstance (String hrid, JsonObject provisionalInstanceJson) {
        JsonObject json = new JsonObject(provisionalInstanceJson.toString());
        if (! json.containsKey(HRID)) {
            json.put(HRID, hrid);
        }
        if (! json.containsKey(InstanceRelationsController.ID)) {
            json.put(InstanceRelationsController.ID, UUID.randomUUID().toString());
        }
        return new Instance(json);
    }

}
