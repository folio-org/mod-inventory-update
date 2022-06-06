package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

import java.util.UUID;

import static org.folio.inventoryupdate.entities.InventoryRecordSet.HRID_IDENTIFIER_KEY;

public abstract class InstanceToInstanceRelation extends InventoryRecord {
    public static final String PROVISIONAL_INSTANCE = "provisionalInstance";
    private boolean needsProvisionalInstance = false;
    protected Instance provisionalInstance = null;
    protected InstanceRelationsClass instanceRelationClass = null;

    /**
     * Planning Create a temporary Instance to create a relationship to.
     * @param hrid human-readable ID of the temporary Instance to create
     * @param provisionalInstanceJson other properties of the Instance to create
     * @return Instance POJO
     */
    public static Instance prepareProvisionalInstance (String hrid, JsonObject provisionalInstanceJson) {
        JsonObject json = new JsonObject(provisionalInstanceJson.toString());
        if (! json.containsKey( HRID_IDENTIFIER_KEY )) {
            json.put( HRID_IDENTIFIER_KEY, hrid);
        }
        if (! json.containsKey("id")) {
            json.put("id", UUID.randomUUID().toString());
        }
        return new Instance(json);
    }

    public enum InstanceRelationsClass {
        TO_PARENT,
        TO_CHILD,
        TO_PRECEDING,
        TO_SUCCEEDING
    }

    public void setInstanceRelationsClass (InstanceRelationsClass typeOfRelation) {
        instanceRelationClass = typeOfRelation;
    }

    public void requiresProvisionalInstanceToBeCreated(boolean bool) {
        needsProvisionalInstance = bool;
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

    public abstract boolean equals (Object o);

    public abstract int hashCode ();
}
