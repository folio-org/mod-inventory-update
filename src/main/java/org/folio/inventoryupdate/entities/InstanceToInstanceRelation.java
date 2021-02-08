package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

import java.util.UUID;

import static org.folio.inventoryupdate.entities.InventoryRecordSet.HRID;

public abstract class InstanceToInstanceRelation extends InventoryRecord {
    public static final String PROVISIONAL_INSTANCE = "provisionalInstance";
    private boolean needsProvisionalInstance = false;
    protected Instance provisionalInstance = null;
    protected InstanceRelationsClass instanceRelationClass = null;
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

}