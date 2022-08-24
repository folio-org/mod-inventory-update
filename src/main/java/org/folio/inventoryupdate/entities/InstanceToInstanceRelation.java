package org.folio.inventoryupdate.entities;
public abstract class InstanceToInstanceRelation extends InventoryRecord {
    public static final String PROVISIONAL_INSTANCE = "provisionalInstance";
    protected Instance provisionalInstance = null;
    protected InstanceRelationsClass instanceRelationClass = null;
    private String referencedInstanceHrid;

    public enum InstanceRelationsClass {
        TO_PARENT,
        TO_CHILD,
        TO_PRECEDING,
        TO_SUCCEEDING
    }

    public void setInstanceRelationsClass (InstanceRelationsClass typeOfRelation) {
        instanceRelationClass = typeOfRelation;
    }

    public boolean hasPreparedProvisionalInstance () {
        return provisionalInstance != null && !provisionalInstance.failed();
    }

    public Instance getProvisionalInstance () {
        return provisionalInstance;
    }

    public abstract boolean equals (Object o);

    public abstract int hashCode ();

    public void setReferencedInstanceHrid(String hrid) {
      this.referencedInstanceHrid = hrid;
    }

    public String getReferenceInstanceHrid () {
      return referencedInstanceHrid;
    }

}
