package org.folio.inventoryupdate.entities;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;

public class InstanceTitleSuccession extends InstanceToInstanceRelation {

    public static final String ID = "id";
    public static final String PRECEDING_INSTANCE_ID = "precedingInstanceId";
    public static final String SUCCEEDING_INSTANCE_ID = "succeedingInstanceId";
    protected static final Logger logger = LoggerFactory.getLogger("inventory-update");

    public static InstanceTitleSuccession makeInstanceTitleSuccessionFromJsonRecord(String instanceId, JsonObject precedingSucceedingJson) {
        InstanceTitleSuccession titleSuccession = new InstanceTitleSuccession();
        titleSuccession.jsonRecord = precedingSucceedingJson;
        titleSuccession.entityType = Entity.INSTANCE_TITLE_SUCCESSION;
        titleSuccession.setInstanceRelationsClass(
                titleSuccession.jsonRecord.getString(PRECEDING_INSTANCE_ID).equals(instanceId) ?
                        InstanceRelationsClass.TO_SUCCEEDING : InstanceRelationsClass.TO_PRECEDING);
        return titleSuccession;
    }

    public static InstanceTitleSuccession makeInstanceTitleSuccession(String instanceId, String precedingInstanceId, String succeedingInstanceId) {
        InstanceTitleSuccession instanceTitleSuccession = new InstanceTitleSuccession();
        instanceTitleSuccession.jsonRecord = new JsonObject();
        instanceTitleSuccession.jsonRecord.put(SUCCEEDING_INSTANCE_ID, succeedingInstanceId);
        instanceTitleSuccession.jsonRecord.put(PRECEDING_INSTANCE_ID, precedingInstanceId);
        instanceTitleSuccession.entityType = Entity.INSTANCE_TITLE_SUCCESSION;
        instanceTitleSuccession.setInstanceRelationsClass( instanceId.equals(precedingInstanceId) ? InstanceRelationsClass.TO_SUCCEEDING : InstanceRelationsClass.TO_PRECEDING);
        return instanceTitleSuccession;
    }

    public static InstanceTitleSuccession makeRelationToSucceeding (String instanceId, String succeedingInstanceId) {
        return makeInstanceTitleSuccession(instanceId, instanceId, succeedingInstanceId);
    }

    public static InstanceTitleSuccession makeRelationToPreceding (String instanceId, String precedingInstanceId) {
        return makeInstanceTitleSuccession(instanceId, precedingInstanceId, instanceId);
    }

    public String getSucceedingInstanceId () {
        return jsonRecord.getString(SUCCEEDING_INSTANCE_ID);
    }

    public String getPrecedingInstanceId() {
        return jsonRecord.getString(PRECEDING_INSTANCE_ID);
    }

    public boolean isSucceedingTitle () {
        return instanceRelationClass.equals(InstanceRelationsClass.TO_SUCCEEDING);
    }

  @Override
  public void prepareCheckedDeletion() {
    throw new UnsupportedOperationException("Checked deletion not implemented for instance relationships");
  }

  @Override
    public void skipDependants() {
        // relations have no dependants
    }

    @Override
    public boolean equals (Object o) {
        if (o instanceof InstanceTitleSuccession other) {
            return other.toString().equals(this.toString());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public String toString () {
        String str = "// Preceding: " + jsonRecord.getString(PRECEDING_INSTANCE_ID) +
                " Succeeding: " + jsonRecord.getString(SUCCEEDING_INSTANCE_ID);
        return str;
    }

}
