package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

public class InstanceTitleSuccession extends InstanceToInstanceRelation {

    public static final String ID = "id";
    public static final String PRECEDING_INSTANCE_ID = "precedingInstanceId";
    public static final String SUCCEEDING_INSTANCE_ID = "succeedingInstanceId";
    private String instanceId;

    public static InstanceTitleSuccession makeInstanceTitleSuccessionFromJsonRecord(String instanceId, JsonObject precedingSucceedingJson) {
        InstanceTitleSuccession titleSuccession = new InstanceTitleSuccession();
        titleSuccession.jsonRecord = precedingSucceedingJson;
        titleSuccession.entityType = Entity.INSTANCE_TITLE_SUCCESSION;
        titleSuccession.instanceId = instanceId;
        titleSuccession.setInstanceRelationsClass(titleSuccession.isSucceedingTitle() ? InstanceRelationsClass.TO_SUCCEEDING : InstanceRelationsClass.TO_PRECEDING);
        return titleSuccession;
    }

    public static InstanceTitleSuccession makeInstanceTitleSuccession(String precedingInstanceId, String succeedingInstanceId) {
        InstanceTitleSuccession instanceTitleSuccession = new InstanceTitleSuccession();
        instanceTitleSuccession.jsonRecord = new JsonObject();
        instanceTitleSuccession.jsonRecord.put(SUCCEEDING_INSTANCE_ID, succeedingInstanceId);
        instanceTitleSuccession.jsonRecord.put(PRECEDING_INSTANCE_ID, precedingInstanceId);
        instanceTitleSuccession.entityType = Entity.INSTANCE_TITLE_SUCCESSION;
        instanceTitleSuccession.setInstanceRelationsClass(instanceTitleSuccession.isSucceedingTitle() ? InstanceRelationsClass.TO_SUCCEEDING : InstanceRelationsClass.TO_PRECEDING);
        return instanceTitleSuccession;
    }

    public String getSucceedingInstanceId () {
        return jsonRecord.getString(SUCCEEDING_INSTANCE_ID);
    }

    public String getPrecedingInstanceId() {
        return jsonRecord.getString(PRECEDING_INSTANCE_ID);
    }

    public boolean isSucceedingTitle () {
        return getPrecedingInstanceId().equals(instanceId);
    }

    public boolean isPrecedingTitle() {
        return getSucceedingInstanceId().equals(instanceId);
    }

    @Override
    public void skipDependants() {}

    @Override
    public boolean equals (Object o) {
        if (o instanceof InstanceTitleSuccession) {
            InstanceTitleSuccession other = (InstanceTitleSuccession) o;
            return (other.getSucceedingInstanceId().equals(this.getSucceedingInstanceId()) &&
                    other.getPrecedingInstanceId().equals(this.getPrecedingInstanceId()));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public String toString () {
        StringBuilder str = new StringBuilder();
        str.append("// Preceding: ").append(jsonRecord.getString(PRECEDING_INSTANCE_ID))
                .append(" Succeeding: ").append(jsonRecord.getString(SUCCEEDING_INSTANCE_ID));
        return str.toString();
    }

}
