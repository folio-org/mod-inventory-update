package org.folio.inventoryupdate.entities;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.json.JsonObject;

public class Instance extends InventoryRecord {

    List<HoldingsRecord> holdingsRecords = new ArrayList<HoldingsRecord>();
    List<InstanceRelationship> parentRelationships = new ArrayList<InstanceRelationship>();
    List<InstanceRelationship> childRelationships = new ArrayList<InstanceRelationship>();

    public Instance (JsonObject instance) {
        jsonRecord = instance;
        type = Entity.INSTANCE;
    }

    public void replaceJson(JsonObject instance) {
        jsonRecord = instance;
        for (HoldingsRecord record : holdingsRecords) {
            record.setInstanceId(getUUID());
        }
    }

    @Override
    public void setUUID(String uuid) {
        super.setUUID(uuid);
        setHoldingsRecordsInstanceId(uuid);
    }

    @Override
    public String generateUUID () {
        String uuid = super.generateUUID();
        setHoldingsRecordsInstanceId(uuid);
        return uuid;
    }

    public void setHoldingsRecordsInstanceId (String uuid) {
        for (HoldingsRecord record : holdingsRecords) {
            record.setInstanceId(uuid);
        }
    }

    public void addHoldingsRecord(HoldingsRecord holdingsRecord) {
        if (hasUUID() && ! holdingsRecord.hasInstanceId()) {
            holdingsRecord.setInstanceId(getUUID());
        }
        holdingsRecords.add(holdingsRecord);
    }

    public List<HoldingsRecord> getHoldingsRecords() {
        return holdingsRecords;
    }

    public HoldingsRecord getHoldingsRecordByHRID (String hrid) {
        for (int i=0; i<holdingsRecords.size(); i++) {
            if (holdingsRecords.get(i).getHRID().equals(hrid)) {
                return holdingsRecords.get(i);
            }
        }
        return null;
    }
/*
    public void addInstanceRelationship(InstanceRelationship instanceRelationship) {
        if (instanceRelationship.getSubInstanceId().equals(getUUID())) {
            parentRelationships.add(instanceRelationship);
        } else if (instanceRelationship.getSuperInstanceId().equals(getUUID())) {
            childRelationships.add(instanceRelationship);
        }
    }
*/
    public List<InstanceRelationship> getSuperInstances () {
        return parentRelationships;
    }

    public List<InstanceRelationship> getSubInstances () {
        return childRelationships;
    }

    public void skipDependants () {
       for (HoldingsRecord rec : holdingsRecords) {
           rec.skip();
           rec.skipDependants();
       }
    }

}