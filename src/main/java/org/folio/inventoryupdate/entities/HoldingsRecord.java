package org.folio.inventoryupdate.entities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.vertx.core.json.JsonObject;

public class HoldingsRecord extends InventoryRecord {
    List<Item> items = new ArrayList<Item>();

    public HoldingsRecord(JsonObject holdingsRecord) {
        this.jsonRecord = holdingsRecord;
        type = Entity.HOLDINGS_RECORD;
    }

    @Override
    public void setUUID (String uuid) {
        super.setUUID(uuid);
        setItemsHoldingsRecordId(uuid);
    }

    @Override
    public String generateUUID () {
        String uuid = super.generateUUID();
        setItemsHoldingsRecordId(uuid);
        return uuid;
    }

    public void setItemsHoldingsRecordId (String uuid) {
        for (Item record : items) {
            record.setHoldingsRecordId(uuid);
        }
    }

    public String getInstanceId () {
        return jsonRecord.getString("instanceId");
    }

    public boolean hasInstanceId () {
        return (getInstanceId() != null && !getInstanceId().isEmpty());
    }

    public void setInstanceId (String uuid) {
        jsonRecord.put("instanceId", uuid);
    }

    public void addItem(Item item) {
        items.add(item);
        if (hasUUID() && ! item.hasHoldingsRecordId()) {
            item.setHoldingsRecordId(getUUID());
        }
    }

    public List<Item> getItems() {
        return items;
    }

    public Item getItemByHRID (String hrid) {
        for (int i=0; i<items.size(); i++) {
            if (items.get(i).getHRID().equals(hrid)) {
                return items.get(i);
            }
        }
        return null;
    }

    public String getPermanentLocationId () {
        return jsonRecord.getString("permanentLocationId");
    }

    public String getInstitutionId (Map<String,String> institutionsMap) {
        return institutionsMap.get(getPermanentLocationId());
    }

    public void skipDependants () {
        for (Item rec : items) {
            rec.skip();
        }
    }

}