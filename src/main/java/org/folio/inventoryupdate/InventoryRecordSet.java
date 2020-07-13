package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.folio.inventoryupdate.entities.HoldingsRecord;
import org.folio.inventoryupdate.entities.Instance;
import org.folio.inventoryupdate.entities.InventoryRecord;
import org.folio.inventoryupdate.entities.Item;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class InventoryRecordSet {

    private JsonObject sourceJson = null;
    private Instance instance = null;
    private Map<String,HoldingsRecord> holdingsRecordsByHRID     = new HashMap<String,HoldingsRecord>();
    private Map<String,Item> itemsByHRID = new HashMap<String,Item>();
    private List<HoldingsRecord> allHoldingsRecords = new ArrayList<HoldingsRecord>();
    private List<Item> allItems = new ArrayList<Item>();
    @SuppressWarnings("unused")
    private final Logger logger = LoggerFactory.getLogger("inventory-update");

    public InventoryRecordSet (JsonObject inventoryRecordSet) {
        if (inventoryRecordSet != null) {
            sourceJson = new JsonObject(inventoryRecordSet.toString());
            JsonObject instanceJson = inventoryRecordSet.getJsonObject("instance");
            JsonArray holdings = inventoryRecordSet.getJsonArray("holdingsRecords");
            instance = new Instance(instanceJson);
            registerHoldingsRecordsAndItems (holdings);
        }
    }

    public void updateInstance (JsonObject updatedInstance) {
        instance.updateJson(updatedInstance);
    }

    private void registerHoldingsRecordsAndItems (JsonArray holdingsRecordsWithEmbeddedItems) {
        if (holdingsRecordsWithEmbeddedItems != null) {
            for (Object holdings : holdingsRecordsWithEmbeddedItems) {
                JsonObject holdingsRecordJson = (JsonObject) holdings;
                JsonArray items = new JsonArray();
                if (holdingsRecordJson.containsKey("items")) {
                    items = extractJsonArrayFromObject(holdingsRecordJson, "items");
                }
                HoldingsRecord holdingsRecord = new HoldingsRecord(holdingsRecordJson);
                for (Object object : items) {
                    JsonObject itemJson = (JsonObject) object;
                    Item item = new Item(itemJson);
                    String itemHrid = itemJson.getString("hrid");
                    if (itemHrid != null && !itemHrid.isEmpty()) {
                        itemsByHRID.put(itemHrid, item);
                    }
                    holdingsRecord.addItem(item);
                    allItems.add(item);
                }
                String holdingsRecordHrid = holdingsRecordJson.getString("hrid");
                if (holdingsRecordHrid != null && !holdingsRecordHrid.isEmpty()) {
                    holdingsRecordsByHRID.put(holdingsRecordHrid, holdingsRecord);
                }
                allHoldingsRecords.add(holdingsRecord);
                instance.addHoldingsRecord(holdingsRecord);
            }
        }
    }


    public JsonObject getSourceJson() {
        return sourceJson;
    }

    public JsonObject asJson() {
        JsonObject recordSetJson = new JsonObject();
        recordSetJson.put("instance", getInstance().asJson());
        JsonArray holdingsAndItemsArray = new JsonArray();
        for (HoldingsRecord holdingsRecord : getHoldingsRecords()) {
            JsonObject holdingsRecordJson = holdingsRecord.asJson();
            JsonArray itemsArray = new JsonArray();
            for (Item item : holdingsRecord.getItems()) {
                itemsArray.add(item.asJson());
            }
            holdingsRecordJson.put("items",itemsArray);
            holdingsAndItemsArray.add(holdingsRecordJson);
        }
        recordSetJson.put("holdingsRecords", holdingsAndItemsArray);
        return recordSetJson;
    }

    public String getInstanceHRID () {
        if (getInstance() == null) {
            return "no instance - no hrid";
        } else {
            return getInstance().getHRID();
        }
    }

    public String getInstanceUUID () {
        if (getInstance() == null) {
            return "no instance - no UUID";
        } else {
            return getInstance().UUID();
        }
    }

    public String getInstitutionIdFromArbitraryHoldingsRecord (Map<String,String> institutionsMap) {
        if (institutionsMap != null && !getHoldingsRecords().isEmpty()) {
            return getHoldingsRecords().get(0).getInstitutionId(institutionsMap);
        } else {
            return "";
        }
    }

    public HoldingsRecord getHoldingsRecordByHRID (String holdingsHrid) {
        return holdingsRecordsByHRID.get(holdingsHrid);
    }

    public Item getItemByHRID (String itemHrid) {
        return itemsByHRID.get(itemHrid);
    }

    public Instance getInstance () {
        return instance;
    }

    public Map<String, HoldingsRecord> getMapOfHoldingsRecordsByHRID () {
        return holdingsRecordsByHRID;
    }

    public Map<String, Item> getMapOfItemsByHRID() {
        return itemsByHRID;
    }

    public List<Item> getItemsByTransitionType (Transaction transition) {
        List<Item> records = new ArrayList<Item>();
        for (Item record : getItems()) {
            if (record.getTransaction() == transition) {
                records.add(record);
            }
        }
        return records;
    }

    public List<HoldingsRecord> getHoldingsRecordsByTransitionType (Transaction transition) {
        List<HoldingsRecord> records = new ArrayList<HoldingsRecord>();
        for (HoldingsRecord record : getHoldingsRecords()) {
            if (record.getTransaction() == transition) {
                records.add(record);
            }
        }
        return records;
    }


    public List<HoldingsRecord> getHoldingsRecords () {
        return allHoldingsRecords;
    }

    public List<Item> getItems () {
        return allItems;
    }

    public void skipHoldingsAndItems () {
        for (InventoryRecord record : getHoldingsRecords()) {
            record.skip();
        }
        for (InventoryRecord record : getItems()) {
            record.skip();
        }
    }

    /**
     * Creates a deep clone of a JSONArray from a JSONObject, removes the array from the source object and returns the clone
     * @param jsonObject Source object containing the array to extract
     * @param arrayName Property name of the array to extract
     * @return  The extracted JsonArray or an empty JsonArray if none found to extract.
     */
    private static JsonArray extractJsonArrayFromObject(JsonObject jsonObject, String arrayName)  {
        JsonArray array = new JsonArray();
        if (jsonObject.containsKey(arrayName)) {
            array = new JsonArray((jsonObject.getJsonArray(arrayName)).encode());
            jsonObject.remove(arrayName);
        }
        return array;
    }


}