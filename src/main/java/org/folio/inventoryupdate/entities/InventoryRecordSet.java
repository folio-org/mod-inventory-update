package org.folio.inventoryupdate.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import org.folio.inventoryupdate.entities.HoldingsRecord;
import org.folio.inventoryupdate.entities.Instance;
import org.folio.inventoryupdate.entities.InventoryRecord;
import org.folio.inventoryupdate.entities.Item;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class InventoryRecordSet extends JsonRepresentation {

    private Instance anInstance = null;
    private Map<String,HoldingsRecord> holdingsRecordsByHRID = new HashMap<String,HoldingsRecord>();
    private Map<String,Item> itemsByHRID = new HashMap<String,Item>();
    private List<HoldingsRecord> allHoldingsRecords = new ArrayList<HoldingsRecord>();
    private List<Item> allItems = new ArrayList<Item>();
    private InstanceRelations instanceRelations;

    public static final String INSTANCE = "instance";
    public static final String HOLDINGS_RECORDS = "holdingsRecords";
    public static final String ITEMS = "items";
    public static final String HRID = "hrid";
    @SuppressWarnings("unused")
    private final Logger logger = LoggerFactory.getLogger("inventory-update");

    public InventoryRecordSet (JsonObject inventoryRecordSet) {
        if (inventoryRecordSet != null) {
            sourceJson = new JsonObject(inventoryRecordSet.toString());
            JsonObject instanceJson = inventoryRecordSet.getJsonObject(INSTANCE);
            JsonArray holdings = inventoryRecordSet.getJsonArray(HOLDINGS_RECORDS);
            anInstance = new Instance(instanceJson);
            registerHoldingsRecordsAndItems (holdings);
            JsonObject instanceRelationsJson = inventoryRecordSet.getJsonObject(InstanceRelations.INSTANCE_RELATIONS);
            this.instanceRelations = new InstanceRelations(anInstance.getUUID(), instanceRelationsJson);
        }
    }

    public static boolean isValidInventoryRecordSet(JsonObject inventoryRecordSet) {
        if (inventoryRecordSet == null) return false;
        if (!inventoryRecordSet.containsKey(INSTANCE)) return false;
        return true;
    }

    public void modifyInstance (JsonObject updatedInstance) {
        anInstance.replaceJson(updatedInstance);
    }

    /**
     * Populate structures `holdingsRecordsByHRID`, `allHoldingsRecords`, `itemsByHRID`, `allItems`,
     * add Item entities to their HoldingsRecord entity, add HoldingsRecord entities to the Instance entity.
     *
     * @param holdingsRecordsWithEmbeddedItems JSON array of holdings records, each record with embedded items.
     */
    private void registerHoldingsRecordsAndItems (JsonArray holdingsRecordsWithEmbeddedItems) {
        if (holdingsRecordsWithEmbeddedItems != null) {
            for (Object holdings : holdingsRecordsWithEmbeddedItems) {
                JsonObject holdingsRecordJson = (JsonObject) holdings;
                JsonArray items = new JsonArray();
                if (holdingsRecordJson.containsKey(ITEMS)) {
                    items = extractJsonArrayFromObject(holdingsRecordJson, ITEMS);
                }
                HoldingsRecord holdingsRecord = new HoldingsRecord(holdingsRecordJson);
                for (Object object : items) {
                    JsonObject itemJson = (JsonObject) object;
                    Item item = new Item(itemJson);
                    String itemHrid = itemJson.getString(HRID);
                    if (itemHrid != null && !itemHrid.isEmpty()) {
                        itemsByHRID.put(itemHrid, item);
                    }
                    holdingsRecord.addItem(item);
                    allItems.add(item);
                }
                String holdingsRecordHrid = holdingsRecordJson.getString(HRID);
                if (holdingsRecordHrid != null && !holdingsRecordHrid.isEmpty()) {
                    holdingsRecordsByHRID.put(holdingsRecordHrid, holdingsRecord);
                }
                allHoldingsRecords.add(holdingsRecord);
                anInstance.addHoldingsRecord(holdingsRecord);
            }
        }
    }

    @Override
    public JsonObject asJson() {
        JsonObject recordSetJson = new JsonObject();
        recordSetJson.put(INSTANCE, getInstance().asJson());
        JsonArray holdingsAndItemsArray = new JsonArray();
        for (HoldingsRecord holdingsRecord : getHoldingsRecords()) {
            JsonObject holdingsRecordJson = new JsonObject(holdingsRecord.asJsonString());
            JsonArray itemsArray = new JsonArray();
            for (Item item : holdingsRecord.getItems()) {
                itemsArray.add(item.asJson());
            }
            holdingsRecordJson.put(ITEMS,itemsArray);
            holdingsAndItemsArray.add(holdingsRecordJson);
        }
        recordSetJson.put(HOLDINGS_RECORDS, holdingsAndItemsArray);
        // GBV-106 also put instance relationships
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
            return getInstance().getUUID();
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
        return anInstance;
    }

    public Map<String, HoldingsRecord> getMapOfHoldingsRecordsByHRID () {
        return holdingsRecordsByHRID;
    }

    public Map<String, Item> getMapOfItemsByHRID() {
        return itemsByHRID;
    }

    public List<Item> getItemsByTransactionType (Transaction transition) {
        List<Item> records = new ArrayList<Item>();
        for (Item record : getItems()) {
            if (record.getTransaction() == transition && ! record.skipped()) {
                records.add(record);
            }
        }
        return records;
    }

    // GBV-106?: get parents, get children, get by type, get succeeding, get preceding

    public List<HoldingsRecord> getHoldingsRecordsByTransactionType (Transaction transition) {
        List<HoldingsRecord> records = new ArrayList<HoldingsRecord>();
        for (HoldingsRecord record : getHoldingsRecords()) {
            if (record.getTransaction() == transition && ! record.skipped()) {
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

    @Override
    public boolean hasErrors () {
        if (getInstance().failed())
            return true;
        for (InventoryRecord record : allItems)
            if (record.failed())
                return true;
        for (InventoryRecord record : allHoldingsRecords)
            if (record.failed())
                return true;
        return false;
    }

    @Override
    public JsonArray getErrors () {
        JsonArray errors = new JsonArray();
        if (getInstance().failed()) {
            errors.add(getInstance().getError());
        }
        for (HoldingsRecord holdingsRecord : allHoldingsRecords) {
            if (holdingsRecord.failed()) {
                errors.add(holdingsRecord.getError());
            }
        }
        for (Item item : allItems) {
            if (item.failed()) {
                errors.add(item.getError());
            }
        }
        return errors;
    }

}