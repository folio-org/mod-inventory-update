package org.folio.inventoryupdate.entities;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.okapi.common.OkapiClient;

import static org.folio.inventoryupdate.entities.RecordIdentifiers.OAI_IDENTIFIER;

public class InventoryRecordSet extends JsonRepresentation {

    private Instance anInstance = null;
    private final Map<String,HoldingsRecord> holdingsRecordsByHRID = new HashMap<>();
    private final Map<String,Item> itemsByHRID = new HashMap<>();
    private final List<HoldingsRecord> allHoldingsRecords = new ArrayList<>();
    private final List<Item> allItems = new ArrayList<>();

    public static final String INSTANCE = "instance";
    public static final String HOLDINGS_RECORDS = "holdingsRecords";
    public static final String ITEMS = "items";
    public static final String HRID = "hrid";

    // supporting data for shared inventory updates
    public static final String PROCESSING = "processing";
    public static final String INSTITUTION_ID = "institutionId";
    public static final String LOCAL_IDENTIFIER = "localIdentifier";
    public static final String IDENTIFIER_TYPE_ID = "identifierTypeId";

    @SuppressWarnings("unused")
    private final Logger logger = LoggerFactory.getLogger("inventory-update");

    // Controller handles planning and update logic for instance-to-instance relations
    private InstanceRelationsManager instanceRelationsManager;
    // Instance relations properties that the controller access directly
    public List<InstanceToInstanceRelation> parentRelations = null;
    public List<InstanceToInstanceRelation> childRelations = null;
    public List<InstanceToInstanceRelation> succeedingTitles = null;
    public List<InstanceToInstanceRelation> precedingTitles = null;
    public JsonObject instanceRelationsJson = new JsonObject();
    public JsonObject processing = new JsonObject();


    public InventoryRecordSet (JsonObject inventoryRecordSet) {
        if (inventoryRecordSet != null) {
            logger.debug("Creating InventoryRecordSet from " + inventoryRecordSet.encodePrettily());
            sourceJson = new JsonObject(inventoryRecordSet.toString());
            JsonObject instanceJson = inventoryRecordSet.getJsonObject(INSTANCE);
            anInstance = new Instance(instanceJson);
            JsonArray holdings = inventoryRecordSet.getJsonArray(HOLDINGS_RECORDS);
            registerHoldingsRecordsAndItems(holdings);
            instanceRelationsManager = new InstanceRelationsManager(this);
            logger.debug("Caching processing info: " + inventoryRecordSet.getJsonObject( PROCESSING ));
            processing = inventoryRecordSet.getJsonObject( PROCESSING );
        }
    }

    public static boolean isValidInventoryRecordSet(JsonObject inventoryRecordSet) {
        if (inventoryRecordSet != null && inventoryRecordSet.containsKey(INSTANCE)
            && (inventoryRecordSet.getValue(INSTANCE) instanceof JsonObject)) return true;
        return false;
    }

    public boolean canLookForRecordsWithPreviousMatchKey() {
        return getLocalIdentifier() != null && getLocalIdentifierTypeId() != null;
    }

    /**
     * Populate structures `holdingsRecordsByHRID`, `allHoldingsRecords`, `itemsByHRID`, `allItems`,
     * add Item entities to their HoldingsRecord entity, add HoldingsRecord entities to the Instance entity.
     *
     * @param holdingsRecordsWithEmbeddedItems JSON array of holdings records, each record with embedded items.
     */
    private void registerHoldingsRecordsAndItems (JsonArray holdingsRecordsWithEmbeddedItems) {
        if (holdingsRecordsWithEmbeddedItems != null) {
            // If property with zero or more holdings is provided
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
        } else {
            // If no holdings property provided, mark holdings to be ignored (ie don't delete holdings)
            anInstance.ignoreHoldings(true);
        }
    }

    public String getInstitutionIdFromArbitraryHoldingsRecord (Map<String,String> institutionsMap) {
        if (institutionsMap != null && !getHoldingsRecords().isEmpty()) {
            return getHoldingsRecords().get(0).getInstitutionId(institutionsMap);
        } else {
            return "";
        }
    }

    public void modifyInstance (JsonObject updatedInstance) {
        anInstance.replaceJson(updatedInstance);
    }

    public Instance getInstance () {
        return anInstance;
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

    public List<Item> getItems () {
        return allItems;
    }

    public Item getItemByHRID (String itemHrid) {
        return itemsByHRID.get(itemHrid);
    }

    public List<Item> getItemsByTransactionType (Transaction transition) {
        List<Item> records = new ArrayList<>();
        for (Item record : getItems()) {
            if (record.getTransaction() == transition && ! record.skipped()) {
                records.add(record);
            }
        }
        return records;
    }
    public List<HoldingsRecord> getHoldingsRecordsByTransactionType (Transaction transition) {
        List<HoldingsRecord> records = new ArrayList<>();
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

    private List<InventoryRecord> getAllInventoryRecords() {
        return Stream.of(
                Arrays.asList(getInstance()),
                allHoldingsRecords,
                allItems,
                getInstanceRelationsController().getInstanceToInstanceRelations()
        ).flatMap(Collection::stream).collect(Collectors.toList());
    }

    // Instance-to-Instance relations methods
    public InstanceRelationsManager getInstanceRelationsController() {
        return instanceRelationsManager;
    }

    public void prepareAllInstanceRelationsForDeletion() {
        instanceRelationsManager.markAllRelationsForDeletion();
    }

    public List<InstanceToInstanceRelation> getInstanceRelationsByTransactionType (Transaction transition) {
        return instanceRelationsManager.getInstanceRelationsByTransactionType(transition);
    }

    public Future<Void> prepareIncomingInstanceRelationRecords(OkapiClient client, String instanceId) {
        return instanceRelationsManager.makeInstanceRelationRecordsFromIdentifiers(client, instanceId);
    }

    public String getLocalIdentifierTypeId () {
        return (processing != null ? processing.getString(IDENTIFIER_TYPE_ID) : null);
    }

    public String getLocalIdentifier () {
        if (processing != null && processing.containsKey( OAI_IDENTIFIER )) {
            String oaiId = processing.getString( OAI_IDENTIFIER );
            return (oaiId != null ? oaiId.substring(oaiId.lastIndexOf(":")+1) : null);
        } else {
            return (processing != null ? processing.getString(LOCAL_IDENTIFIER) : null);
        }
    }

    // Errors
    @Override
    public boolean hasErrors () {
        for (InventoryRecord record : getAllInventoryRecords()) {
            if (record.failed()) return true;
        }
        return false;
    }

    @Override
    public JsonArray getErrors () {
        JsonArray errors = new JsonArray();
        if (getInstance().failed()) {
            errors.add(getInstance().getError());
        }
        for (InventoryRecord record : getAllInventoryRecords()) {
            if (record.failed()) {
                errors.add(record.getError());
            }
        }
        return errors;
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
        recordSetJson.put( InstanceRelationsManager.INSTANCE_RELATIONS, instanceRelationsManager.asJson());
        recordSetJson.put(PROCESSING, processing);
        return recordSetJson;
    }

}