package org.folio.inventoryupdate.entities;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import org.folio.inventoryupdate.UpdateMetrics;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.okapi.common.OkapiClient;

import static org.folio.inventoryupdate.entities.InstanceRelations.*;
import static org.folio.inventoryupdate.entities.RecordIdentifiers.OAI_IDENTIFIER;

public class InventoryRecordSet extends JsonRepresentation {

    private Instance anInstance = null;
    private final Map<String,Item> itemsByHRID = new HashMap<>();
    private final List<HoldingsRecord> allHoldingsRecords = new ArrayList<>();
    private final List<Item> allItems = new ArrayList<>();

    public static final String INSTANCE = "instance";
    public static final String HOLDINGS_RECORDS = "holdingsRecords";
    public static final String ITEMS = "items";
    public static final String HRID_IDENTIFIER_KEY = "hrid";
    public static final String UUID_IDENTIFIER_KEY = "uuid";

    // supporting data for shared inventory updates
    public static final String PROCESSING = "processing";
    public static final String INSTITUTION_ID = "institutionId";
    public static final String LOCAL_IDENTIFIER = "localIdentifier";
    public static final String IDENTIFIER_TYPE_ID = "identifierTypeId";

    @SuppressWarnings("unused")
    private final Logger logger = LoggerFactory.getLogger("inventory-update");

    // Controller handles planning and update logic for instance-to-instance relations
    private InstanceRelations instanceRelations;
    // Instance relations properties that the controller access directly
    public List<InstanceToInstanceRelation> parentRelations = null;
    public List<InstanceToInstanceRelation> childRelations = null;
    public List<InstanceToInstanceRelation> succeedingTitles = null;
    public List<InstanceToInstanceRelation> precedingTitles = null;

    // Incoming, intended relations
    public JsonObject instanceRelationsJson = new JsonObject();
    public InstanceReferences instanceReferences;
    public JsonObject processing = new JsonObject();

    private boolean relationsMaintainedInRepository = false;


    public InventoryRecordSet (JsonObject inventoryRecordSet) {
        if (inventoryRecordSet != null) {
            logger.debug("Creating InventoryRecordSet from " + inventoryRecordSet.encodePrettily());
            sourceJson = new JsonObject(inventoryRecordSet.toString());
            JsonObject instanceJson = inventoryRecordSet.getJsonObject(INSTANCE);
            anInstance = new Instance(instanceJson);
            JsonArray holdings = inventoryRecordSet.getJsonArray(HOLDINGS_RECORDS);
            registerHoldingsRecordsAndItems(holdings);
            instanceRelations = new InstanceRelations(this);
            logger.debug("Caching processing info: " + inventoryRecordSet.getJsonObject( PROCESSING ));
            processing = inventoryRecordSet.getJsonObject( PROCESSING );
        }
    }

    public static boolean isValidInventoryRecordSet(JsonObject inventoryRecordSet) {
        return inventoryRecordSet != null && inventoryRecordSet.containsKey(
                INSTANCE ) && ( inventoryRecordSet.getValue( INSTANCE ) instanceof JsonObject );
    }

    public boolean canLookForRecordsWithPreviousMatchKey() {
        return getLocalIdentifier() != null && getLocalIdentifierTypeId() != null;
    }

    public JsonObject getProcessingInfoAsJson () {
        return processing;
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
                    String itemHrid = itemJson.getString( HRID_IDENTIFIER_KEY );
                    if (itemHrid != null && !itemHrid.isEmpty()) {
                        itemsByHRID.put(itemHrid, item);
                    }
                    holdingsRecord.addItem(item);
                    allItems.add(item);
                }
                String holdingsRecordHrid = holdingsRecordJson.getString( HRID_IDENTIFIER_KEY );
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
    public InstanceRelations getInstanceRelationsController() {
        return instanceRelations;
    }

    public void prepareAllInstanceRelationsForDeletion() {
        instanceRelations.markAllRelationsForDeletion();
    }

    public List<InstanceToInstanceRelation> getInstanceRelationsByTransactionType (Transaction transition) {
        return instanceRelations.getInstanceRelationsByTransactionType(transition);
    }

    public Future<Void> prepareIncomingInstanceRelationRecords(OkapiClient client, String instanceId) {
        return instanceRelations.makeInstanceRelationRecordsFromIdentifiers(client, instanceId);
    }

    public List<InstanceToInstanceRelation> getInstanceToInstanceRelations() {
        return Stream.of(
                parentRelations == null ?
                        new ArrayList<InstanceToInstanceRelation>() : parentRelations,
                childRelations == null ?
                        new ArrayList<InstanceToInstanceRelation>() : childRelations,
                precedingTitles == null ?
                        new ArrayList<InstanceToInstanceRelation>() : precedingTitles,
                succeedingTitles == null ?
                        new ArrayList<InstanceToInstanceRelation>() : succeedingTitles
        ).flatMap(Collection::stream).collect(Collectors.toList());
    }

    public boolean hasThisRelation(InstanceToInstanceRelation relation) {
        for (InstanceToInstanceRelation relationHere : getInstanceToInstanceRelations()) {
            if (relation.equals(relationHere)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A relation is considered omitted if the list it would be in exists but the relation is not in the list.
     *
     * Can be used to signal that relations of a given type should be deleted from the Instance by providing an
     * empty list of that type. When no such list is provided, on the other hand, then existing relations of that type
     * should be retained.
     *
     * @param relation Relationship to check
     * @return true if the relation is not in a provides list of relations, false if its present or if no list was provided
     */
    public boolean isThisRelationOmitted(InstanceToInstanceRelation relation) {
        switch (relation.instanceRelationClass) {
            case TO_PARENT:
                return ((instanceRelationsJson.containsKey(PARENT_INSTANCES)
                        && instanceRelationsJson.getJsonArray(PARENT_INSTANCES).isEmpty())
                        || isThisRelationOmitted(parentRelations, relation));
            case TO_CHILD:
                return ((instanceRelationsJson.containsKey(CHILD_INSTANCES)
                        && instanceRelationsJson.getJsonArray(CHILD_INSTANCES).isEmpty())
                        || isThisRelationOmitted(childRelations, relation));
            case TO_PRECEDING:
                return ((instanceRelationsJson.containsKey(PRECEDING_TITLES)
                        && instanceRelationsJson.getJsonArray(PRECEDING_TITLES).isEmpty())
                        || isThisRelationOmitted(precedingTitles, relation));
            case TO_SUCCEEDING:
                return ((instanceRelationsJson.containsKey(SUCCEEDING_TITLES)
                        && instanceRelationsJson.getJsonArray(SUCCEEDING_TITLES).isEmpty())
                        || isThisRelationOmitted(succeedingTitles, relation));
        }
        return false;
    }

    /**
     * Create Instance relationship records from incoming Instance references by HRIDs, by looking
     * up the corresponding UUIDs for those HRIDs in the repository.
     * Requires that the Instance is already assigned a UUID and that the repository is loaded.
     * @param repository cache containing prefetched referenced Instances.
     */
    public void resolveIncomingInstanceRelationsUsingRepository(Repository repository) {
        relationsMaintainedInRepository = true;
        if (instanceReferences != null) {
            for (InstanceReference reference : instanceReferences.references) {
                if (reference.hasReferenceHrid() || reference.hasReferenceUuid()) {
                    reference.setFromInstanceId(getInstanceUUID());
                    if (reference.hasReferenceHrid()) {
                        Instance referencedInstance = repository.referencedInstancesByHrid.get(
                                reference.getReferenceHrid());
                        if (referencedInstance != null) {
                            reference.setReferencedInstanceId(referencedInstance.getUUID());
                        }
                    } else if (reference.hasReferenceUuid()) {
                        if (reference.getReferenceUuid() != null) {
                            Instance referencedInstance = repository.referencedInstancesByUUID.get(
                                    reference.getReferenceUuid());
                            if (referencedInstance != null) {
                                reference.setReferencedInstanceId(referencedInstance.getUUID());
                            }
                        }
                    }
                    InstanceToInstanceRelation relation = reference.getInstanceToInstanceRelation();
                    if (relation != null) {
                        if (relation.instanceRelationClass == InstanceToInstanceRelation.InstanceRelationsClass.TO_PARENT) {
                            if (parentRelations == null) {
                                parentRelations = new ArrayList<>();
                            }
                            parentRelations.add(relation);
                        }
                        if (relation.instanceRelationClass == InstanceToInstanceRelation.InstanceRelationsClass.TO_CHILD) {
                            if (childRelations == null) {
                                childRelations = new ArrayList<>();
                            }
                            childRelations.add(relation);

                        }
                        if (relation.instanceRelationClass == InstanceToInstanceRelation.InstanceRelationsClass.TO_SUCCEEDING) {
                            if (succeedingTitles == null) {
                                succeedingTitles = new ArrayList<>();
                            }
                            succeedingTitles.add(relation);
                        }
                        if (relation.instanceRelationClass == InstanceToInstanceRelation.InstanceRelationsClass.TO_PRECEDING) {
                            if (precedingTitles == null) {
                                precedingTitles = new ArrayList<>();
                            }
                            precedingTitles.add(relation);
                        }
                    }
                }

            }
        }
    }

    /**
     * A relation is considered omitted from the list if the list exists (is not null) but the relation is not in it.
     * @param list list of relations to check the relation against
     * @param relation the relation to check
     * @return true if a list was provided and the relation is not in the list
     */
    private boolean isThisRelationOmitted(
            List<InstanceToInstanceRelation> list, InstanceToInstanceRelation relation) {
        return ( list != null && !list.contains( relation ) );
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
        if (relationsMaintainedInRepository) {
            JsonObject json = new JsonObject();
            for (InstanceToInstanceRelation relation : getInstanceToInstanceRelations()) {
                JsonObject relationJson = new JsonObject(relation.asJsonString());
                if (relation.hasPreparedProvisionalInstance() && relation.getProvisionalInstance().getHRID() != null) {
                    relationJson.put("CREATE_PROVISIONAL_INSTANCE", relation.getProvisionalInstance().asJson());
                }
                if (!relation.failed() && !(relation.hasPreparedProvisionalInstance() && relation.getProvisionalInstance().getHRID()==null)) {
                    switch ( relation.instanceRelationClass ) {
                        case TO_PARENT:
                            if (!json.containsKey(PARENT_INSTANCES)) json.put(PARENT_INSTANCES, new JsonArray());
                            json.getJsonArray(PARENT_INSTANCES).add(relationJson);
                            break;
                        case TO_CHILD:
                            if (!json.containsKey(CHILD_INSTANCES)) json.put(CHILD_INSTANCES, new JsonArray());
                            json.getJsonArray(CHILD_INSTANCES).add(relationJson);
                            break;
                        case TO_PRECEDING:
                            if (!json.containsKey(PRECEDING_TITLES)) json.put(PRECEDING_TITLES, new JsonArray());
                            json.getJsonArray(PRECEDING_TITLES).add(relationJson);
                            break;
                        case TO_SUCCEEDING:
                            if (!json.containsKey(SUCCEEDING_TITLES)) json.put(SUCCEEDING_TITLES, new JsonArray());
                            json.getJsonArray(SUCCEEDING_TITLES).add(relationJson);
                            break;
                    }
                }
            }
            recordSetJson.put(INSTANCE_RELATIONS, json);

        } else {
            recordSetJson.put(InstanceRelations.INSTANCE_RELATIONS, instanceRelations.asJson());
        }
        recordSetJson.put(PROCESSING, processing);
        return recordSetJson;
    }

}