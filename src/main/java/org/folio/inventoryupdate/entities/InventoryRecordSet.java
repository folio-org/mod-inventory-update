package org.folio.inventoryupdate.entities;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import org.folio.inventoryupdate.ErrorReport;
import org.folio.inventoryupdate.InventoryUpdateOutcome;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.instructions.ProcessingInstructionsDeletion;
import org.folio.inventoryupdate.instructions.ProcessingInstructionsUpsert;

import static org.folio.inventoryupdate.ErrorReport.BAD_REQUEST;
import static org.folio.inventoryupdate.entities.RecordIdentifiers.OAI_IDENTIFIER;

public class InventoryRecordSet extends JsonRepresentation {

    private boolean isExisting = false;

    private Instance theInstance = null;
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


    // Instance relations properties that the controller access directly
    public List<InstanceToInstanceRelation> parentRelations = null;
    public List<InstanceToInstanceRelation> childRelations = null;
    public List<InstanceToInstanceRelation> succeedingTitles = null;
    public List<InstanceToInstanceRelation> precedingTitles = null;

    // Incoming, intended relations
    public JsonObject instanceRelationsJson;
    public InstanceReferences instanceReferences;
    public JsonObject processing = new JsonObject();


    private InventoryRecordSet (JsonObject inventoryRecordSet) {
        if (inventoryRecordSet != null) {
            logger.debug("Creating InventoryRecordSet from " + inventoryRecordSet.encodePrettily());
            sourceJson = new JsonObject(inventoryRecordSet.toString());
            theInstance = new Instance(new JsonObject(inventoryRecordSet.getJsonObject(INSTANCE).encode()), sourceJson);
            registerHoldingsRecordsAndItems(inventoryRecordSet.getJsonArray(HOLDINGS_RECORDS));
            instanceRelationsJson = (sourceJson.containsKey(InstanceReferences.INSTANCE_RELATIONS) ? sourceJson.getJsonObject(
                    InstanceReferences.INSTANCE_RELATIONS) : new JsonObject());
            logger.debug("Caching processing info: " + inventoryRecordSet.getJsonObject( PROCESSING ));
            processing = inventoryRecordSet.getJsonObject( PROCESSING );
        }
    }

    public boolean isExisting () {
        return isExisting;
    }

    public static InventoryRecordSet makeExistingRecordSet(JsonObject inventoryRecordSet) {
        InventoryRecordSet set = new InventoryRecordSet(inventoryRecordSet);
        set.isExisting = true;
        if (!set.instanceRelationsJson.isEmpty()) {
            set.registerRelationshipJsonRecords(
                    set.getInstance().getUUID(),
                    set.instanceRelationsJson);
        }
        return set;
    }

    public static InventoryRecordSet makeIncomingRecordSet(JsonObject inventoryRecordSet) {
        InventoryRecordSet set = new InventoryRecordSet(inventoryRecordSet);
        if (!set.instanceRelationsJson.isEmpty()) {
            set.instanceReferences = new InstanceReferences(set.instanceRelationsJson, set.sourceJson);
        }
        return set;
    }

    public static InventoryUpdateOutcome isValidInventoryRecordSet(JsonObject inventoryRecordSet) {
        if (inventoryRecordSet != null
                && inventoryRecordSet.containsKey(INSTANCE )
                && ( inventoryRecordSet.getValue( INSTANCE ) instanceof JsonObject )) {
            return new InventoryUpdateOutcome();
        } else {
            return new InventoryUpdateOutcome(
                    new ErrorReport(
                            ErrorReport.ErrorCategory.VALIDATION,
                            BAD_REQUEST,
                            "Did not recognize input as an Inventory record set")
                            .setRequestJson(inventoryRecordSet)
                            .setShortMessage("Not an Inventory record set.")
                            .setEntity(inventoryRecordSet));
        }
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
                JsonObject holdingsRecordJson = new JsonObject(((JsonObject) holdings).encode());
                JsonArray items = new JsonArray();
                if (holdingsRecordJson.containsKey(ITEMS)) {
                    items = extractJsonArrayFromObject(holdingsRecordJson, ITEMS);
                }
                HoldingsRecord holdingsRecord = new HoldingsRecord(holdingsRecordJson, sourceJson);
                for (Object object : items) {
                    JsonObject itemJson = (JsonObject) object;
                    Item item = new Item(itemJson, sourceJson);
                    String itemHrid = itemJson.getString( HRID_IDENTIFIER_KEY );
                    if (itemHrid != null && !itemHrid.isEmpty()) {
                        itemsByHRID.put(itemHrid, item);
                    }
                    holdingsRecord.addItem(item);
                    allItems.add(item);
                }
                allHoldingsRecords.add(holdingsRecord);
                theInstance.addHoldingsRecord(holdingsRecord);
            }
        } else {
            // If no holdings property provided, mark holdings to be ignored (ie don't delete holdings)
            theInstance.ignoreHoldings(true);
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
        theInstance.replaceJson(updatedInstance);
    }

    public Instance getInstance () {
        return theInstance;
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
        for (Item item : getItems()) {
            if (item.getTransaction() == transition && ! item.skipped()) {
                records.add(item);
            }
        }
        return records;
    }

    public List<HoldingsRecord> getHoldingsRecordsForSilentUpdate () {
      List<HoldingsRecord> records = new ArrayList<>();
      for (HoldingsRecord holdingsRecord : getHoldingsRecords()) {
        if (holdingsRecord.updateSilently) {
          records.add(holdingsRecord);
        }
      }
      return records;
    }

    public List<Item> getItemsForSilentUpdate () {
      List<Item> records = new ArrayList<>();
      for (Item item : getItems()) {
        if (item.updateSilently) {
          records.add(item);
        }
      }
      return records;
    }




    public List<HoldingsRecord> getHoldingsRecordsByTransactionType (Transaction transition) {
        List<HoldingsRecord> records = new ArrayList<>();
        for (HoldingsRecord holdingsRecord : getHoldingsRecords()) {
            if (holdingsRecord.getTransaction() == transition && ! holdingsRecord.skipped()) {
                records.add(holdingsRecord);
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
                getInstanceToInstanceRelations()
        ).flatMap(Collection::stream).collect(Collectors.toList());
    }

    // INSTANCE-TO-INSTANCE RELATIONS METHODS

    /**
     * Planning: Takes Instance relation records from storage and creates Instance relations objects
     *
     * @param instanceId        The ID of the Instance to create relationship objects for
     * @param instanceRelations a set of relations from storage
     */
    public void registerRelationshipJsonRecords(String instanceId, JsonObject instanceRelations) {
        if (instanceRelations.containsKey(InstanceReferences.EXISTING_PARENT_CHILD_RELATIONS)) {
            JsonArray existingRelations = instanceRelations.getJsonArray(
                    InstanceReferences.EXISTING_PARENT_CHILD_RELATIONS);
            for (Object o : existingRelations) {
                InstanceRelationship relationship = InstanceRelationship.makeRelationshipFromJsonRecord(instanceId, (JsonObject) o);
                if (relationship.isRelationToChild()) {
                    if (childRelations == null) childRelations = new ArrayList<>();
                    childRelations.add(relationship);
                } else {
                    if (parentRelations == null) parentRelations = new ArrayList<>();
                    parentRelations.add(relationship);
                }
            }
        }
        if (instanceRelations.containsKey(InstanceReferences.EXISTING_PRECEDING_SUCCEEDING_TITLES)) {
            JsonArray existingTitles = instanceRelations.getJsonArray(
                    InstanceReferences.EXISTING_PRECEDING_SUCCEEDING_TITLES);
            for (Object o : existingTitles) {
                InstanceTitleSuccession relation = InstanceTitleSuccession.makeInstanceTitleSuccessionFromJsonRecord(instanceId, (JsonObject) o);
                if (relation.isSucceedingTitle()) {
                    if (succeedingTitles == null) succeedingTitles = new ArrayList<>();
                    succeedingTitles.add(relation);
                } else {
                    if (precedingTitles == null) precedingTitles = new ArrayList<>();
                    precedingTitles.add(relation);
                }
            }
        }
    }

    public void prepareInstanceRelationsForDeleteOrSkip() {
        for (InstanceToInstanceRelation relation : getInstanceToInstanceRelations()) {
            relation.setTransition(InventoryRecord.Transaction.DELETE);
            if (getInstance().isDeleting() && getInstance().skipped()) {
              relation.skip();
            }
        }
    }

    public List<InstanceToInstanceRelation> getInstanceRelationsByTransactionType (Transaction transition) {
        List<InstanceToInstanceRelation> records = new ArrayList<>();
        for (InstanceToInstanceRelation instanceToInstanceRelation : getInstanceToInstanceRelations())  {
            if (instanceToInstanceRelation.getTransaction() == transition && ! instanceToInstanceRelation.skipped()) {
                records.add(instanceToInstanceRelation);
            }
        }
        return records;

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
                return ((instanceRelationsJson.containsKey(InstanceReferences.PARENT_INSTANCES)
                        && instanceRelationsJson.getJsonArray(InstanceReferences.PARENT_INSTANCES).isEmpty())
                        || isThisRelationOmitted(parentRelations, relation));
            case TO_CHILD:
                return ((instanceRelationsJson.containsKey(InstanceReferences.CHILD_INSTANCES)
                        && instanceRelationsJson.getJsonArray(InstanceReferences.CHILD_INSTANCES).isEmpty())
                        || isThisRelationOmitted(childRelations, relation));
            case TO_PRECEDING:
                return ((instanceRelationsJson.containsKey(InstanceReferences.PRECEDING_TITLES)
                        && instanceRelationsJson.getJsonArray(InstanceReferences.PRECEDING_TITLES).isEmpty())
                        || isThisRelationOmitted(precedingTitles, relation));
            case TO_SUCCEEDING:
                return ((instanceRelationsJson.containsKey(InstanceReferences.SUCCEEDING_TITLES)
                        && instanceRelationsJson.getJsonArray(InstanceReferences.SUCCEEDING_TITLES).isEmpty())
                        || isThisRelationOmitted(succeedingTitles, relation));
        }
        return false;
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

    /**
     * Create Instance relationship records from incoming Instance references by HRIDs, by looking
     * up the corresponding UUIDs for those HRIDs in the repository.
     * Requires that the Instance is already assigned a UUID and that the repository is loaded.
     * @param repository cache containing prefetched referenced Instances.
     */
    public void resolveIncomingInstanceRelationsUsingRepository(RepositoryByHrids repository) {
        if (instanceReferences != null) {
            for (InstanceReference reference : instanceReferences.references) {
                if (reference.hasReferenceHrid() || reference.hasReferenceUuid()) {
                    reference.setFromInstanceId(getInstanceUUID());
                    if (reference.hasReferenceHrid()) {
                        Instance referencedInstance =
                            repository.referencedInstancesByHrid.get(reference.getReferenceHrid());
                        if (referencedInstance == null) {
                          referencedInstance = repository.getCreatingInstanceByHrid(reference.getReferenceHrid());
                        }
                        if (referencedInstance == null) {
                          if (repository.provisionalInstancesByHrid.containsKey(reference.getReferenceHrid())) {
                            referencedInstance = repository.provisionalInstancesByHrid.get(reference.getReferenceHrid());
                          } else {
                            referencedInstance = reference.getProvisionalInstance();
                            repository.provisionalInstancesByHrid.put(referencedInstance.getHRID(),referencedInstance);
                          }
                        }
                        if (referencedInstance != null) {
                            reference.setReferencedInstanceId(referencedInstance.getUUID());
                        }
                    } else if (reference.hasReferenceUuid() && reference.getReferenceUuid() != null) {
                        Instance referencedInstance = repository.referencedInstancesByUUID.get(
                                reference.getReferenceUuid());
                        if (referencedInstance != null) {
                            reference.setReferencedInstanceId(referencedInstance.getUUID());
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

  public void setDeleteInstructions(ProcessingInstructionsDeletion deleteInstructions) {
    getInstance().setDeleteInstructions(deleteInstructions.forInstance().getRecordRetention(), deleteInstructions.forInstance().getStatisticalCoding());
    for (HoldingsRecord rec : getHoldingsRecords()) {
      rec.setDeleteInstructions(deleteInstructions.forHoldingsRecord().getRecordRetention(), deleteInstructions.forHoldingsRecord().getStatisticalCoding());
    }
    for (Item rec : getItems()) {
      rec.setDeleteInstructions(deleteInstructions.forItem().getRecordRetention(), deleteInstructions.forItem().getStatisticalCoding());
    }
  }

  public void setDeleteInstructions(ProcessingInstructionsUpsert instructions) {
      getInstance().setDeleteInstructions(instructions.forInstance().recordRetention, instructions.forInstance().statisticalCoding);
    for (HoldingsRecord rec : getHoldingsRecords()) {
      rec.setDeleteInstructions(instructions.forHoldingsRecord().recordRetention, instructions.forHoldingsRecord().statisticalCoding);
    }
    for (Item rec : getItems()) {
      rec.setDeleteInstructions(instructions.forItem().recordRetention, instructions.forItem().statisticalCoding);
    }
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

    @Override
    public JsonArray getErrors () {
        JsonArray errors = new JsonArray();
        for (InventoryRecord inventoryRecord : getAllInventoryRecords()) {
            if (inventoryRecord.failed()) {
                errors.add(inventoryRecord.getErrorAsJson());
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
        JsonObject relationsJson = new JsonObject();
        for (InstanceToInstanceRelation relation : getInstanceToInstanceRelations()) {
            JsonObject relationJson = new JsonObject(relation.asJsonString());
            if (relation.hasPreparedProvisionalInstance() && relation.getProvisionalInstance().getHRID() != null) {
                relationJson.put("CREATE_PROVISIONAL_INSTANCE", relation.getProvisionalInstance().asJson());
            }
            if (!relation.failed() && !(relation.hasPreparedProvisionalInstance() && relation.getProvisionalInstance().getHRID()==null)) {
                switch ( relation.instanceRelationClass ) {
                    case TO_PARENT:
                        if (!relationsJson.containsKey(InstanceReferences.PARENT_INSTANCES)) relationsJson.put(
                                InstanceReferences.PARENT_INSTANCES, new JsonArray());
                        relationsJson.getJsonArray(InstanceReferences.PARENT_INSTANCES).add(relationJson);
                        break;
                    case TO_CHILD:
                        if (!relationsJson.containsKey(InstanceReferences.CHILD_INSTANCES)) relationsJson.put(
                                InstanceReferences.CHILD_INSTANCES, new JsonArray());
                        relationsJson.getJsonArray(InstanceReferences.CHILD_INSTANCES).add(relationJson);
                        break;
                    case TO_PRECEDING:
                        if (!relationsJson.containsKey(InstanceReferences.PRECEDING_TITLES)) relationsJson.put(
                                InstanceReferences.PRECEDING_TITLES, new JsonArray());
                        relationsJson.getJsonArray(InstanceReferences.PRECEDING_TITLES).add(relationJson);
                        break;
                    case TO_SUCCEEDING:
                        if (!relationsJson.containsKey(InstanceReferences.SUCCEEDING_TITLES)) relationsJson.put(
                                InstanceReferences.SUCCEEDING_TITLES, new JsonArray());
                        relationsJson.getJsonArray(InstanceReferences.SUCCEEDING_TITLES).add(relationJson);
                        break;
                }
            }
        }
        recordSetJson.put(InstanceReferences.INSTANCE_RELATIONS, relationsJson);
        recordSetJson.put(PROCESSING, processing);
        return recordSetJson;
    }

}
