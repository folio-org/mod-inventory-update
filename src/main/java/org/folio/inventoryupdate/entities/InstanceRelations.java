package org.folio.inventoryupdate.entities;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.UpdateMetrics;

/**
 * Instance-to-Instance relations are held in the InventoryRecordSet class but the planning and update logic
 * is performed by this controller.
 */
public class InstanceRelations extends JsonRepresentation {

    // JSON property keys
    public static final String INSTANCE_RELATIONS = "instanceRelations";
    public static final String EXISTING_PARENT_CHILD_RELATIONS = "existingParentChildRelations";
    public static final String PARENT_INSTANCES = "parentInstances";
    public static final String CHILD_INSTANCES = "childInstances";
    public static final String SUCCEEDING_TITLES = "succeedingTitles";
    public static final String PRECEDING_TITLES = "precedingTitles";
    public static final String EXISTING_PRECEDING_SUCCEEDING_TITLES = "existingPrecedingSucceedingTitles";
    public static final String INSTANCE_IDENTIFIER = "instanceIdentifier";
    public static final String ID = "id";
    public static final String INSTANCE_TYPE_ID = "instanceTypeId";
    public static final String TITLE = "title";
    public static final String SOURCE = "source";
    // EOF JSON property keys

    public static final String LF = System.lineSeparator();
    InventoryRecordSet irs;
    protected static final Logger logger = LoggerFactory.getLogger("inventory-update");

    public InstanceRelations(InventoryRecordSet inventoryRecordSet) {
        this.irs = inventoryRecordSet;
    }


    /**
     * Executing plan: Force Instance relation creation to fail; used when the planning logic detected a problem creating a
     * provisional record to relate to.
     * @param relation  the problematic Instance relation
     */
    public static Future<Void> failRelationCreation(InstanceToInstanceRelation relation) {
        Promise<Void> promise = Promise.promise();
        promise.fail(relation.getErrorAsJson().encodePrettily());
        return promise.future();
    }

    public static Future<Void> failProvisionalInstanceCreation (Instance provisionalInstance) {
        Promise<Void> promise = Promise.promise();
        promise.fail(provisionalInstance.getErrorAsJson().encodePrettily());
        return promise.future();
    }

    @Override
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        for (InstanceToInstanceRelation relation : irs.getInstanceToInstanceRelations()) {
            JsonObject relationJson = new JsonObject(relation.asJsonString());
            if (relation.hasPreparedProvisionalInstance()) {
                relationJson.put("CREATE_PROVISIONAL_INSTANCE", relation.getProvisionalInstance().asJson());
            }
            switch (relation.instanceRelationClass) {
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
        return json;
    }

    @Override
    public JsonArray getErrors() {
        return null;
    }


    public void writeToStats(UpdateMetrics metrics) {
        if (! irs.getInstanceToInstanceRelations().isEmpty()) {
            for ( InstanceToInstanceRelation record : irs.getInstanceToInstanceRelations() ) {
                logger.debug("Record: " + record.jsonRecord.encode());
                logger.debug("Transaction: " + record.getTransaction());
                logger.debug("Entity type: " + record.entityType);
                if ( !record.getTransaction().equals( InventoryRecord.Transaction.NONE ) ) {
                    metrics.entity( record.entityType ).transaction( record.transaction ).outcomes.increment(
                            record.getOutcome() );
                    if ( record.requiresProvisionalInstanceToBeCreated() ) {
                        Instance provisionalInstance = record.getProvisionalInstance();
                        ( (UpdateMetrics.InstanceRelationsMetrics) metrics.entity( record.entityType ) ).provisionalInstanceMetrics.increment(
                                provisionalInstance.getOutcome() );
                    }
                }
            }
        }
    }

    @Override
    public String toString () {
        StringBuilder str = new StringBuilder();
        for (InstanceToInstanceRelation rel : irs.getInstanceToInstanceRelations()) {
            str.append(rel.toString());
        }
        return str.toString();
    }

}
