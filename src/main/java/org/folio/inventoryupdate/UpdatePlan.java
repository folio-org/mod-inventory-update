package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.List;

import org.folio.inventoryupdate.entities.HoldingsRecord;
import org.folio.inventoryupdate.entities.Instance;
import org.folio.inventoryupdate.entities.InventoryRecord;
import org.folio.inventoryupdate.entities.Item;
import org.folio.inventoryupdate.entities.InventoryRecord.Entity;
import org.folio.inventoryupdate.entities.InventoryRecord.Outcome;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Base class for implementing update plans
 *
 * Method 'planInventoryUpdates' is meant to create an in-memory representation of the records
 * to update with all mandatory fields and required identifiers (UUIDs) set. Once the planning
 * is run, the 'incomingSet' should contain records flagged as CREATING or UPDATING and
 * 'existingSet' should contain records flagged as DELETING, if any.
 *
 * Method 'updateInventory' is meant to run through 'incomingSet' and 'existingSet' and perform
 * updates as per the CREATING, UPDATING, and DELETING flags set in the planning phase -- in the
 * appropriate order to observe integrity constraints.
 *
 */
public abstract class UpdatePlan {

    protected InventoryRecordSet incomingSet;
    protected InventoryQuery instanceQuery;
    protected InventoryRecordSet existingSet = new InventoryRecordSet(null);
    protected final Logger logger = LoggerFactory.getLogger("inventory-update");



    public UpdatePlan (InventoryRecordSet incomingSet, InventoryQuery existingInstanceQuery) {
        this.incomingSet = incomingSet;
        this.instanceQuery = existingInstanceQuery;
    }

    public abstract Future<Void> planInventoryUpdates (OkapiClient client);

    public abstract Future<Void> doInventoryUpdates (OkapiClient client);

    protected Future<Void> lookupExistingRecordSet (OkapiClient okapiClient, InventoryQuery instanceQuery) {
        Promise<Void> promise = Promise.promise();
        Future<JsonObject> promisedExistingInventoryRecordSet = InventoryStorage.lookupSingleInventoryRecordSet(okapiClient, instanceQuery);
        promisedExistingInventoryRecordSet.onComplete( recordSet -> {
            if (recordSet.succeeded()) {
                JsonObject existingInventoryRecordSetJson = recordSet.result();
                this.existingSet = new InventoryRecordSet(existingInventoryRecordSetJson);

                promise.complete();
            } else {
                promise.fail("Error looking up existing record set");
            }
        });
        return promise.future();
    }

    /**
     * Flag transaction type and set ID for the instance
     * @param existingInstance
     * @param incomingInstance
     */
    protected void flagAndIdTheInstance () {
        if (getExistingInstance() == null) {
            getIncomingInstance().generateUUID();
            getIncomingInstance().setTransition(Transaction.CREATE);
        } else {
            getIncomingInstance().setUUID(getExistingInstance().UUID());
            getIncomingInstance().setTransition(Transaction.UPDATE);
        }
    }


    public Instance getIncomingInstance() {
        return incomingSet.getInstance();
    }

    public Instance getExistingInstance() {
        return existingSet.getInstance();
    }

    public InventoryRecordSet getIncomingRecordSet () {
        return incomingSet;
    }

    public InventoryRecordSet getExistingRecordSet () {
        return existingSet;
    }

    public List<Item> itemsToDelete () {
        return existingSet.getItemsByTransitionType(Transaction.DELETE);
    }

    public List<HoldingsRecord> holdingsToDelete () {
        return existingSet.getHoldingsRecordsByTransitionType(Transaction.DELETE);
    }

    public boolean hasHoldingsToCreate () {
        return holdingsToCreate().size()>0;
    }
    public List<HoldingsRecord> holdingsToCreate () {
        return incomingSet.getHoldingsRecordsByTransitionType(Transaction.CREATE);
    }

    public boolean hasItemsToCreate () {
        return itemsToCreate().size()>0;
    }

    public List<Item> itemsToCreate () {
        return incomingSet.getItemsByTransitionType(Transaction.CREATE);
    }

    public List<HoldingsRecord> holdingsToUpdate () {
        return incomingSet.getHoldingsRecordsByTransitionType(Transaction.UPDATE);
    }

    public List<Item> itemsToUpdate () {
        return incomingSet.getItemsByTransitionType(Transaction.UPDATE);
    }

    public boolean isInstanceUpdating () {
        return incomingSet.getInstance().getTransaction() == Transaction.UPDATE;
    }

    public boolean isInstanceCreating () {
        return incomingSet.getInstance().getTransaction() == Transaction.CREATE;
    }

    public boolean isInstanceDeleting () {
        return incomingSet.getInstance().getTransaction() == Transaction.DELETE;
    }

    public void writePlanToLog () {
        logger.info("Planning done: ");
        logger.info("Instance transition: " + getIncomingInstance().getTransaction());

        logger.info("Holdings to create: ");
        for (HoldingsRecord record : holdingsToCreate()) {
          logger.info(record.asJson().encodePrettily());
        }
        logger.info("Holdings to update: ");
        for (HoldingsRecord record : holdingsToUpdate()) {
          logger.info(record.asJson().encodePrettily());
        }
        logger.info("Items to create: ");
        for (Item record : itemsToCreate()) {
          logger.info(record.asJson().encodePrettily());
        }
        logger.info("Items to update: ");
        for (Item record : itemsToUpdate()) {
          logger.info(record.asJson().encodePrettily());
        }
        logger.info("Items to delete: ");
        for (Item record : itemsToDelete()) {
          logger.info(record.asJson().encodePrettily());
        }
        logger.info("Holdings to delete: ");
        for (HoldingsRecord record : holdingsToDelete()) {
          logger.info(record.asJson().encodePrettily());
        }
    }

    /* UPDATE METHODS */

    /**
     * Perform storage creates that other updates will depend on for succesful completion
     * (must by synchronized)
     */
    public Future<Void> createRecordsWithDependants (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        Future<Void> promisedNewInstanceIfAny = createNewInstanceIfAny(okapiClient);
        promisedNewInstanceIfAny.onComplete( handler -> {
            if (handler.succeeded()) {
                Future<Void> promisedNewHoldingsIfAny = createNewHoldingsIfAny(okapiClient);
                promisedNewHoldingsIfAny.onComplete(handler2 -> {
                    if (promisedNewHoldingsIfAny.succeeded()) {
                        logger.debug("Created new holdings");
                        promise.complete();
                    } else {
                        promise.fail("Failed to create new holdings records");
                    }
                });
            } else {
                incomingSet.skipHoldingsAndItems();
                promise.fail("There was an error trying to create an instance: " + handler.cause().getMessage());
            }
        });
        return promise.future();
    }

    /**
     * Perform instance and holdings updates
     * @param okapiClient
     * @return
     */
    public Future<Void> handleInstanceAndHoldingsUpdatesIfAny(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        @SuppressWarnings("rawtypes")
        List<Future> instanceAndHoldingsFutures = new ArrayList<Future>();
        if (isInstanceUpdating()) {
            instanceAndHoldingsFutures.add(InventoryStorage.putInventoryRecord(okapiClient, getIncomingInstance()));
        }
        for (HoldingsRecord holdingsRecord : holdingsToUpdate()) {
            instanceAndHoldingsFutures.add(InventoryStorage.putInventoryRecord(okapiClient, holdingsRecord));
        }
        CompositeFuture.join(instanceAndHoldingsFutures).onComplete ( allDone -> {
            if (allDone.succeeded()) {
                promise.complete();
            } else {
                promise.fail("Failed to update some non-prerequisite records");
            }
        });

        return promise.future();
    }

    public Future<JsonObject> handleItemUpdatesAndCreatesIfAny (OkapiClient okapiClient) {
        Promise<JsonObject> promise = Promise.promise();
        @SuppressWarnings("rawtypes")
        List<Future> itemFutures = new ArrayList<Future>();
        for (Item item : itemsToUpdate()) {
            itemFutures.add(InventoryStorage.putInventoryRecord(okapiClient, item));
        }
        for (Item item : itemsToCreate()) {
            itemFutures.add((InventoryStorage.postInventoryRecord(okapiClient, item)));
        }
        CompositeFuture.join(itemFutures).onComplete ( allItemsDone -> {
            if (allItemsDone.succeeded()) {
                promise.complete(new JsonObject());
            } else {
                promise.fail("There was an error updating/creating items: " + allItemsDone.cause().getMessage());
            }
        });
        return promise.future();
    }

    /**
     * Perform deletions of items, holdings records, instance (if any and in that order)
     */
    @SuppressWarnings("rawtypes")
    public Future<Void> handleDeletionsIfAny (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        List<Future> deleteItems = new ArrayList<Future>();
        for (Item item : itemsToDelete()) {
            deleteItems.add(InventoryStorage.deleteInventoryRecord(okapiClient, item));
        }
        CompositeFuture.join(deleteItems).onComplete ( allItemsDone -> {
            if (allItemsDone.succeeded()) {
                List<Future> deleteHoldingsRecords = new ArrayList<Future>();
                for (HoldingsRecord holdingsRecord : holdingsToDelete()) {
                    deleteHoldingsRecords.add(InventoryStorage.deleteInventoryRecord(okapiClient, holdingsRecord));
                }
                CompositeFuture.join(deleteHoldingsRecords).onComplete( allHoldingsDone -> {
                    if (allHoldingsDone.succeeded()) {
                        if (isInstanceDeleting()) {
                            Future<JsonObject> promisedInstanceDeletion = InventoryStorage.deleteInventoryRecord(okapiClient, getExistingRecordSet().getInstance());
                            promisedInstanceDeletion.onComplete( handler -> {
                                if (handler.succeeded()) {
                                    promise.complete();
                                } else {
                                    promise.fail("Failed to delete instance: " + handler.cause().getMessage());
                                }
                            });
                        } else {
                            promise.complete();
                        }
                    } else {
                        promise.fail("There was a problem deleting holdings record(s): " + allHoldingsDone.cause().getMessage());
                    }

                });
            } else {
                promise.fail("Failed to delete item(s): " + allItemsDone.cause().getMessage());
            }
        });
        return promise.future();
    }


    public Future<Void> createNewInstanceIfAny (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        if (isInstanceCreating()) {
            Future<JsonObject> promisedInstance = InventoryStorage.postInventoryRecord(okapiClient, getIncomingInstance());
            promisedInstance.onComplete(handler -> {
                if (handler.succeeded()) {
                    promise.complete();
                } else {
                    promise.fail("Failed to POST incoming instance: " + handler.cause().getMessage());
                }
            });
        } else {
            promise.complete();
        }
        return promise.future();
    }

    public Future<Void> createNewHoldingsIfAny (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        @SuppressWarnings("rawtypes")
        List<Future> holdingsRecordCreated = new ArrayList<Future>();
        for (HoldingsRecord record : holdingsToCreate()) {
            holdingsRecordCreated.add(InventoryStorage.postInventoryRecord(okapiClient, record));
        }
        CompositeFuture.join(holdingsRecordCreated).onComplete( handler -> {
            if (handler.succeeded()) {
                promise.complete();
            } else {
                promise.fail("Failed to create new holdings records");
            }
        });
        return promise.future();
    }

    /* END OF UPDATE METHODS */

    public JsonObject getPostedRecordSet () {
        return incomingSet.asJson();
    }


    public JsonObject getUpdateStats () {

        JsonObject stats = new JsonObject();
        String outcomeStats = "{ \"" + Outcome.COMPLETED + "\": 0, \"" + Outcome.FAILED + "\": 0, \"" + Outcome.SKIPPED + "\": 0 }";
        String transactionStats = "{ \""+ Transaction.CREATE + "\": " + outcomeStats + ", \""
                                        + Transaction.UPDATE + "\": " + outcomeStats + ", \""
                                        + Transaction.DELETE + "\": " + outcomeStats + " }";
        stats.put(Entity.INSTANCE.toString(), new JsonObject(transactionStats));
        stats.put(Entity.HOLDINGSRECORD.toString(), new JsonObject(transactionStats));
        stats.put(Entity.ITEM.toString(), new JsonObject(transactionStats));

        JsonObject instance = stats.getJsonObject(Entity.INSTANCE.toString());
        JsonObject instanceOutcomes = instance.getJsonObject(getIncomingInstance().getTransaction().toString());
        instanceOutcomes.put(getIncomingInstance().getOutcome().toString(), instanceOutcomes.getInteger(getIncomingInstance().getOutcome().toString())+1);

        for (InventoryRecord record : incomingSet.getHoldingsRecords()) {
            JsonObject entity = stats.getJsonObject(record.entityType().toString());
            JsonObject outcomes = entity.getJsonObject(record.getTransaction().toString());
            outcomes.put(record.getOutcome().toString(), outcomes.getInteger(record.getOutcome().toString())+1);
        }

        for (InventoryRecord record : incomingSet.getItems()) {
            JsonObject entity = stats.getJsonObject(record.entityType().toString());
            JsonObject outcomes = entity.getJsonObject(record.getTransaction().toString());
            outcomes.put(record.getOutcome().toString(), outcomes.getInteger(record.getOutcome().toString())+1);
        }

        return stats;
    }
}