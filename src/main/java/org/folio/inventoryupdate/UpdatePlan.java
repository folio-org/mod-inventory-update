package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.List;

import org.folio.inventoryupdate.InventoryRecordSet.HoldingsRecord;
import org.folio.inventoryupdate.InventoryRecordSet.Instance;
import org.folio.inventoryupdate.InventoryRecordSet.Item;
import org.folio.inventoryupdate.InventoryRecordSet.Transition;
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
    protected final Logger logger = LoggerFactory.getLogger("inventory-matcher");



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
            getIncomingInstance().setTransition(Transition.CREATING);
        } else {
            getIncomingInstance().setUUID(getExistingInstance().getUUID());
            getIncomingInstance().setTransition(Transition.UPDATING);
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
        return existingSet.getItemsByTransitionType(Transition.DELETING);
    }

    public List<HoldingsRecord> holdingsToDelete () {
        return existingSet.getHoldingsRecordsByTransitionType(Transition.DELETING);
    }

    public boolean hasHoldingsToCreate () {
        return holdingsToCreate().size()>0;
    }
    public List<HoldingsRecord> holdingsToCreate () {
        return incomingSet.getHoldingsRecordsByTransitionType(Transition.CREATING);
    }

    public boolean hasItemsToCreate () {
        return itemsToCreate().size()>0;
    }

    public List<Item> itemsToCreate () {
        return incomingSet.getItemsByTransitionType(Transition.CREATING);
    }

    public List<HoldingsRecord> holdingsToUpdate () {
        return incomingSet.getHoldingsRecordsByTransitionType(Transition.UPDATING);
    }

    public List<Item> itemsToUpdate () {
        return incomingSet.getItemsByTransitionType(Transition.UPDATING);
    }

    public boolean isInstanceUpdating () {
        return incomingSet.getInstance().getTransition() == Transition.UPDATING;
    }

    public boolean isInstanceCreating () {
        return incomingSet.getInstance().getTransition() == Transition.CREATING;
    }

    public boolean isInstanceDeleting () {
        return incomingSet.getInstance().getTransition() == Transition.DELETING;
    }

    public void writePlanToLog () {
        logger.info("Planning done: ");
        logger.info("Instance transition: " + getIncomingInstance().getTransition());

        logger.info("Holdings to create: ");
        for (HoldingsRecord record : holdingsToCreate()) {
          logger.info(record.getJson().encodePrettily());
        }
        logger.info("Holdings to update: ");
        for (HoldingsRecord record : holdingsToUpdate()) {
          logger.info(record.getJson().encodePrettily());
        }
        logger.info("Items to create: ");
        for (Item record : itemsToCreate()) {
          logger.info(record.getJson().encodePrettily());
        }
        logger.info("Items to update: ");
        for (Item record : itemsToUpdate()) {
          logger.info(record.getJson().encodePrettily());
        }
        logger.info("Items to delete: ");
        for (Item record : itemsToDelete()) {
          logger.info(record.getJson().encodePrettily());
        }
        logger.info("Holdings to delete: ");
        for (HoldingsRecord record : holdingsToDelete()) {
          logger.info(record.getJson().encodePrettily());
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
            instanceAndHoldingsFutures.add(InventoryStorage.putInstance(okapiClient, getIncomingInstance().getJson(), getIncomingInstance().getUUID()));
        }
        for (HoldingsRecord record : holdingsToUpdate()) {
            instanceAndHoldingsFutures.add(InventoryStorage.putHoldingsRecord(okapiClient, record.getJson(), record.getUUID()));
        }
        CompositeFuture.all(instanceAndHoldingsFutures).onComplete ( allDone -> {
            if (allDone.succeeded()) {
                promise.complete();
            } else {
                promise.fail("Failed to update some non-prerequisite records");
            }
        });

        return promise.future();
    }

    public Future<Void> handleItemUpdatesAndCreatesIfAny (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        @SuppressWarnings("rawtypes")
        List<Future> itemFutures = new ArrayList<Future>();
        for (Item record : itemsToUpdate()) {
            itemFutures.add(InventoryStorage.putItem(okapiClient, record.getJson(), record.getUUID()));
        }
        for (Item record : itemsToCreate()) {
            itemFutures.add((InventoryStorage.postItem(okapiClient, record.getJson())));
        }
        CompositeFuture.all(itemFutures).onComplete ( allItemsDone -> {
            if (allItemsDone.succeeded()) {
                promise.complete();
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
        for (Item record : itemsToDelete()) {
            deleteItems.add(InventoryStorage.deleteItem(okapiClient, record.getUUID()));
        }
        CompositeFuture.all(deleteItems).onComplete ( allItemsDone -> {
            if (allItemsDone.succeeded()) {
                List<Future> deleteHoldingsRecords = new ArrayList<Future>();
                for (HoldingsRecord record : holdingsToDelete()) {
                    deleteHoldingsRecords.add(InventoryStorage.deleteHoldingsRecord(okapiClient, record.getUUID()));
                }
                CompositeFuture.all(deleteHoldingsRecords).onComplete( allHoldingsDone -> {
                    if (allHoldingsDone.succeeded()) {
                        if (isInstanceDeleting()) {
                            Future<Void> promisedInstanceDeletion = InventoryStorage.deleteInstance(okapiClient, getExistingRecordSet().getInstanceUUID());
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
            Future<JsonObject> promisedInstance = InventoryStorage.postInstance(okapiClient, getIncomingInstance().getJson());
            promisedInstance.onComplete(handler -> {
                if (handler.succeeded()) {
                    promise.complete();
                } else {
                    promise.fail("Failed to POST incoming instance");
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
            holdingsRecordCreated.add(InventoryStorage.postHoldingsRecord(okapiClient, record.getJson()));
        }
        CompositeFuture.all(holdingsRecordCreated).onComplete( handler -> {
            if (handler.succeeded()) {
                promise.complete();
            } else {
                promise.fail("Failed to create new holdings records");
            }
        });
        return promise.future();
    }

    /* END OF UPDATE METHODS */


}