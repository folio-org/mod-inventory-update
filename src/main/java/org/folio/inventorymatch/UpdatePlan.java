package org.folio.inventorymatch;

import java.util.ArrayList;
import java.util.List;

import org.folio.inventorymatch.InventoryRecordSet.HoldingsRecord;
import org.folio.inventorymatch.InventoryRecordSet.Instance;
import org.folio.inventorymatch.InventoryRecordSet.Item;
import org.folio.inventorymatch.InventoryRecordSet.Transition;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.CompositeFuture;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Base class for implementing update plans (by creating an in-memory representation of the records to update)
 *
 * Implementing classes would perform their specific Inventory update scenario in the method planInventoryUpdates
 * based on the incoming record set held up against an existing record set (if any)
 */
public abstract class UpdatePlan {

    protected InventoryRecordSet incomingSet;
    protected InventoryRecordSet existingSet = null;
    protected final Logger logger = LoggerFactory.getLogger("inventory-matcher");


    public UpdatePlan (InventoryRecordSet incomingSet, InventoryRecordSet existingSet, OkapiClient okapiClient) {
        this.incomingSet = incomingSet;
        this.existingSet = existingSet;
    }

    public abstract Future<Void> planInventoryUpdates (OkapiClient client);

    public Future<Void> updateInventory (OkapiClient okapiClient) {
      Promise<Void> promise = Promise.promise();
      Future<Void> promisedPrerequisites = createRecordsWithDependants(okapiClient);
      promisedPrerequisites.onComplete(prerequisites -> {
        if (prerequisites.succeeded()) {
            logger.debug("Successfully created records referenced by other records if any");

            // this has issues for updating holdings and items concurrently
            /*
            @SuppressWarnings("rawtypes")
            List<Future> testFutures = new ArrayList<Future>();
            testFutures.add(handleInstanceAndHoldingsUpdatesIfAny(okapiClient));
            testFutures.add(handleItemUpdatesAndCreatesIfAny(okapiClient));
            CompositeFuture.all(testFutures).onComplete(composite -> {
                if (composite.succeeded()) {
                    Future<Void> promisedDeletes = handleDeletionsIfAny(okapiClient);
                    promisedDeletes.onComplete(deletes -> {
                        if (deletes.succeeded()) {
                            logger.debug("Successfully processed deletions if any.");
                            promise.complete();
                        } else {
                            promise.fail("There was a problem processing deletes " + deletes.cause().getMessage());
                        }
                    });
                } else {
                    promise.fail("Failed to successfully process instance, holdings, item updates: " + composite.cause().getMessage());
                }
            });
            */

            /* This works by updating holdings and items non-concurrently */
            Future<Void> promisedInstanceAndHoldingsUpdates = handleInstanceAndHoldingsUpdatesIfAny(okapiClient);
            promisedInstanceAndHoldingsUpdates.onComplete( instanceAndHoldingsUpdates -> {
                if (instanceAndHoldingsUpdates.succeeded()) {
                    logger.debug("Successfully processed instance and holdings updates if any");
                    Future<Void> promisedItemUpdates = handleItemUpdatesAndCreatesIfAny (okapiClient);
                    promisedItemUpdates.onComplete(itemUpdatesAndCreates -> {
                        if (itemUpdatesAndCreates.succeeded()) {
                            Future<Void> promisedDeletes = handleDeletionsIfAny(okapiClient);
                            promisedDeletes.onComplete(deletes -> {
                                if (deletes.succeeded()) {
                                    logger.debug("Successfully processed deletions if any.");
                                    promise.complete();
                                } else {
                                    promise.fail("There was a problem processing deletes " + deletes.cause().getMessage());
                                }
                            });
                        } else {
                            promise.fail("Error updating items: " + itemUpdatesAndCreates.cause().getMessage());
                        }
                    });
                } else {
                    promise.fail("Failed to process reference record(s) (instances,holdings): " + prerequisites.cause().getMessage());
                }
            });
            /* end */
        } else {
            promise.fail("Failed to create prerequisites");
        }
      });
      return promise.future();
    }

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


    public Instance getIncomingInstance() {
        return incomingSet.getInstance();
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

}