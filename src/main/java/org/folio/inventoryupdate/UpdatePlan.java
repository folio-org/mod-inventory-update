package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import org.folio.inventoryupdate.entities.*;
import org.folio.inventoryupdate.entities.InventoryRecord.Entity;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Base class for implementing update plans
 *
 * Method 'planInventoryUpdates' is meant to create an in-memory representation of the records
 * to update with all mandatory fields and required identifiers (UUIDs) set. Once the planning
 * is done, the 'incomingSet' should contain records flagged as CREATING or UPDATING and
 * 'existingSet' should contain records flagged as DELETING, if any.
 *
 * The planning phase is not meant to stop on failure (unless some unexpected exception occurs for
 * which there is no planned recovery, of course). Rather it's supposed to register possible
 * record level errors but run to completion.
 *
 * Method 'updateInventory' is meant to run through the 'incomingSet' and 'existingSet' and perform
 * the actual updates in Inventory storage as per the CREATING, UPDATING, and DELETING flags set
 * in the planning phase and in the appropriate order to observe integrity constraints.
 *
 * The execution phase may fail certain operations, skip dependant operations of those that failed,
 * and pick up the error messages along the way. If it thus completes with partial success, it should
 * have updated whatever it could and should return an error code - typically 422 -- and display
 * the error condition in the response.
 *
 * Or, put another way, even if the request results in a HTTP error response code, some Inventory
 * records may have been successfully updated during the processing of the request.
 *
 */
public abstract class UpdatePlan {

    // The record set to update Inventory with - either coming in with the request when creating/updating
    // or being derived from existing records in Inventory when deleting
    protected InventoryRecordSet updatingSet;
    protected InventoryQuery instanceQuery;
    // Existing Inventory records matching either an incoming record set or a set of deletion identifiers
    protected InventoryRecordSet existingSet = null;
    // A secondary existing set that needs updating too (currently relevant only in the context of a shared index).
    protected InventoryRecordSet secondaryExistingSet;

    protected final Logger logger = LoggerFactory.getLogger("inventory-update");
    protected boolean isDeletion = false;

    protected Repository repository;

    /**
     * Constructor for plan for creating or updating an Inventory record set
     * @param incomingSet  The inventory record set from the request
     * @param existingInstanceQuery The query to use for checking if the instance already exists
     */
    public UpdatePlan (InventoryRecordSet incomingSet, InventoryQuery existingInstanceQuery) {
        this.updatingSet = incomingSet;
        this.instanceQuery = existingInstanceQuery;
    }

    public UpdatePlan(Repository repository) {
        this.repository = repository;
    }

    public boolean foundExistingRecordSet () {
      return existingSet != null;
    }

    public boolean gotUpdatingRecordSet () {
      return updatingSet != null;
    }

    protected boolean foundSecondaryExistingSet () {
        return (secondaryExistingSet != null);
    }

    public abstract Future<Void> planInventoryUpdates (OkapiClient client);

    public abstract Future<Void> doInventoryUpdates (OkapiClient client);
    public abstract Future<Void> doInventoryUpdatesUsingRepository (OkapiClient client);
    public abstract RequestValidation validateIncomingRecordSet (JsonObject inventoryRecordSet);

    protected Future<InventoryRecordSet> lookupExistingRecordSet (OkapiClient okapiClient, InventoryQuery instanceQuery) {
        Promise<InventoryRecordSet> promise = Promise.promise();
        InventoryStorage.lookupSingleInventoryRecordSet(okapiClient, instanceQuery).onComplete( recordSet -> {
            if (recordSet.succeeded()) {
                JsonObject existingInventoryRecordSetJson = recordSet.result();
                if (existingInventoryRecordSetJson != null) {
                    promise.complete(new InventoryRecordSet(existingInventoryRecordSetJson));
                } else
                {
                    promise.complete( null );
                }
            } else {
                promise.fail("Error looking up existing record set: " + recordSet.cause().getMessage());
            }
        });
        return promise.future();
    }

    /**
     * Set transaction type and ID for the instance
     */
    protected void prepareTheUpdatingInstance() {
      if (foundExistingRecordSet()) {
        getUpdatingInstance().setUUID(getExistingInstance().getUUID());
        getUpdatingInstance().setTransition(Transaction.UPDATE);
        getUpdatingInstance().setVersion( getExistingInstance().getVersion() );
      } else {
        // Use UUID on incoming record if any, otherwise generate
        if (!getUpdatingInstance().hasUUID()) {
          getUpdatingInstance().generateUUID();
        }
        getUpdatingInstance().setTransition(Transaction.CREATE);
      }
    }

    public Instance getUpdatingInstance() {
        return gotUpdatingRecordSet() ? updatingSet.getInstance() : null;
    }

    public Instance getExistingInstance() {
        return foundExistingRecordSet() ? existingSet.getInstance() : null;
    }

    public InventoryRecordSet getUpdatingRecordSet () {
        return updatingSet;
    }

    public InventoryRecordSet getExistingRecordSet () {
        return existingSet;
    }

    public List<Item> itemsToDelete () {
        return foundExistingRecordSet() ? existingSet.getItemsByTransactionType(Transaction.DELETE) : new ArrayList<>();
    }

    public List<HoldingsRecord> holdingsToDelete () {
        return foundExistingRecordSet() ? existingSet.getHoldingsRecordsByTransactionType(Transaction.DELETE) : new ArrayList<>();
    }

    public List<HoldingsRecord> holdingsToCreate () {
        return gotUpdatingRecordSet() ? updatingSet.getHoldingsRecordsByTransactionType(Transaction.CREATE) : new ArrayList<>();
    }

    public List<Item> itemsToCreate () {
        return gotUpdatingRecordSet() ? updatingSet.getItemsByTransactionType(Transaction.CREATE) : new ArrayList<>();
    }

    public List<HoldingsRecord> holdingsToUpdate () {
        return gotUpdatingRecordSet() ? updatingSet.getHoldingsRecordsByTransactionType(Transaction.UPDATE) : new ArrayList<>();
    }

    public List<Item> itemsToUpdate () {
        return gotUpdatingRecordSet() ? updatingSet.getItemsByTransactionType(Transaction.UPDATE) : new ArrayList<>();
    }

    public List<InstanceToInstanceRelation> instanceRelationsToDelete() {
        return foundExistingRecordSet() ? existingSet.getInstanceRelationsByTransactionType(Transaction.DELETE) : new ArrayList<>();
    }

    public boolean isInstanceUpdating () {
        return gotUpdatingRecordSet() && updatingSet.getInstance().getTransaction() == Transaction.UPDATE;
    }

    public boolean isInstanceCreating () {
        return gotUpdatingRecordSet() && updatingSet.getInstance().getTransaction() == Transaction.CREATE;
    }

    public boolean isInstanceDeleting () {
        return foundExistingRecordSet() && existingSet.getInstance().getTransaction() == Transaction.DELETE;
    }

    public void writePlanToLog () {
        logger.debug("Planning of " + (isDeletion ? " delete " : " create/update ") + " of Inventory records set done: ");
        if (isDeletion) {
            if (foundExistingRecordSet()) {
                logger.debug("Instance transition: " + (gotUpdatingRecordSet() ? getUpdatingInstance().getTransaction() : getExistingInstance().getTransaction()));
                logger.debug("Items to delete: ");
                for (Item record : itemsToDelete()) {
                    logger.debug(record.asJson().encodePrettily());
                }
                logger.debug("Holdings to delete: ");
                for (HoldingsRecord record : holdingsToDelete()) {
                    logger.debug(record.asJson().encodePrettily());
                }
                logger.debug("Relationships to delete: ");
                for (InstanceToInstanceRelation record : instanceRelationsToDelete()) {
                    logger.debug(record.asJson().encodePrettily());
                }
            } else {
                logger.debug("Got delete request but no existing records found with provided identifier(s)");
            }
        } else {
            logger.debug("Instance transition: " + getUpdatingInstance().getTransaction());
            logger.debug("Holdings to create: ");
            for (HoldingsRecord record : holdingsToCreate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Holdings to update: ");
            for (HoldingsRecord record : holdingsToUpdate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Items to create: ");
            for (Item record : itemsToCreate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Items to update: ");
            for (Item record : itemsToUpdate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Items to delete: ");
            for (Item record : itemsToDelete()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Holdings to delete: ");
            for (HoldingsRecord record : holdingsToDelete()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Instance to Instance relations to create: ");
            for (InstanceToInstanceRelation record : getUpdatingRecordSet().getInstanceRelationsController().instanceRelationsToCreate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Provisional Instances to create: ");
            for (Instance record : getUpdatingRecordSet().getInstanceRelationsController().provisionalInstancesToCreate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Instance to Instance relationships to delete: ");
            for (InstanceToInstanceRelation record : instanceRelationsToDelete()) {
                logger.debug(record.asJson().encodePrettily());
            }

        }
    }

    public void writePlanToLogUsingRepository () {
        logger.debug("Planning of " + (isDeletion ? " delete " : " create/update ") + " of Inventory records set done: ");
        if (isDeletion) {
            if (repository.getPairsOfRecordSets().get(0).hasExistingRecordSet()) {

                //logger.debug("Instance transition: " + (gotUpdatingRecordSet() ? getUpdatingInstance().getTransaction() : getExistingInstance().getTransaction()));
                logger.debug("Items to delete: ");
                for (Item record : itemsToDelete()) {
                    logger.debug(record.asJson().encodePrettily());
                }
                logger.debug("Holdings to delete: ");
                for (HoldingsRecord record : holdingsToDelete()) {
                    logger.debug(record.asJson().encodePrettily());
                }
                logger.debug("Relationships to delete: ");
                for (InstanceToInstanceRelation record : instanceRelationsToDelete()) {
                    logger.debug(record.asJson().encodePrettily());
                }
            } else {
                logger.debug("Got delete request but no existing records found with provided identifier(s)");
            }
        } else {

            logger.debug("Instance transition: " + getUpdatingInstance().getTransaction());
            logger.debug("Holdings to create: ");
            for (HoldingsRecord record : holdingsToCreate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Holdings to update: ");
            for (HoldingsRecord record : holdingsToUpdate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Items to create: ");
            for (Item record : itemsToCreate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Items to update: ");
            for (Item record : itemsToUpdate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Items to delete: ");
            for (Item record : itemsToDelete()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Holdings to delete: ");
            for (HoldingsRecord record : holdingsToDelete()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Instance to Instance relations to create: ");
            for (InstanceToInstanceRelation record : getUpdatingRecordSet().getInstanceRelationsController().instanceRelationsToCreate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Provisional Instances to create: ");
            for (Instance record : getUpdatingRecordSet().getInstanceRelationsController().provisionalInstancesToCreate()) {
                logger.debug(record.asJson().encodePrettily());
            }
            logger.debug("Instance to Instance relationships to delete: ");
            for (InstanceToInstanceRelation record : instanceRelationsToDelete()) {
                logger.debug(record.asJson().encodePrettily());
            }

        }
    }


    /* UPDATE METHODS */

    /**
     * Perform storage creates that other updates will depend on for successful completion
     * (must by synchronized)
     */
    public Future<Void> createRecordsWithDependants (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        createNewInstanceIfAny(okapiClient).onComplete( instanceResult -> {
            if (instanceResult.succeeded()) {
                createNewHoldingsIfAny(okapiClient).onComplete(handler2 -> {
                    if (handler2.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail("Failed to create new holdings records: " + handler2.cause().getMessage());
                    }
                });
            } else {
                promise.fail("There was an error trying to create an instance: " + instanceResult.cause().getMessage());
            }
        });
        return promise.future();
    }

    public Future<Void> createRecordsWithDependantsUsingRepository (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        createNewInstancesIfAnyUsingRepository(okapiClient).onComplete(instances -> {
            createNewHoldingsIfAnyUsingRepository(okapiClient).onComplete(holdings -> {
                promise.complete();
            });
        });
        return promise.future();
    }

    public Future<Void> createNewInstancesIfAnyUsingRepository (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        List<Future> instanceCreates = new ArrayList<>();
        for (Instance instance : repository.getInstancesToCreate()) {
            instanceCreates.add(InventoryStorage.postInventoryRecord(okapiClient, instance));
        }
        CompositeFuture.join(instanceCreates).onComplete(handler -> {
            if (handler.succeeded()) {
                logger.info("InstanceCreates complete");
                promise.complete();
            } else {
                promise.fail("Failed to create new instances: " + handler.cause().toString());
            }
        });
        return promise.future();
    }

    public Future<Void> createNewHoldingsIfAnyUsingRepository (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        @SuppressWarnings("rawtypes")
        List<Future> holdingsRecordCreates = new ArrayList<>();
        for (HoldingsRecord record : repository.getHoldingsToCreate()) {
            holdingsRecordCreates.add(InventoryStorage.postInventoryRecord(okapiClient, record));
        }
        CompositeFuture.join(holdingsRecordCreates).onComplete( handler -> {
            if (handler.succeeded()) {
                promise.complete();
            } else {
                promise.fail("Failed to create new holdings records: " + handler.cause().toString());
            }
        });
        return promise.future();
    }

    public Future<Void> handleInstanceAndHoldingsUpdatesIfAnyUsingRepository(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        List<Future> updates = new ArrayList<>();
        for (Instance instance : repository.getInstancesToUpdate()) {
            updates.add(InventoryStorage.putInventoryRecord(okapiClient, instance));
        }
        for (HoldingsRecord holdingsRecord : repository.getHoldingsToUpdate()) {
            updates.add(InventoryStorage.putInventoryRecord(okapiClient, holdingsRecord));
        }
        CompositeFuture.join(updates).onComplete(handler -> {
            if (handler.succeeded()) {
                promise.complete();
            } else {
                promise.fail("Error updating instances or holdings records");
            }
        });
        return promise.future();
    }

    /**
     * Perform instance and holdings updates
     * @param okapiClient client for inventory storage requests
     */
    public Future<Void> handleInstanceAndHoldingsUpdatesIfAny(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        @SuppressWarnings("rawtypes")
        List<Future> instanceAndHoldingsFutures = new ArrayList<>();
        if (isInstanceUpdating()) {
            instanceAndHoldingsFutures.add(InventoryStorage.putInventoryRecord(okapiClient, getUpdatingInstance()));
        }
        for (HoldingsRecord holdingsRecord : holdingsToUpdate()) {
            instanceAndHoldingsFutures.add(InventoryStorage.putInventoryRecord(okapiClient, holdingsRecord));
        }
        CompositeFuture.join(instanceAndHoldingsFutures).onComplete ( allDone -> {
            if (allDone.succeeded()) {
                promise.complete();
            } else {
                promise.fail("Failed to update some non-prerequisite records: " + allDone.cause().getMessage());
            }
        });

        return promise.future();
    }

    public Future<Void> handleItemUpdatesAndCreatesIfAnyUsingRepository (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        List<Future> itemFutures = new ArrayList<>();
        for (Item item: repository.getItemsToUpdate()) {
            item.setVersion( item.getVersion() + 1 );
            itemFutures.add(InventoryStorage.putInventoryRecord(okapiClient, item));
        }
        for (Item item: repository.getItemsToCreate()) {
            itemFutures.add((InventoryStorage.postInventoryRecord(okapiClient, item)));
        }
        CompositeFuture.join(itemFutures).onComplete ( allItemsDone -> {
            if (allItemsDone.succeeded()) {
                promise.complete();
            } else {
                promise.fail("There was an error updating/creating items: " + allItemsDone.cause().getMessage());
            }
        });
        return promise.future();
    }

    public Future<JsonObject> handleItemUpdatesAndCreatesIfAny (OkapiClient okapiClient) {
        Promise<JsonObject> promise = Promise.promise();
        @SuppressWarnings("rawtypes")
        List<Future> itemFutures = new ArrayList<>();
        for (Item item : itemsToUpdate()) {
            // Since the items were updated by storage behind the scenes, triggered by the holdings record updates,
            // the Item's versions have to be bumped again.
            item.setVersion( item.getVersion() + 1 );
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

    public Future<Void> handleDeletionsIfAnyUsingRepository (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        List<Future> deleteRelationsDeleteItems = new ArrayList<>();
        for (InstanceToInstanceRelation relation : repository.getInstanceRelationsToDelete()) {
            deleteRelationsDeleteItems.add(InventoryStorage.deleteInventoryRecord(okapiClient,relation));
        }
        for (Item item : repository.getItemsToDelete()) {
            deleteRelationsDeleteItems.add(InventoryStorage.deleteInventoryRecord(okapiClient, item));
        }
        CompositeFuture.join(deleteRelationsDeleteItems).onComplete ( relationshipsAndItemsDeleted -> {
            if (relationshipsAndItemsDeleted.succeeded()) {
                List<Future> deleteHoldingsRecords = new ArrayList<>();
                for (HoldingsRecord holdingsRecord : repository.getHoldingsToDelete()) {
                    deleteHoldingsRecords.add(InventoryStorage.deleteInventoryRecord(okapiClient, holdingsRecord));
                }
                CompositeFuture.join(deleteHoldingsRecords).onComplete( holdingsDeleted -> {
                    if (holdingsDeleted.succeeded()) {
                        if (isInstanceDeleting()) {
                            InventoryStorage.deleteInventoryRecord(okapiClient, getExistingRecordSet().getInstance()).onComplete( handler -> {
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
                        promise.fail("There was a problem deleting holdings record(s): " + holdingsDeleted.cause().getMessage());
                    }

                });
            } else {
                promise.fail("Failed to delete item(s) and/or instance-to-instance relations: " + relationshipsAndItemsDeleted.cause().getMessage());
            }
        });
        return promise.future();

    }
    /**
     * Perform deletions of any relations to other instances and
     * deletions of items, holdings records, instance (if any and in that order)
     */
    @SuppressWarnings("rawtypes")
    public Future<Void> handleDeletionsIfAny (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        List<Future> deleteRelationsDeleteItems = new ArrayList<>();
        for (InstanceToInstanceRelation relation : instanceRelationsToDelete()) {
            deleteRelationsDeleteItems.add(InventoryStorage.deleteInventoryRecord(okapiClient,relation));
        }
        for (Item item : itemsToDelete()) {
            deleteRelationsDeleteItems.add(InventoryStorage.deleteInventoryRecord(okapiClient, item));
        }

        CompositeFuture.join(deleteRelationsDeleteItems).onComplete ( allRelationshipsDoneAllItemsDone -> {
            if (allRelationshipsDoneAllItemsDone.succeeded()) {
                List<Future> deleteHoldingsRecords = new ArrayList<>();
                for (HoldingsRecord holdingsRecord : holdingsToDelete()) {
                    deleteHoldingsRecords.add(InventoryStorage.deleteInventoryRecord(okapiClient, holdingsRecord));
                }
                CompositeFuture.join(deleteHoldingsRecords).onComplete( allHoldingsDone -> {
                    if (allHoldingsDone.succeeded()) {
                        if (isInstanceDeleting()) {
                            InventoryStorage.deleteInventoryRecord(okapiClient, getExistingRecordSet().getInstance()).onComplete( handler -> {
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
                promise.fail("Failed to delete item(s) and/or instance-to-instance relations: " + allRelationshipsDoneAllItemsDone.cause().getMessage());
            }
        });
        return promise.future();
    }

    public Future<Void> createNewInstanceIfAny (OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        if (isInstanceCreating()) {
            InventoryStorage.postInventoryRecord(okapiClient, getUpdatingInstance()).onComplete(handler -> {
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
        List<Future> holdingsRecordCreated = new ArrayList<>();
        for (HoldingsRecord record : holdingsToCreate()) {
            holdingsRecordCreated.add(InventoryStorage.postInventoryRecord(okapiClient, record));
        }
        CompositeFuture.join(holdingsRecordCreated).onComplete( handler -> {
            if (handler.succeeded()) {
                promise.complete();
            } else {
                promise.fail("Failed to create new holdings records: " + handler.cause().toString());
            }
        });
        return promise.future();
    }

    /* END OF UPDATE METHODS */

    public JsonObject getUpdatingRecordSetJson () {
        return gotUpdatingRecordSet() ? updatingSet.asJson() : new JsonObject();
    }

    public JsonObject getUpdatingRecordSetJsonFromRepository () {
        return repository.getPairsOfRecordSets().get(0).hasIncomingRecordSet() ?
                repository.getPairsOfRecordSets().get(0).getIncomingRecordSet().asJson() : new JsonObject();
    }

    public JsonObject getUpdateStatsFromRepository() {
        UpdateMetrics metrics = new UpdateMetrics();
        if (repository.getPairsOfRecordSets().get(0).hasIncomingRecordSet()) {
            InventoryRecordSet updatingSet = repository.getPairsOfRecordSets().get(0).getIncomingRecordSet();
            Instance updatingInstance = repository.getPairsOfRecordSets().get(0).getIncomingRecordSet().getInstance();
            metrics.entity(Entity.INSTANCE).transaction(updatingInstance.getTransaction()).outcomes.increment(updatingInstance.getOutcome());
            List<InventoryRecord> holdingsRecordsAndItemsInUpdatingSet = Stream.of(
                    updatingSet.getHoldingsRecords(),
                    updatingSet.getItems())
                    .flatMap(Collection::stream).collect(Collectors.toList());
            for (InventoryRecord record : holdingsRecordsAndItemsInUpdatingSet) {
                metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(record.getOutcome());
            }
            updatingSet.getInstanceRelationsController().writeToStats(metrics);
        }
        if (repository.getPairsOfRecordSets().get(0).hasExistingRecordSet()) {
            InventoryRecordSet existingSet = repository.getPairsOfRecordSets().get(0).getExistingRecordSet();
            if (existingSet.getInstance().isDeleting()) {
                InventoryRecord record = existingSet.getInstance();
                metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(record.getOutcome());
            }
            List<InventoryRecord> holdingsRecordsAndItemsInExistingSet = Stream.of(
                    existingSet.getHoldingsRecords(),
                    existingSet.getItems()
            ).flatMap(Collection::stream).collect(Collectors.toList());

            for (InventoryRecord record : holdingsRecordsAndItemsInExistingSet) {
                if (record.isDeleting()) {
                    metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(record.getOutcome());
                }
            }
            existingSet.getInstanceRelationsController().writeToStats(metrics);

        }
        return metrics.asJson();
    }

    public JsonObject getUpdateStats () {
        UpdateMetrics metrics = new UpdateMetrics();

        if (gotUpdatingRecordSet()) {
          metrics.entity(Entity.INSTANCE).transaction(getUpdatingInstance().getTransaction()).outcomes.increment(getUpdatingInstance().getOutcome());
          List<InventoryRecord> holdingsRecordsAndItemsInUpdatingSet = Stream.of(
                  updatingSet.getHoldingsRecords(),
                  updatingSet.getItems())
                  .flatMap(Collection::stream).collect(Collectors.toList());

          for (InventoryRecord record : holdingsRecordsAndItemsInUpdatingSet) {
              metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(record.getOutcome());
          }
          updatingSet.getInstanceRelationsController().writeToStats(metrics);
        }

        if (foundExistingRecordSet()) {
          if (existingSet.getInstance().isDeleting()) {
              InventoryRecord record = existingSet.getInstance();
              metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(record.getOutcome());
          }
          List<InventoryRecord> holdingsRecordsAndItemsInExistingSet = Stream.of(
              existingSet.getHoldingsRecords(),
              existingSet.getItems()
          ).flatMap(Collection::stream).collect(Collectors.toList());

          for (InventoryRecord record : holdingsRecordsAndItemsInExistingSet) {
              if (record.isDeleting()) {
                  metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(record.getOutcome());
              }
          }
          existingSet.getInstanceRelationsController().writeToStats(metrics);
        }

        if (foundSecondaryExistingSet()) {
            InventoryRecord instanceRecord = secondaryExistingSet.getInstance();
            metrics.entity(instanceRecord.entityType()).transaction(instanceRecord.getTransaction()).outcomes.increment(instanceRecord.getOutcome());
            List<InventoryRecord> holdingsRecordsAndItemsInSecondaryExistingSet = Stream.of(
                    secondaryExistingSet.getHoldingsRecords(),
                    secondaryExistingSet.getItems()
            ).flatMap( Collection::stream ).collect( Collectors.toList());
            for (InventoryRecord record : holdingsRecordsAndItemsInSecondaryExistingSet) {
                if (record.isDeleting()) {
                    metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(record.getOutcome());
                }
            }
        }
        return metrics.asJson();
    }

    public JsonArray getErrors () {
        // todo: combine error sets?
        return isDeletion ? getExistingRecordSet().getErrors() : getUpdatingRecordSet().getErrors();
    }
}