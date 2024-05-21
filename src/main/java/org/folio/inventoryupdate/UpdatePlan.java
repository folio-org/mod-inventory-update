package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.*;
import org.folio.inventoryupdate.entities.InventoryRecord.Entity;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.inventoryupdate.instructions.ProcessingInstructionsDeletion;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import static org.folio.inventoryupdate.ErrorReport.UNPROCESSABLE_ENTITY;
import static org.folio.inventoryupdate.InventoryStorage.getOkapiClient;
import static org.folio.inventoryupdate.InventoryUpdateOutcome.MULTI_STATUS;
import static org.folio.inventoryupdate.InventoryUpdateOutcome.OK;

/**
 * Base class for implementing update plans
 * Method 'planInventoryUpdates' is meant to create an in-memory representation of the records
 * to update with all mandatory fields and required identifiers (UUIDs) set. Once the planning
 * is done, the 'incomingSet' should contain records flagged as CREATING or UPDATING and
 * 'existingSet' should contain records flagged as DELETING, if any.
 * The planning phase is not meant to stop on failure (unless some unexpected exception occurs for
 * which there is no planned recovery, of course). Rather it's supposed to register possible
 * record level errors but run to completion.
 * Method 'updateInventory' is meant to run through the 'incomingSet' and 'existingSet' and perform
 * the actual updates in Inventory storage as per the CREATING, UPDATING, and DELETING flags set
 * in the planning phase and in the appropriate order to observe integrity constraints.
 * The execution phase may fail certain operations, skip dependant operations of those that failed,
 * and pick up the error messages along the way. If it thus completes with partial success, it should
 * have updated whatever it could and should return an error code - typically 422 -- and display
 * the error condition in the response.
 * Or, put another way, even if the request results in an HTTP error response code, some Inventory
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

    protected static final Logger logger = LoggerFactory.getLogger("inventory-update");
    protected boolean isDeletion = false;

    protected Repository repository;

    protected UpdatePlan (InventoryRecordSet incomingInventoryRecordSet, InventoryQuery existingInstanceQuery) {
        this.updatingSet = incomingInventoryRecordSet;
        this.instanceQuery = existingInstanceQuery;
    }

    protected UpdatePlan () {}

    public abstract Repository getNewRepository ();

    public abstract Future<List<InventoryUpdateOutcome>> multipleSingleRecordUpserts(
            RoutingContext routingContext, JsonArray inventoryRecordSets);

    public abstract UpdatePlan planInventoryUpdates();

    public abstract Future<Void> doInventoryUpdates(OkapiClient okapiClient);

    public abstract Future<Void> doCreateInstanceRelations (OkapiClient okapiClient);

    public Future<InventoryUpdateOutcome> upsertBatch(RoutingContext routingContext, JsonArray inventoryRecordSets) {
        repository = getNewRepository();
        Promise<InventoryUpdateOutcome> promise = Promise.promise();
        RequestValidation validations = validateIncomingRecordSets (inventoryRecordSets);
        final boolean batchOfOne = (inventoryRecordSets.size() == 1);
        if (validations.passed()) {
            setIncomingRecordSets(inventoryRecordSets)
                    .buildRepositoryFromStorage(routingContext).onComplete(
                            result -> {
                                if (result.succeeded()) {
                                    planInventoryUpdates()
                                            .doInventoryUpdates(
                                                    getOkapiClient(routingContext)).onComplete(inventoryUpdated -> {

                                                JsonObject response = (batchOfOne ?
                                                        getOneUpdatingRecordSetJsonFromRepository() :
                                                        new JsonObject());

                                                if (inventoryUpdated.succeeded()) {
                                                    response.put("metrics", getUpdateMetricsFromRepository().asJson());
                                                    promise.complete(
                                                            new InventoryUpdateOutcome(response)
                                                                    .setResponseStatusCode(OK));
                                                } else {
                                                    handleUpdateError(promise, inventoryUpdated.cause().getMessage(), batchOfOne);
                                                }
                                            });
                                } else {
                                    promise.complete(new InventoryUpdateOutcome(
                                            ErrorReport.makeErrorReportFromJsonString(
                                                            result.cause().getMessage())
                                                    .setShortMessage("Fetching from storage before update failed.")));
                                }
                            });
        } else {
            handleValidationError(promise, validations, batchOfOne);
        }
        return promise.future();
    }

    private void handleUpdateError(Promise<InventoryUpdateOutcome> promise, String errorJson, boolean batchOfOne) {
        // If the error affected an entire batch of records
        // then fail the request as a whole. This particular error
        // outcome will not go to the client but rather be picked
        // up and acted upon by the module.
        //
        // If the error only affected individual records, either
        // because the update was for a batch of one or because the
        // error affected entities that are not batch updated
        // (ie relationships) then return a (partial) success,
        // a multi-status (207) that is.
        ErrorReport report = ErrorReport.makeErrorReportFromJsonString(errorJson);
        if (report.isBatchStorageError() && !batchOfOne) {
            // This will cause the controller to switch to record-by-record upserts
            promise.fail(report.asJsonString());
        } else {
            // Report multi-status, add errors but don't fail promise.
            JsonObject response = batchOfOne ? getOneUpdatingRecordSetJsonFromRepository() : new JsonObject();
            JsonArray errors = new JsonArray();
            errors.add(report.asJson());
            response.put("errors", errors);
            InventoryUpdateOutcome outcome =
                    new InventoryUpdateOutcome(response)
                            .setResponseStatusCode(MULTI_STATUS)
                            .setMetrics(getUpdateMetricsFromRepository());
            promise.complete(outcome);
        }
    }

    private void handleValidationError(Promise<InventoryUpdateOutcome> promise, RequestValidation validations, boolean batchOfOne) {
        ErrorReport report = new ErrorReport(
                ErrorReport.ErrorCategory.VALIDATION,
                UNPROCESSABLE_ENTITY,
                validations.firstMessage())
                .setRequestJson(validations.getFirstRequestJson())
                .setEntityType(validations.firstEntityType())
                .setEntity(validations.firstEntity())
                .setShortMessage(validations.firstShortMessage())
                .setRequestJson(validations.getFirstRequestJson());
        if (batchOfOne) {
            UpdateMetrics metrics = getUpdateMetricsFromRepository();
            metrics.entity(Entity.INSTANCE)
                    .transaction(Transaction.CREATE)
                    .outcomes.increment(InventoryRecord.Outcome.SKIPPED);
            InventoryUpdateOutcome validationErrorOutcome =
                    new InventoryUpdateOutcome(report).setMetrics(metrics);
            promise.complete(validationErrorOutcome);
        } else {
            // Pre-validation of batch of multiple record sets failed, switch to record-by-record upsert
            // to process the good record sets, if any.
            promise.fail(report.asJsonString());
        }
    }

    public UpdatePlan setIncomingRecordSets(JsonArray inventoryRecordSets) {
        repository.setIncomingRecordSets(inventoryRecordSets);
        return this;
    }

    public RequestValidation validateIncomingRecordSets (JsonArray incomingRecordSets) {
        RequestValidation validations = new RequestValidation();
        for (Object recordSetObject : incomingRecordSets) {
            RequestValidation validation = validateIncomingRecordSet((JsonObject) recordSetObject);
            if (validation.hasErrors()) {
                validations.addValidation(validation);
            }
        }
        return validations;
    }

    public Future<Void> buildRepositoryFromStorage (RoutingContext routingContext) {
        long buildRepoStart = System.currentTimeMillis();
        Promise<Void> promise = Promise.promise();
        repository.buildRepositoryFromStorage(routingContext).onComplete(repositoryBuilt -> {
            if (repositoryBuilt.succeeded()) {
                long builtMs = System.currentTimeMillis() - buildRepoStart;
                logger.debug("Repo built in " +  builtMs + " ms.");
                promise.complete();
            } else {
                promise.fail(repositoryBuilt.cause().getMessage());
            }
        });
        return promise.future();
    }


    protected Future<List<InventoryUpdateOutcome>> chainSingleRecordUpserts(RoutingContext routingContext, List<JsonArray> arraysOfOneRecordSet, BiFunction<RoutingContext, JsonArray, Future<InventoryUpdateOutcome>> upsertMethod) {
        Promise<List<InventoryUpdateOutcome>> promise = Promise.promise();
        List<InventoryUpdateOutcome> outcomes = new ArrayList<>();
        Future<InventoryUpdateOutcome> fut = Future.succeededFuture();
        for (JsonArray arrayOfOneRecordSet : arraysOfOneRecordSet) {
            fut = fut.compose(v -> {
                // First time around, a null outcome is passed in
                if (v != null) {
                    outcomes.add(v);
                }
                return upsertMethod.apply(routingContext, arrayOfOneRecordSet);
            });
        }
        fut.onComplete( result -> {
            // capture the last outcome too
            outcomes.add(result.result());
            promise.complete(outcomes);
        });
        return promise.future();
    }

    // DELETION
    /**
     * Constructor for plan for creating or updating an Inventory record set
     * @param existingInstanceQuery The query to use for checking if the instance already exists
     */
    protected UpdatePlan (InventoryQuery existingInstanceQuery) {
        this.instanceQuery = existingInstanceQuery;
        this.isDeletion = true;
    }

    public abstract Future<Void> planInventoryDelete(OkapiClient client, ProcessingInstructionsDeletion deleteInstructions);

    protected Future<InventoryRecordSet> lookupExistingRecordSet (OkapiClient okapiClient, InventoryQuery instanceQuery) {
        Promise<InventoryRecordSet> promise = Promise.promise();
        InventoryStorage.lookupSingleInventoryRecordSet(okapiClient, instanceQuery).onComplete( recordSet -> {
            if (recordSet.succeeded()) {
                JsonObject existingInventoryRecordSetJson = recordSet.result();
                if (existingInventoryRecordSetJson != null) {
                    promise.complete(InventoryRecordSet.makeExistingRecordSet(existingInventoryRecordSetJson));
                } else
                {
                    promise.complete( null );
                }
            } else {
                promise.fail(recordSet.cause().getMessage());
            }
        });
        return promise.future();
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
    public abstract Future<Void> doInventoryDelete(OkapiClient client);
    // DELETION

    public abstract RequestValidation validateIncomingRecordSet (JsonObject inventoryRecordSet);

    /**
     * Set transaction type and ID for the instance
     */
    protected static void prepareTheUpdatingInstance(
            InventoryRecordSet incomingSet, InventoryRecordSet existingSet) {
      if (existingSet != null) {
        incomingSet.getInstance().setUUID(existingSet.getInstance().getUUID());
        incomingSet.getInstance().setTransition(Transaction.UPDATE);
        incomingSet.getInstance().setVersion( existingSet.getInstance().getVersion() );
      } else {
        // Use UUID on incoming record if any, otherwise generate
        if (!incomingSet.getInstance().hasUUID()) {
          incomingSet.getInstance().generateUUID();
        }
        incomingSet.getInstance().setTransition(Transaction.CREATE);
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

    public List<Item> itemsToSilentlyUpdate () {
      return foundExistingRecordSet() ? existingSet.getItemsForSilentUpdate() : new ArrayList<>();
    }



    public List<HoldingsRecord> holdingsToDelete () {
        return foundExistingRecordSet() ? existingSet.getHoldingsRecordsByTransactionType(Transaction.DELETE) : new ArrayList<>();
    }

    public List<HoldingsRecord> holdingsRecordsToSilentlyUpdate () {
      return foundExistingRecordSet() ? existingSet.getHoldingsRecordsForSilentUpdate() : new ArrayList<>();
    }


  public List<InstanceToInstanceRelation> instanceRelationsToDelete() {
        return foundExistingRecordSet() ? existingSet.getInstanceRelationsByTransactionType(Transaction.DELETE) : new ArrayList<>();
    }

    public boolean isInstanceUpdating () {
        return gotUpdatingRecordSet() && updatingSet.getInstance().getTransaction() == Transaction.UPDATE;
    }


    public boolean isInstanceDeleting () {
        return foundExistingRecordSet() && existingSet.getInstance().getTransaction() == Transaction.DELETE;
    }


    /* UPDATE METHODS */


    public Future<Void> doCreateRecordsWithDependants(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        doCreateNewInstances(okapiClient).onComplete(instances -> {
            if (instances.succeeded()) {
                doCreateNewHoldings(okapiClient).onComplete(holdings -> {
                    if (holdings.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(holdings.cause().getMessage());
                    }
                });
            } else {
                promise.fail(instances.cause().getMessage());
            }
        });
        return promise.future();
    }

    public Future<Void> doCreateNewInstances(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        InventoryStorage.postInstances(okapiClient, repository.getInstancesToCreate()).onComplete(
                handler -> {
                    if (handler.succeeded()) {
                        promise.complete();
                    } else {
                        logger.error("Message: " + handler.cause().getMessage());
                        promise.fail(handler.cause().getMessage());
                    }
                });
        return promise.future();
    }

    public Future<Void> doCreateNewHoldings(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        InventoryStorage.postHoldingsRecords(okapiClient, repository.getHoldingsToCreate()).onComplete(
                handler -> {
                    if (handler.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(handler.cause().getMessage());
                    }
                });
        return promise.future();
    }

    public Future<Void> doUpdateInstancesAndHoldings(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        List<Future> updates = new ArrayList<>();
        updates.add(InventoryStorage.postInstances(okapiClient,repository.getInstancesToUpdate()));
        updates.add(InventoryStorage.postHoldingsRecords(okapiClient,repository.getHoldingsToUpdate()));
        CompositeFuture.join(updates).onComplete(handler -> {
            if (handler.succeeded()) {
                promise.complete();
            } else {
                logger.error("Error updating instances/holdings: " + handler.cause().getMessage());
                promise.fail(handler.cause().getMessage());
            }
        });
        return promise.future();
    }

    public Future<Void> doUpdateItems(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        InventoryStorage.postItems(okapiClient, repository.getItemsToUpdate()).onComplete(
                handler -> {
                    if (handler.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(handler.cause().getMessage());
                    }
                });
        return promise.future();
    }

  public Future<Void> doCreateItems(OkapiClient okapiClient) {
    Promise<Void> promise = Promise.promise();
    InventoryStorage.postItems(okapiClient, repository.getItemsToCreate()).onComplete(
        handler -> {
          if (handler.succeeded()) {
            promise.complete();
          } else {
            promise.fail(handler.cause().getMessage());
          }
        });
    return promise.future();
  }


  public Future<Void> doDeleteRelationsItemsHoldings(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        List<Future> deleteRelationsDeleteItems = new ArrayList<>();
        for (InstanceToInstanceRelation relation : repository.getInstanceRelationsToDelete()) {
            deleteRelationsDeleteItems.add(InventoryStorage.deleteInventoryRecord(okapiClient,relation));
        }
        for (Item item : repository.getItemsToDelete()) {
            deleteRelationsDeleteItems.add(InventoryStorage.deleteInventoryRecord(okapiClient, item));
        }
        for (Item item : repository.getDeletingItemsToSilentlyUpdate()) {
            deleteRelationsDeleteItems.add(InventoryStorage.putInventoryRecordOutcomeLess(okapiClient,item));
        }
        CompositeFuture.join(deleteRelationsDeleteItems).onComplete ( relationshipsAndItemsDeleted -> {
            if (relationshipsAndItemsDeleted.succeeded()) {
                List<Future> deleteHoldingsRecords = new ArrayList<>();
                for (HoldingsRecord holdingsRecord : repository.getHoldingsToDelete()) {
                    deleteHoldingsRecords.add(InventoryStorage.deleteInventoryRecord(okapiClient, holdingsRecord));
                }
                for (HoldingsRecord holdingsRecord : repository.getDeletingHoldingsToSilentlyUpdate()) {
                   deleteHoldingsRecords.add(InventoryStorage.putInventoryRecordOutcomeLess(okapiClient, holdingsRecord));
                }
                CompositeFuture.join(deleteHoldingsRecords).onComplete( holdingsDeleted -> {
                    if (holdingsDeleted.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(holdingsDeleted.cause().getMessage());
                    }

                });
            } else {
                promise.fail(relationshipsAndItemsDeleted.cause().getMessage());
            }
        });
        return promise.future();

    }

    /**
     * Perform instance and holdings updates
     * @param okapiClient client for inventory storage requests
     */
    public Future<Void> handleSingleInstanceUpdate(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        @SuppressWarnings("rawtypes")
        List<Future> instanceAndHoldingsFutures = new ArrayList<>();
        if (isInstanceUpdating()) {
            instanceAndHoldingsFutures.add(InventoryStorage.putInventoryRecord(okapiClient, getUpdatingInstance()));
        }
        CompositeFuture.join(instanceAndHoldingsFutures).onComplete ( allDone -> {
            if (allDone.succeeded()) {
                promise.complete();
            } else {
                promise.fail(allDone.cause().getMessage());
            }
        });

        return promise.future();
    }

    /**
     * Perform deletions of any relations to other instances and
     * deletions of items, holdings records, instance (if any and in that order)
     */
    @SuppressWarnings("rawtypes")
    public Future<Void> handleSingleSetDelete(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        List<Future> deleteRelationsDeleteItems = new ArrayList<>();
        for (InstanceToInstanceRelation relation : instanceRelationsToDelete()) {
            deleteRelationsDeleteItems.add(InventoryStorage.deleteInventoryRecord(okapiClient,relation));
        }
        for (Item item : itemsToDelete()) {
            deleteRelationsDeleteItems.add(InventoryStorage.deleteInventoryRecord(okapiClient, item));
        }
        for (Item item : itemsToSilentlyUpdate()) {
            deleteRelationsDeleteItems.add(InventoryStorage.putInventoryRecordOutcomeLess(okapiClient, item));
        }

        CompositeFuture.join(deleteRelationsDeleteItems).onComplete ( allRelationshipsDoneAllItemsDone -> {
            if (allRelationshipsDoneAllItemsDone.succeeded()) {
                List<Future> deleteHoldingsRecords = new ArrayList<>();
                for (HoldingsRecord holdingsRecord : holdingsToDelete()) {
                    deleteHoldingsRecords.add(InventoryStorage.deleteInventoryRecord(okapiClient, holdingsRecord));
                }
                for (HoldingsRecord holdingsRecord : holdingsRecordsToSilentlyUpdate()) {
                  deleteHoldingsRecords.add(InventoryStorage.putInventoryRecordOutcomeLess(okapiClient, holdingsRecord));
                }
                CompositeFuture.join(deleteHoldingsRecords).onComplete( allHoldingsDone -> {
                    if (allHoldingsDone.succeeded()) {
                        if (isInstanceDeleting()) {
                          if (getExistingInstance().skipped()) {
                            InventoryStorage.putInventoryRecordOutcomeLess(okapiClient, getExistingInstance()).onComplete(
                                handler -> {
                                  if (handler.succeeded()) {
                                    promise.complete();
                                  } else {
                                    promise.fail(handler.cause().getMessage());
                                  }
                                }
                            );
                          } else {
                            InventoryStorage.deleteInventoryRecord(okapiClient, getExistingRecordSet().getInstance()).onComplete( handler -> {
                                if (handler.succeeded()) {
                                    promise.complete();
                                } else {
                                    promise.fail(handler.cause().getMessage());
                                }
                            });}
                        } else {
                            promise.complete();
                        }
                    } else {
                        promise.fail(allHoldingsDone.cause().getMessage());
                    }

                });
            } else {
                promise.fail(allRelationshipsDoneAllItemsDone.cause().getMessage());
            }
        });
        return promise.future();
    }

    /* END OF UPDATE METHODS */


    public JsonObject getOneUpdatingRecordSetJsonFromRepository() {
        return (repository.getPairsOfRecordSets().size() == 1
                && repository.getPairsOfRecordSets().get(0).hasIncomingRecordSet()) ?
                repository.getPairsOfRecordSets().get(0).getIncomingRecordSet().asJson()
                : new JsonObject();
    }

    public UpdateMetrics getUpdateMetricsFromRepository() {
        UpdateMetrics metrics = new UpdateMetrics();
        for (PairedRecordSets pair : repository.getPairsOfRecordSets()) {
            if (pair.hasIncomingRecordSet()) {
                InventoryRecordSet updatingSet = pair.getIncomingRecordSet();
                Instance updatingInstance = pair.getIncomingRecordSet().getInstance();
                metrics.entity(Entity.INSTANCE).transaction(updatingInstance.getTransaction()).outcomes.increment(
                        updatingInstance.getOutcome());
                List<InventoryRecord> holdingsRecordsAndItemsInUpdatingSet = Stream.of(
                        updatingSet.getHoldingsRecords(), updatingSet.getItems()).flatMap(
                        Collection::stream).collect(Collectors.toList());
                for (InventoryRecord record : holdingsRecordsAndItemsInUpdatingSet) {
                    metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(record.getOutcome());
                }
                if (!updatingSet.getInstanceToInstanceRelations().isEmpty()) {
                    for (InstanceToInstanceRelation record : updatingSet.getInstanceToInstanceRelations()) {
                        if (!record.getTransaction().equals(InventoryRecord.Transaction.NONE)) {
                            if (record.getTransaction().equals(Transaction.UNKNOWN)) {
                                logger.debug("Cannot increment outcome for transaction UNKNOWN");
                            } else {
                                metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(
                                        record.getOutcome());
                              if (repository instanceof RepositoryByHrids) {
                                Map<String, Instance> provisionalInstances = ((RepositoryByHrids) repository).provisionalInstancesByHrid;
                                if (provisionalInstances.containsKey(record.getReferenceInstanceHrid()))
                                {
                                  Instance provisional = provisionalInstances.get(record.getReferenceInstanceHrid());
                                  ( (UpdateMetrics.InstanceRelationsMetrics) metrics.entity(record.entityType()) ).provisionalInstanceMetrics.increment(
                                      provisional.getOutcome());
                                }
                              }
                            }
                        }
                    }
                }
            }
        }
        if (!repository.getPairsOfRecordSets().isEmpty() && repository.getPairsOfRecordSets().get(
                0).hasExistingRecordSet()) {
            InventoryRecordSet existingSet = repository.getPairsOfRecordSets().get(0).getExistingRecordSet();
            if (existingSet.getInstance().isDeleting()) {
                InventoryRecord record = existingSet.getInstance();
                metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(record.getOutcome());
            }
            List<InventoryRecord> holdingsRecordsAndItemsInExistingSet = Stream.of(existingSet.getHoldingsRecords(),
                    existingSet.getItems()).flatMap(Collection::stream).collect(Collectors.toList());

            for (InventoryRecord record : holdingsRecordsAndItemsInExistingSet) {
                if (record.isDeleting()) {
                    metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(record.getOutcome());
                }
            }
            if (!existingSet.getInstanceToInstanceRelations().isEmpty()) {
                for (InstanceToInstanceRelation record : existingSet.getInstanceToInstanceRelations()) {
                    if (!record.getTransaction().equals(InventoryRecord.Transaction.NONE)) {
                        metrics.entity(record.entityType()).transaction(record.getTransaction()).outcomes.increment(
                                record.getOutcome());
                        if (repository instanceof RepositoryByHrids) {
                          Map<String, Instance> provisionalInstances = ((RepositoryByHrids) repository).provisionalInstancesByHrid;
                          if (provisionalInstances.containsKey(record.getReferenceInstanceHrid()))
                          {
                            Instance provisional = provisionalInstances.get(record.getReferenceInstanceHrid());
                              ( (UpdateMetrics.InstanceRelationsMetrics) metrics.entity(record.entityType()) ).provisionalInstanceMetrics.increment(
                                  provisional.getOutcome());
                          }
                        }
                    }
                }
            }
        }
        return metrics;
    }

    public JsonObject getUpdateStats () {
        UpdateMetrics metrics = new UpdateMetrics();

        if (gotUpdatingRecordSet()) {
            metrics.entity(Entity.INSTANCE).transaction(getUpdatingInstance().getTransaction()).outcomes.increment(getUpdatingInstance().getOutcome());
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
            if (! existingSet.getInstanceToInstanceRelations().isEmpty()) {
                for ( InstanceToInstanceRelation record : existingSet.getInstanceToInstanceRelations() ) {
                    logger.debug("Record: " + record.asJson().encode());
                    logger.debug("Transaction: " + record.getTransaction());
                    logger.debug("Entity type: " + record.entityType());
                    if ( !record.getTransaction().equals( InventoryRecord.Transaction.NONE ) ) {
                        metrics.entity( record.entityType() ).transaction( record.getTransaction() ).outcomes.increment(
                                record.getOutcome() );
                    }
                }
            }
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
