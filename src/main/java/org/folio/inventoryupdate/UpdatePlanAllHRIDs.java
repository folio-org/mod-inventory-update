package org.folio.inventoryupdate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.*;
import org.folio.inventoryupdate.entities.InventoryRecord.Transaction;
import org.folio.inventoryupdate.instructions.ProcessingInstructionsUpsert;
import org.folio.okapi.common.OkapiClient;

import io.vertx.core.Future;
import io.vertx.core.Promise;

import static org.folio.inventoryupdate.ErrorReport.UNPROCESSABLE_ENTITY;
import static org.folio.inventoryupdate.entities.InventoryRecord.Transaction.*;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.*;

public class UpdatePlanAllHRIDs extends UpdatePlan {

    @Override
    public Repository getNewRepository() {
        return new RepositoryByHrids();
    }

    @Override
    public RequestValidation validateIncomingRecordSets (JsonArray incomingRecordSets) {
        RequestValidation requestValidation = super.validateIncomingRecordSets(incomingRecordSets);
        UpdatePlanAllHRIDs.checkForUniqueHRIDsInBatch(requestValidation, incomingRecordSets);
        return requestValidation;
    }

    @Override
    public Future<List<InventoryUpdateOutcome>> multipleSingleRecordUpserts(RoutingContext routingContext, JsonArray inventoryRecordSets) {
        List<JsonArray> arraysOfOneRecordSet = new ArrayList<>();
        for (Object o : inventoryRecordSets) {
            JsonArray batchOfOne = new JsonArray().add(o);
            arraysOfOneRecordSet.add(batchOfOne);
        }
        return chainSingleRecordUpserts(routingContext, arraysOfOneRecordSet, new UpdatePlanAllHRIDs()::upsertBatch);
    }

    @Override
    public RequestValidation validateIncomingRecordSet(JsonObject inventoryRecordSet) {
        RequestValidation validationErrors = new RequestValidation();
        if (isDeletion) return validationErrors;
        String instanceHRID = inventoryRecordSet.getJsonObject(INSTANCE).getString(HRID_IDENTIFIER_KEY);
        String instanceTitle = inventoryRecordSet.getJsonObject(INSTANCE).getString("title");
        if (instanceHRID == null || instanceHRID.isEmpty()) {
            logger.error("Missing or empty HRID. Instances must have a HRID to be processed by this API. Title: " + instanceTitle);
            validationErrors.registerError(
                    new ErrorReport(
                            ErrorReport.ErrorCategory.VALIDATION,
                            UNPROCESSABLE_ENTITY,
                            "HRID is missing or empty.")
                            .setRequestJson(inventoryRecordSet)
                            .setEntityType(InventoryRecord.Entity.INSTANCE)
                            .setEntity(inventoryRecordSet.getJsonObject(INSTANCE)));
        }
        if (inventoryRecordSet.containsKey(HOLDINGS_RECORDS)) {
            inventoryRecordSet.getJsonArray(HOLDINGS_RECORDS)
                    .stream()
                    .map( rec -> (JsonObject) rec)
                    .forEach( holdingsRecord -> {
                        if (!holdingsRecord.containsKey(HRID_IDENTIFIER_KEY)) {
                            logger.error("Holdings Records must have a HRID to be processed by this API. Received: " + holdingsRecord.encodePrettily());
                            validationErrors.registerError(
                                new ErrorReport(
                                    ErrorReport.ErrorCategory.VALIDATION,
                                    UNPROCESSABLE_ENTITY,
                                    "Holdings must have a HRID to be processed by this API. Title: " + instanceTitle)
                                        .setRequestJson(inventoryRecordSet)
                                        .setShortMessage("Missing HRID in holdings record")
                                        .setEntityType(InventoryRecord.Entity.HOLDINGS_RECORD)
                                        .setEntity(holdingsRecord)
                                        .setDetails(inventoryRecordSet));
                        }
                        if (holdingsRecord.containsKey(ITEMS)) {
                            holdingsRecord.getJsonArray(ITEMS)
                                    .stream()
                                    .map(item -> (JsonObject) item)
                                    .forEach(item -> {
                                        if (!item.containsKey(HRID_IDENTIFIER_KEY)) {
                                            logger.error("Items must have a HRID to be processed by this API. Received: " + item.encodePrettily());
                                            validationErrors.registerError(
                                                 new ErrorReport(
                                                    ErrorReport.ErrorCategory.VALIDATION,
                                                    UNPROCESSABLE_ENTITY,
                                                    "Items must have a HRID to be processed by this API. Title: " + instanceTitle)
                                                         .setRequestJson(inventoryRecordSet)
                                                         .setShortMessage("Missing HRID in Item")
                                                         .setEntity(item)
                                                         .setDetails(inventoryRecordSet));
                                        }
                                    });
                        }
                    });
        }
        return validationErrors;
    }

    public static void checkForUniqueHRIDsInBatch(RequestValidation validation, JsonArray inventoryRecordSets) {
        Set<String> instanceHrids = new HashSet<>();
        Set<String> holdingsHrids = new HashSet<>();
        Set<String> itemHrids = new HashSet<>();

        for (Object recordSetObject : inventoryRecordSets) {
            JsonObject recordSet = (JsonObject) recordSetObject;
            String instanceHrid = recordSet.getJsonObject(INSTANCE).getString(HRID_IDENTIFIER_KEY);
            if (instanceHrid != null) {
                if (instanceHrids.contains(instanceHrid)) {
                    validation.registerError(
                            new ErrorReport(
                                    ErrorReport.ErrorCategory.VALIDATION,
                                    UNPROCESSABLE_ENTITY,
                                    "Instance HRID " + instanceHrid + " occurs more that once in this batch.")
                                    .setShortMessage("Instance HRID is repeated in this batch")
                                    .setEntityType(InventoryRecord.Entity.INSTANCE)
                                    .setEntity(recordSet.getJsonObject(INSTANCE)));
                } else {
                    instanceHrids.add(instanceHrid);
                }
            }
            if (recordSet.containsKey(HOLDINGS_RECORDS)) {
                for (Object holdingsObject : recordSet.getJsonArray(HOLDINGS_RECORDS)) {
                    JsonObject holdingsRecord = ((JsonObject) holdingsObject);
                    String holdingsHrid = holdingsRecord.getString(HRID_IDENTIFIER_KEY);
                    if (holdingsHrid != null) {
                        if (holdingsHrids.contains(holdingsHrid)) {
                            validation.registerError(
                               new ErrorReport(
                                    ErrorReport.ErrorCategory.VALIDATION,
                                    UNPROCESSABLE_ENTITY,
                                    "Holdings record HRID " + holdingsHrid
                                            + " appears more that once in batch.")
                                    .setShortMessage("Recurring HRID detected in batch")
                                    .setEntity(holdingsRecord)
                                    .setEntityType(InventoryRecord.Entity.HOLDINGS_RECORD)
                            );
                        } else {
                            holdingsHrids.add(holdingsHrid);
                        }
                    }
                    if (holdingsRecord.containsKey(ITEMS)) {
                        for (Object itemObject : holdingsRecord.getJsonArray(ITEMS)) {
                            String itemHrid = ((JsonObject) itemObject).getString(HRID_IDENTIFIER_KEY);
                            if (itemHrid != null) {
                                if (itemHrids.contains(itemHrid)) {
                                    validation.registerError(
                                            new ErrorReport(
                                                    ErrorReport.ErrorCategory.VALIDATION,
                                                    UNPROCESSABLE_ENTITY,
                                                    "Item HRID " + itemHrid
                                                            + " appears more than once in batch.")
                                                    .setShortMessage("Recurring HRID detected in batch")
                                                    .setEntityType(InventoryRecord.Entity.ITEM)
                                                    .setEntity((JsonObject) itemObject));
                                } else {
                                    itemHrids.add(itemHrid);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /* PLANNING CREATES, UPDATES, DELETES */
    /**
     * Creates an in-memory representation of the instance, holdings, and items
     * as well as possible instance-to-instance relationships, that need to be created,
     * updated, or deleted in Inventory storage.
     *
     * @return a Future to confirm that plan was created
     */
    @Override
    public UpdatePlanAllHRIDs planInventoryUpdates() {
        for (PairedRecordSets pair : repository.getPairsOfRecordSets()) {
            planInstanceHoldingsAndItems(pair);
        }
        // Do two iterations of pairs because ALL Instances in a batch must be planned before any
        // relations to avoid duplicate HRID scenarios with provisional instances.
        for (PairedRecordSets pair : repository.getPairsOfRecordSets()) {
          planInstanceRelations(pair);
        }
    return this;
    }

    private void planInstanceHoldingsAndItems(PairedRecordSets pair) {
        if (pair.hasIncomingRecordSet()) {
            InventoryRecordSet incomingSet = pair.getIncomingRecordSet();
            Instance incomingInstance = incomingSet.getInstance();
            ProcessingInstructionsUpsert rules = new ProcessingInstructionsUpsert(
                pair.getIncomingRecordSet().getProcessingInfoAsJson());
            if (pair.hasExistingRecordSet()) {
                if (rules.provided()) {
                  pair.getExistingRecordSet().setDeleteInstructions(rules);
                }
                // Updates, deletes
                Instance existingInstance = pair.getExistingRecordSet().getInstance();
                incomingInstance.setTransition(UPDATE).applyOverlays(existingInstance, rules.forInstance());
                if (!incomingInstance.ignoreHoldings()) {
                    // If a record set came in with a list of holdings records (even if it was an empty list)
                    for (HoldingsRecord existingHoldingsRecord : existingInstance.getHoldingsRecords()) {
                        HoldingsRecord incomingHoldingsRecord = incomingInstance.getHoldingsRecordByHRID(
                                existingHoldingsRecord.getHRID());
                        // HoldingsRecord gone, mark for deletion and check for existing items to delete with it
                        // unless instructed to keep certain holdings records even if missing from input
                        if (incomingHoldingsRecord == null) {
                          planToDeleteOrRetainHoldingsRecord(existingHoldingsRecord, rules);
                        } else {
                            // There is an existing holdings record with the same HRID on the same Instance
                            incomingHoldingsRecord.setTransition(UPDATE)
                               .applyOverlays(existingHoldingsRecord, rules.forHoldingsRecord());
                        }
                        for (Item existingItem : existingHoldingsRecord.getItems()) {
                            Item incomingItem = pair.getIncomingRecordSet().getItemByHRID(existingItem.getHRID());
                            if (incomingItem == null) {
                                // An existing Item is gone from the Instance, delete it from storage
                              planToDeleteOrRetainItem(existingItem, rules, existingHoldingsRecord);
                            } else {
                                // Existing Item still exists in incoming record (possibly under a
                                // different holdings record)
                                incomingItem.setTransition(UPDATE);
                                incomingItem.applyOverlays(existingItem, rules.forItem());
                            }
                        }
                    }
                }
            } else {
                incomingInstance.setTransition(CREATE).generateUUIDIfNotProvided();
            }
            // Remaining holdings and item transactions: Creates, imports from other Instance(s)
            // Find incoming holdings we didn't already resolve (UPDATE or DELETE) above
            List<HoldingsRecord> holdingsRecords =
                    incomingSet.getHoldingsRecordsByTransactionType(Transaction.UNKNOWN);
            for (HoldingsRecord holdingsRecord : holdingsRecords) {
              planToCreateNewHoldingsRecordOrMoveExistingOver(holdingsRecord, rules);
            }
            // Find incoming items we didn't already resolve (UPDATE or DELETE) above
            List<Item> items = incomingSet.getItemsByTransactionType(Transaction.UNKNOWN);
            for (Item item : items) {
              planToCreateNewItemOrMoveExistingOver(item, rules, incomingInstance.getUUID());
            }
        }
    }

  private void planToCreateNewItemOrMoveExistingOver(Item item, ProcessingInstructionsUpsert rules, String instanceId) {
    if (repository.existingItemsByHrid.containsKey(item.getHRID())) {
        // Import from different Instance
        Item existing = repository.existingItemsByHrid.get(item.getHRID());
        item.setTransition(UPDATE);
        item.applyOverlays(existing, rules.forItem());
        item.isSwitchingToInstance(instanceId);
    } else {
        // The HRID does not exist in Inventory, create
        item.setTransition(CREATE).generateUUIDIfNotProvided();
    }
  }

  private void planToCreateNewHoldingsRecordOrMoveExistingOver(HoldingsRecord holdingsRecord, ProcessingInstructionsUpsert rules) {
    if (repository.existingHoldingsRecordsByHrid.containsKey(holdingsRecord.getHRID())) {
        // Import from different Instance
        HoldingsRecord existingHoldingsRecord = repository.existingHoldingsRecordsByHrid.get(holdingsRecord.getHRID());
        holdingsRecord.setTransition(UPDATE).applyOverlays(existingHoldingsRecord, rules.forHoldingsRecord());
    } else {
        // The HRID does not exist in Inventory, create
        holdingsRecord.setTransition(CREATE).generateUUIDIfNotProvided();
    }
  }

  private static void planToDeleteOrRetainHoldingsRecord(HoldingsRecord existingHoldingsRecord, ProcessingInstructionsUpsert rules) {
    existingHoldingsRecord.setTransition(DELETE);
    if (rules.forHoldingsRecord().retainOmittedRecord(existingHoldingsRecord)) {
      existingHoldingsRecord.handleDeleteProtection(InventoryRecord.DeletionConstraint.HOLDINGS_RECORD_PATTERN_MATCH);
      existingHoldingsRecord.skip();
    }
  }

  private static void planToDeleteOrRetainItem(Item existingItem, ProcessingInstructionsUpsert rules, HoldingsRecord existingHoldingsRecord) {
    existingItem.setTransition(DELETE);
    // unless item matches an instruction to keep omitted items
    if (rules.forItem().retainOmittedRecord(existingItem)) {
      existingItem.handleDeleteProtection(InventoryRecord.DeletionConstraint.ITEM_PATTERN_MATCH);
      existingItem.skip();
      if (existingHoldingsRecord.isDeleting()) {
        existingHoldingsRecord.handleDeleteProtection(InventoryRecord.DeletionConstraint.ITEM_PATTERN_MATCH);
        existingHoldingsRecord.skip();
      }
    }
    // and unless item appears to be still circulating
    if (existingItem.isCirculating()) {
      existingItem.handleDeleteProtection(InventoryRecord.DeletionConstraint.ITEM_STATUS);
      existingItem.skip();
      if (existingHoldingsRecord.isDeleting()) {
        existingHoldingsRecord.handleDeleteProtection(InventoryRecord.DeletionConstraint.ITEM_STATUS);
        existingHoldingsRecord.skip();
      }
    }
    // and unless item is linked to a purchase order line
    if (existingItem.isInAcquisitions()) {
      existingItem.handleDeleteProtection(InventoryRecord.DeletionConstraint.PO_LINE_REFERENCE);
      existingItem.skip();
      if (existingHoldingsRecord.isDeleting()) {
        existingHoldingsRecord.handleDeleteProtection(InventoryRecord.DeletionConstraint.PO_LINE_REFERENCE);
        existingHoldingsRecord.skip();
      }
    }
  }

  private void planInstanceRelations(PairedRecordSets pair) {
        // Plan creates and deletes
        if (pair.hasIncomingRecordSet()) {
            // Set UUIDs for from-instance and to-instance, create provisional instance if required and possible
            pair.getIncomingRecordSet().resolveIncomingInstanceRelationsUsingRepository((RepositoryByHrids) repository);

            // Plan storage transactions
            for (InstanceToInstanceRelation incomingRelation : pair.getIncomingRecordSet().getInstanceToInstanceRelations()) {
                if (pair.hasExistingRecordSet()) {
                    if (pair.getExistingRecordSet().hasThisRelation(incomingRelation)) {
                        incomingRelation.skip();
                    } else {
                        incomingRelation.setTransition(Transaction.CREATE);
                    }
                } else {
                    incomingRelation.setTransition(Transaction.CREATE);
                }
            }
        }
        if (pair.hasExistingRecordSet()) {
            for (InstanceToInstanceRelation existingRelation : pair.getExistingRecordSet().getInstanceToInstanceRelations()) {
                if (pair.getIncomingRecordSet().isThisRelationOmitted(existingRelation)) {
                    existingRelation.setTransition(Transaction.DELETE);
                } else {
                    existingRelation.setTransition(NONE);
                }
            }
        }
    }

    /* END OF PLANNING METHODS */


    // EXECUTE CREATES, UPDATES, DELETES.

    public Future<Void> doInventoryUpdates(OkapiClient okapiClient) {
        Promise<Void> promise = Promise.promise();
        doCreateRecordsWithDependants(okapiClient).onComplete(prerequisitesCreated ->
          doUpdateInstancesAndHoldings(okapiClient).onComplete(instancesAndHoldingsUpdated ->
            doUpdateItems(okapiClient).onComplete(itemsUpdated -> {
              if (prerequisitesCreated.succeeded()) {
                doCreateInstanceRelations(okapiClient).onComplete(relationsCreated -> {
                  if (instancesAndHoldingsUpdated.succeeded() && itemsUpdated.succeeded()) {
                    doDeleteRelationsItemsHoldings(okapiClient).onComplete(deletes -> {
                      if (deletes.succeeded()) {
                        doCreateItems(okapiClient). onComplete(itemsCreated -> {
                          if (itemsCreated.succeeded()) {
                            if (relationsCreated.succeeded()) {
                              promise.complete();
                            } else {
                              promise.fail(relationsCreated.cause().getMessage());
                            }
                          } else {
                            promise.fail(itemsCreated.cause().getMessage());
                          }
                        });
                      } else {
                        promise.fail(deletes.cause().getMessage());
                      }
                    });
                  } else {
                    if (prerequisitesCreated.failed()) {
                      promise.fail(prerequisitesCreated.cause().getMessage());
                    } else if (instancesAndHoldingsUpdated.failed()) {
                      promise.fail(instancesAndHoldingsUpdated.cause().getMessage());
                    } else if (itemsUpdated.failed()) {
                      promise.fail(itemsUpdated.cause().getMessage());
                    }
                  }
                });
              } else {
                promise.fail(prerequisitesCreated.cause().getMessage());
              }
            })));
        return promise.future();
    }

    public Future<Void> doCreateInstanceRelations(OkapiClient okapiClient){
        Promise<Void> promise = Promise.promise();

        List<Future<JsonObject>> provisionalInstancesFutures = new ArrayList<>();
        for (Instance instance : ((RepositoryByHrids) repository).provisionalInstancesByHrid.values()) {
          provisionalInstancesFutures.add(
              InventoryStorage.postInventoryRecord(
                  okapiClient, instance));

        }
        Future.join(provisionalInstancesFutures).onComplete(allProvisionalInstancesCreated -> {
            if (allProvisionalInstancesCreated.succeeded()) {
                List<Future<JsonObject>> createFutures = new ArrayList<>();
                for (InstanceToInstanceRelation relation : repository.getInstanceRelationsToCreate()) {
                    createFutures.add(InventoryStorage.postInventoryRecord(okapiClient, relation));
                }
                Future.join(createFutures).onComplete( allRelationsCreated -> {
                    if (allRelationsCreated.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(
                                ErrorReport.makeErrorReportFromJsonString(
                                        allRelationsCreated.cause().getMessage())
                                        .setShortMessage(
                                                "UpdatePlan using repository: There was an error creating instance relations")
                                        .asJsonString());
                    }
                });
            } else {
                promise.fail(ErrorReport.makeErrorReportFromJsonString(allProvisionalInstancesCreated.cause().getMessage())
                        .setShortMessage("There was an error creating provisional Instances")
                        .asJsonString());
            }
        });
        return promise.future();
    }
    // END OF STORAGE METHODS

}
