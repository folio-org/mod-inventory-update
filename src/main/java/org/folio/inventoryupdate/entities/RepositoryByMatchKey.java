package org.folio.inventoryupdate.entities;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.ErrorReport;
import org.folio.inventoryupdate.InventoryStorage;
import org.folio.inventoryupdate.QueryByListOfIds;
import org.folio.inventoryupdate.QueryShiftingMatchKey;
import org.folio.inventoryupdate.UpdatePlanSharedInventory;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.folio.inventoryupdate.entities.InstanceRelations.logger;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.*;

public class RepositoryByMatchKey extends Repository {

  protected final Map<String,Instance> existingInstancesByMatchKey = new HashMap<>();
  public final Map<String,Instance> secondaryInstancesByLocalIdentifier = new HashMap<>();


  public Future<Void> buildRepositoryFromStorage (RoutingContext routingContext) {
    Promise<Void> promise = Promise.promise();
    List<Future> existingRecordsByMatchKeyFutures = new ArrayList<>();
    for (List<String> idList : getSubListsOfFive(getIncomingMatchKeys())) {
      existingRecordsByMatchKeyFutures.add(requestInstancesByMatchKeys(routingContext, idList));
    }
    for (PairedRecordSets pair : pairsOfRecordSets) {
      QueryShiftingMatchKey query = new QueryShiftingMatchKey(
              pair.getIncomingRecordSet().getLocalIdentifier(),
              pair.getIncomingRecordSet().getLocalIdentifierTypeId(),
              pair.getIncomingRecordSet().getInstance().getMatchKey());
      existingRecordsByMatchKeyFutures.add(requestInstanceWithOtherMatchKey(routingContext,query));
    }
    CompositeFuture.join(existingRecordsByMatchKeyFutures).onComplete(recordsByMatchKeys -> {
      if (recordsByMatchKeys.succeeded()) {
        List<Future> holdingsRecordsFutures = new ArrayList<>();
        for (List<String> idList : getSubListsOfFifty(getExistingInstanceIdsIncludingSecondaryInstances())) {
          holdingsRecordsFutures.add(requestHoldingsRecordsByInstanceIds(routingContext, idList));
        }
        CompositeFuture.join(holdingsRecordsFutures).onComplete(holdingsRecordsByInstanceIds -> {
          if (holdingsRecordsByInstanceIds.succeeded()) {
            List<Future> itemsFutures = new ArrayList<>();
            for (List<String> idList : getSubListsOfFifty(getExistingHoldingsRecordIds())) {
              itemsFutures.add(requestItemsByHoldingsRecordIds(routingContext, idList));
            }
            CompositeFuture.join(itemsFutures).onComplete(itemsByHoldingsRecordIds -> {
              if (itemsByHoldingsRecordIds.succeeded()) {
                setExistingRecordSets();
                mapLocationsToInstitutionIds(routingContext).onComplete(locationMapUpdate -> {
                  if (locationMapUpdate.succeeded()) {
                    promise.complete();
                  } else {
                    promise.fail(locationMapUpdate.cause().getMessage());
                  }
                });

              } else {
                promise.fail(itemsByHoldingsRecordIds.cause().getMessage());
              }
            });
          } else {
            promise.fail(holdingsRecordsByInstanceIds.cause().getMessage());
          }
        });
      } else {
        promise.fail(recordsByMatchKeys.cause().getMessage());
      }
    });
    return promise.future();
  }

  protected List<String> getExistingInstanceIds () {
    return new ArrayList<>(existingInstancesByUUID.keySet());
  }

  protected List<String> getExistingInstanceIdsIncludingSecondaryInstances () {
    List<String> instanceIds = new ArrayList<>();
    instanceIds.addAll(getExistingInstanceIds());
    for (Instance instance : secondaryInstancesByLocalIdentifier.values()) {
      instanceIds.add(instance.getUUID());
    }
    return instanceIds;
  }

  protected void setExistingRecordSets () {
    for (PairedRecordSets pair : pairsOfRecordSets) {
      String incomingInstanceMatchKey = pair.getIncomingRecordSet().getInstance().getMatchKey();
      if (existingInstancesByMatchKey.containsKey(incomingInstanceMatchKey)) {
        JsonObject existingRecordSetJson = new JsonObject();
        Instance existingInstance = existingInstancesByMatchKey.get(incomingInstanceMatchKey);
        existingRecordSetJson.put(INSTANCE, existingInstance.asJson());
        if (existingHoldingsRecordsByInstanceId.containsKey(existingInstance.getUUID())) {
          JsonArray holdingsWithItems = new JsonArray();
          for (HoldingsRecord holdingsRecord : existingHoldingsRecordsByInstanceId.get(
                  existingInstance.getUUID()).values()) {
            JsonObject jsonRecord = holdingsRecord.asJson();
            if (existingItemsByHoldingsRecordId.containsKey(holdingsRecord.getUUID())) {
              jsonRecord.put(ITEMS, new JsonArray());
              for (Item item : existingItemsByHoldingsRecordId.get(holdingsRecord.getUUID()).values()) {
                jsonRecord.getJsonArray(ITEMS).add(item.asJson());
              }
            }
            holdingsWithItems.add(jsonRecord);
          }
          existingRecordSetJson.put(HOLDINGS_RECORDS, holdingsWithItems);
        }
        InventoryRecordSet existingSet = InventoryRecordSet.makeExistingRecordSet(existingRecordSetJson);
        pair.setExistingRecordSet(existingSet);
      } else {
        logger.debug( "No existing Instance MatchKey [" + incomingInstanceMatchKey + "] found in repo.");
      }
    }
  }

  private List<String> getIncomingMatchKeys () {
    List<String> matchKeys = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      matchKeys.add(pair.getIncomingRecordSet().getInstance().getMatchKey());
    }
    return matchKeys;
  }

  private Future<Void> requestInstancesByMatchKeys(RoutingContext routingContext,
                                                   List<String> matchKeys) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupInstances(okapiClient,
                    new QueryByListOfIds("matchKey", matchKeys))
            .onComplete(instances -> {
              if (instances.succeeded()) {
                if (instances.result() != null) {
                  for (Object o : instances.result()) {
                    Instance instance = new Instance((JsonObject) o);
                    existingInstancesByMatchKey.put(instance.getMatchKey(), instance);
                    existingInstancesByUUID.put(instance.getUUID(), instance);
                  }
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing Instances by incoming MatchKeys "
                        + instances.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> requestInstanceWithOtherMatchKey(RoutingContext routingContext,
                                                        QueryShiftingMatchKey query) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupInstance(okapiClient, query)
            .onComplete(instance -> {
              if (instance.succeeded()) {
                if (instance.result() != null) {
                    Instance inst = new Instance(instance.result());
                    secondaryInstancesByLocalIdentifier.put(query.localIdentifier, inst);
                    existingInstancesByUUID.put(inst.getUUID(),inst);
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing Instances by incoming MatchKeys "
                        + instance.cause().getMessage());
              }
            });
    return promise.future();
  }

  private Future<Void> mapLocationsToInstitutionIds (RoutingContext routingContext) {
    Promise<Void> mapReady = Promise.promise();
    boolean missMappings = false;
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasIncomingRecordSet()) {
        for (HoldingsRecord holdings : pair.getIncomingRecordSet().getHoldingsRecords()) {
          if (!UpdatePlanSharedInventory.locationsToInstitutionsMap
                  .containsKey(holdings.getPermanentLocationId())) {
            missMappings = true;
            break;
          }
        }
      }
      if (pair.hasExistingRecordSet()) {
        for (HoldingsRecord holdings : pair.getIncomingRecordSet().getHoldingsRecords()) {
          if (!UpdatePlanSharedInventory.locationsToInstitutionsMap
                  .containsKey(holdings.getPermanentLocationId())) {
            missMappings = true;
            break;
          }
        }
      }
    }
    if (missMappings) {
      logger.debug("Miss mappings for at least one location, retrieving locations from Inventory storage");
      InventoryStorage.getLocations(InventoryStorage.getOkapiClient(routingContext)).onComplete(gotLocations -> {
        if (gotLocations.succeeded()) {
          JsonArray locationsJson = gotLocations.result();
          if (locationsJson == null || locationsJson.isEmpty()) {
            mapReady.fail(
                    new ErrorReport(
                            ErrorReport.ErrorCategory.STORAGE,
                            ErrorReport.INTERNAL_SERVER_ERROR,
                            "Retrieved a null or zero length array of locations from storage. Cannot map locations to institutions.")
                            .setEntityType(InventoryRecord.Entity.LOCATION)
                            .setTransaction(InventoryRecord.Transaction.GET.name())
                            .asJsonString());
          } else {
            Iterator<?> locationsIterator = locationsJson.iterator();
            while (locationsIterator.hasNext()) {
              JsonObject location = (JsonObject) locationsIterator.next();
              UpdatePlanSharedInventory.locationsToInstitutionsMap.put(location.getString("id"), location.getString("institutionId"));
            }
            logger.debug("Updated a map of " + UpdatePlanSharedInventory.locationsToInstitutionsMap.size() + " FOLIO locations to institutions.");
            mapReady.complete();
          }
        } else {
          mapReady.fail(gotLocations.cause().getMessage());
        }
      });
    } else {
      mapReady.complete();
    }
    return mapReady.future();
  }

  public List<Instance> getInstancesToUpdate() {
    List<Instance> instances = super.getInstancesToUpdate();
    for (Instance instance : secondaryInstancesByLocalIdentifier.values()) {
      if (instance.isUpdating()) {
        instances.add(instance);
      }
    }
    return instances;
  }

  public List<Item> getItemsToDelete () {
    List<Item> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasExistingRecordSet()) {
        for (Item item : pair.getExistingRecordSet().getItems()) {
          if (item.isDeleting()) {
            list.add(item);
          }
        }
      }
    }
    for (Instance instance : secondaryInstancesByLocalIdentifier.values()) {
      for (HoldingsRecord holdingsRecord : instance.getHoldingsRecords()) {
        for (Item item : holdingsRecord.getItems()) {
          if (item.isDeleting()) {
            list.add(item);
          }
        }
      }
    }
    return list;
  }

  public List<HoldingsRecord> getHoldingsToDelete () {
    List<HoldingsRecord> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasExistingRecordSet()) {
        for (HoldingsRecord holdingsRecord : pair.getExistingRecordSet().getHoldingsRecords()) {
          if (holdingsRecord.isDeleting()) {
            list.add(holdingsRecord);
          }
        }
      }
    }
    for (Instance instance : secondaryInstancesByLocalIdentifier.values()) {
      for (HoldingsRecord holdingsRecord : instance.getHoldingsRecords()) {
          if (holdingsRecord.isDeleting()) {
            list.add(holdingsRecord);
          }
      }
    }
    return list;
  }

}
