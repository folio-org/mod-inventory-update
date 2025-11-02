package org.folio.inventoryupdate.updating.entities;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.updating.ErrorReport;
import org.folio.inventoryupdate.updating.InventoryStorage;
import org.folio.inventoryupdate.updating.QueryByListOfIds;
import org.folio.inventoryupdate.updating.QueryShiftingMatchKey;
import org.folio.inventoryupdate.updating.UpdatePlanSharedInventory;
import org.folio.inventoryupdate.updating.UpdateRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class RepositoryByMatchKey extends Repository {

  protected final Map<String,Instance> existingInstancesByMatchKey = new HashMap<>();
  public final Map<String,Instance> secondaryInstancesByLocalIdentifier = new HashMap<>();

  @Override
  public Future<Void> buildRepositoryFromStorage (UpdateRequest request) {
    Promise<Void> promise = Promise.promise();
    List<Future<Void>> existingRecordsByMatchKeyFutures = new ArrayList<>();
    for (List<String> idList : getSubListsOfFive(getIncomingMatchKeys())) {
      existingRecordsByMatchKeyFutures.add(requestInstancesByMatchKeys(request, idList));
    }
    for (PairedRecordSets pair : pairsOfRecordSets) {
      QueryShiftingMatchKey query = new QueryShiftingMatchKey(
              pair.getIncomingRecordSet().getLocalIdentifier(),
              pair.getIncomingRecordSet().getLocalIdentifierTypeId(),
              pair.getIncomingRecordSet().getInstance().getMatchKey());
      existingRecordsByMatchKeyFutures.add(requestInstanceWithOtherMatchKey(request,query));
    }
    Future.join(existingRecordsByMatchKeyFutures).onComplete(recordsByMatchKeys -> {
      if (recordsByMatchKeys.succeeded()) {
        List<Future<Void>> holdingsRecordsFutures = new ArrayList<>();
        for (List<String> idList : getSubListsOfFifty(getExistingInstanceIdsIncludingSecondaryInstances())) {
          holdingsRecordsFutures.add(requestHoldingsRecordsByInstanceIds(request, idList));
        }
        Future.join(holdingsRecordsFutures).onComplete(holdingsRecordsByInstanceIds -> {
          if (holdingsRecordsByInstanceIds.succeeded()) {
            List<Future<Void>> itemsFutures = new ArrayList<>();
            for (List<String> idList : getSubListsOfFifty(getExistingHoldingsRecordIds())) {
              itemsFutures.add(requestItemsByHoldingsRecordIds(request, idList));
            }
            Future.join(itemsFutures).onComplete(itemsByHoldingsRecordIds -> {
              if (itemsByHoldingsRecordIds.succeeded()) {
                setExistingRecordSets();
                mapLocationsToInstitutionIds(request).onComplete(locationMapUpdate -> {
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

  protected List<String> getExistingInstanceIdsIncludingSecondaryInstances () {
    List<String> instanceIds = new ArrayList<>(getExistingInstanceIds());
    for (Instance instance : secondaryInstancesByLocalIdentifier.values()) {
      instanceIds.add(instance.getUUID());
    }
    return instanceIds;
  }

  @Override
  protected void setExistingRecordSets () {
    for (PairedRecordSets pair : pairsOfRecordSets) {
      String incomingInstanceMatchKey = pair.getIncomingRecordSet().getInstance().getMatchKey();
      if (existingInstancesByMatchKey.containsKey(incomingInstanceMatchKey)) {
        Instance existingInstance = existingInstancesByMatchKey.get(incomingInstanceMatchKey);
        JsonObject existingRecordSetJson = assembleRecordSetJsonFromRepository(existingInstance);
        InventoryRecordSet existingSet = InventoryRecordSet.makeExistingRecordSet(existingRecordSetJson);
        pair.setExistingRecordSet(existingSet);
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

  private Future<Void> requestInstancesByMatchKeys(UpdateRequest request,
                                                   List<String> matchKeys) {
    Promise<Void> promise = Promise.promise();
    InventoryStorage.lookupInstances(request.getOkapiClient(),
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
                promise.fail(instances.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> requestInstanceWithOtherMatchKey(UpdateRequest request,
                                                        QueryShiftingMatchKey query) {
    Promise<Void> promise = Promise.promise();
    InventoryStorage.lookupInstance(request.getOkapiClient(), query)
            .onComplete(instance -> {
              if (instance.succeeded()) {
                if (instance.result() != null) {
                    Instance inst = new Instance(instance.result());
                    secondaryInstancesByLocalIdentifier.put(query.localIdentifier(), inst);
                    existingInstancesByUUID.put(inst.getUUID(),inst);
                }
                promise.complete();
              } else {
                promise.fail(instance.cause().getMessage());
              }
            });
    return promise.future();
  }

  private Future<Void> mapLocationsToInstitutionIds (UpdateRequest request) {
    Promise<Void> mapReady = Promise.promise();
    boolean missMappings = false;
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasIncomingRecordSet()) {
        for (HoldingsRecord holdings : pair.getIncomingRecordSet().getHoldingsRecords()) {
          if (!UpdatePlanSharedInventory.locationsToInstitutionsMap()
                  .containsKey(holdings.getPermanentLocationId())) {
            missMappings = true;
            break;
          }
        }
      }
      if (pair.hasExistingRecordSet()) {
        for (HoldingsRecord holdings : pair.getIncomingRecordSet().getHoldingsRecords()) {
          if (!UpdatePlanSharedInventory.locationsToInstitutionsMap()
                  .containsKey(holdings.getPermanentLocationId())) {
            missMappings = true;
            break;
          }
        }
      }
    }
    if (missMappings) {
      InventoryStorage.getLocations(request.getOkapiClient()).onComplete(gotLocations -> {
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
              UpdatePlanSharedInventory.locationsToInstitutionsMap().put(location.getString("id"), location.getString("institutionId"));
            }
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

  @Override
  public List<Instance> getInstancesToUpdate() {
    List<Instance> instances = super.getInstancesToUpdate();
    for (Instance instance : secondaryInstancesByLocalIdentifier.values()) {
      if (instance.isUpdating()) {
        instances.add(instance);
      }
    }
    return instances;
  }

  @Override
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

  @Override
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
