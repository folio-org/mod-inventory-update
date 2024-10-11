package org.folio.inventoryupdate.entities;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.InventoryStorage;
import org.folio.inventoryupdate.QueryByListOfIds;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.folio.inventoryupdate.entities.InventoryRecordSet.*;

public abstract class Repository {

  protected final Map<String,Instance> existingInstancesByUUID = new HashMap<>();
  protected final Map<String,Instance> existingInstancesByHrid = new HashMap<>();

  protected final Map<String,HoldingsRecord> existingHoldingsRecordsByUUID = new HashMap<>();

  public final Map<String, Map<String,HoldingsRecord>> existingHoldingsRecordsByInstanceId = new HashMap<>();

  public final Map<String,Item> existingItemsByHrid = new HashMap<>();
  public final Map<String,Map<String,Item>> existingItemsByHoldingsRecordId = new HashMap<>();

  public final Map<String,HoldingsRecord> existingHoldingsRecordsByHrid = new HashMap<>();

  // List of incoming record sets paired with existing record sets
  protected final List<PairedRecordSets> pairsOfRecordSets = new ArrayList<>();

  public void setIncomingRecordSets (JsonArray incomingInventoryRecordSets) {
    for (Object inventoryRecordSet : incomingInventoryRecordSets) {
      PairedRecordSets pair = new PairedRecordSets();
      InventoryRecordSet recordSet =
              InventoryRecordSet.makeIncomingRecordSet((JsonObject) inventoryRecordSet);
      pair.setIncomingRecordSet(recordSet);
      pairsOfRecordSets.add(pair);
    }
  }

  public abstract Future<Void> buildRepositoryFromStorage(RoutingContext routingContext);

  protected abstract void setExistingRecordSets();

  protected Future<Void> requestHoldingsRecordsByInstanceIds(RoutingContext routingContext,
                                                           List<String> instanceIds) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupHoldingsRecords(okapiClient,
                    new QueryByListOfIds("instanceId", instanceIds))
            .onComplete(records -> {
              if (records.succeeded()) {
                if (records.result() != null) {
                  stashExistingHoldingsRecords(records);
                }
                promise.complete();
              } else {
                promise.fail(records.cause().getMessage());
              }

            });
    return promise.future();
  }

  protected void stashExistingInstance(JsonObject instanceObject) {
    Instance instance = new Instance(instanceObject);
    existingInstancesByHrid.put(instance.getHRID(), instance);
    existingInstancesByUUID.put(instance.getUUID(), instance);
  }

  void stashExistingHoldingsRecords(AsyncResult<JsonArray> records) {
    stashExistingHoldingsRecords(records.result());
  }

  void stashExistingHoldingsRecords(JsonArray records) {
    for (Object o : records) {
      HoldingsRecord holdingsRecord = new HoldingsRecord((JsonObject) o);
      existingHoldingsRecordsByHrid.put(holdingsRecord.getHRID(), holdingsRecord);
      existingHoldingsRecordsByUUID.put(holdingsRecord.getUUID(), holdingsRecord);
      if (!existingHoldingsRecordsByInstanceId.containsKey(holdingsRecord.getInstanceId())) {
        existingHoldingsRecordsByInstanceId.put(holdingsRecord.getInstanceId(), new HashMap<>());
      }
      existingHoldingsRecordsByInstanceId.get(holdingsRecord.getInstanceId()).put(
              holdingsRecord.getUUID(), holdingsRecord);
    }
  }

  void stashExistingItems(AsyncResult<JsonArray> records) {
    stashExistingItems(records.result());
  }

  void stashExistingItems(JsonArray records) {
    for (Object o : records) {
      Item item = new Item((JsonObject) o);
      existingItemsByHrid.put(item.getHRID(), item);
      if (!existingItemsByHoldingsRecordId.containsKey(item.getHoldingsRecordId())) {
        existingItemsByHoldingsRecordId.put(item.getHoldingsRecordId(), new HashMap<>());
      }
      existingItemsByHoldingsRecordId.get(item.getHoldingsRecordId()).put(item.getUUID(),item);
    }
  }

  protected JsonObject assembleRecordSetJsonFromRepository(Instance existingInstance) {
    JsonObject existingRecordSetJson = new JsonObject();
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
    return existingRecordSetJson;
  }


  protected Future<Void> requestItemsByHoldingsRecordIds(RoutingContext routingContext,
                                                       List<String> holdingsRecordIds) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupItems(okapiClient,
                    new QueryByListOfIds("holdingsRecordId", holdingsRecordIds))
            .onComplete(records -> {
              if (records.succeeded()) {
                if (records.result() != null) {
                  stashExistingItems(records);
                }
                promise.complete();
              } else {
                promise.fail(records.cause().getMessage());
              }

            });
    return promise.future();
  }

  protected List<String> getExistingInstanceIds () {
    return new ArrayList<>(existingInstancesByUUID.keySet());
  }

  protected List<String> getExistingHoldingsRecordIds () {
    return new ArrayList<>(existingHoldingsRecordsByUUID.keySet());
  }

  public List<PairedRecordSets> getPairsOfRecordSets() {
    return pairsOfRecordSets;
  }

  public static List<List<String>> getSubLists (List<String> list, int lengthOfSubLists) {
    List<List<String>> subLists = new ArrayList<>();
    for (int i = 0; i < list.size(); i += lengthOfSubLists) {
      List<String> sub = list.subList(i, Math.min(list.size(),i+lengthOfSubLists));
      subLists.add(sub);
    }
    return subLists;
  }

  public static List<List<String>> getSubListsOfFifty (List<String> list) {
      return getSubLists(list, 50);
    }

  public static List<List<String>> getSubListsOfTen (List<String> list) {
      return getSubLists(list, 10);
    }

  public static List<List<String>> getSubListsOfFive (List<String> list) {
    return getSubLists(list, 5);
  }

  public List<Instance> getInstancesToCreate () {
    List<Instance> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasIncomingRecordSet()) {
        Instance instance = pair.getIncomingRecordSet().getInstance();
        if (instance.isCreating()) {
          list.add(instance);
        }
      }
    }
    return list;
  }

  public List<Instance> getInstancesToUpdate() {
    List<Instance> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasIncomingRecordSet()) {
        Instance instance = pair.getIncomingRecordSet().getInstance();
        if (instance.isUpdating()) {
          list.add(instance);
        }
      }
    }
    return list;
  }

  public List<HoldingsRecord> getHoldingsToCreate () {
    List<HoldingsRecord> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasIncomingRecordSet()) {
        for (HoldingsRecord holdingsRecord : pair.getIncomingRecordSet().getHoldingsRecords()) {
          if (holdingsRecord.isCreating()) {
            list.add(holdingsRecord);
          }
        }
      }
    }
    return list;
  }

  public List<HoldingsRecord> getHoldingsToUpdate () {
    List<HoldingsRecord> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasIncomingRecordSet()) {
        for (HoldingsRecord holdingsRecord : pair.getIncomingRecordSet().getHoldingsRecords()) {
          if (holdingsRecord.isUpdating()) {
            list.add(holdingsRecord);
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
          if (holdingsRecord.isDeleting() && !holdingsRecord.skipped()) {
            list.add(holdingsRecord);
          }
        }
      }
    }
    return list;
  }

  public List<HoldingsRecord> getDeletingHoldingsToSilentlyUpdate () {
    List<HoldingsRecord> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasExistingRecordSet()) {
        for (HoldingsRecord holdingsRecord : pair.getExistingRecordSet().getHoldingsRecords()) {
          if (holdingsRecord.isDeleting() && holdingsRecord.updateSilently) {
            list.add(holdingsRecord);
          }
        }
      }
    }
    return list;
  }


  public List<Item> getItemsToUpdate () {
    List<Item> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasIncomingRecordSet()) {
        for (Item item : pair.getIncomingRecordSet().getItems()) {
          if (item.isUpdating()) {
            list.add(item);
          }
        }
      }
    }
    return list;
  }

  public List<Item> getItemsToCreate () {
    List<Item> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasIncomingRecordSet()) {
        for (Item item : pair.getIncomingRecordSet().getItems()) {
          if (item.isCreating()) {
            list.add(item);
          }
        }
      }
    }
    return list;
  }

  public List<Item> getItemsToDelete () {
    List<Item> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasExistingRecordSet()) {
        for (Item item : pair.getExistingRecordSet().getItems()) {
          if (item.isDeleting() && !item.skipped()) {
            list.add(item);
          }
        }
      }
    }
    return list;
  }

  public List<Item> getDeletingItemsToSilentlyUpdate() {
    List<Item> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasExistingRecordSet()) {
        for (Item item : pair.getExistingRecordSet().getItems()) {
          if (item.isDeleting() && item.updateSilently) {
            list.add(item);
          }
        }
      }
    }
    return list;
  }

  public List<InstanceToInstanceRelation> getInstanceRelationsToCreate () {
    List<InstanceToInstanceRelation> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasIncomingRecordSet()) {
        for (InstanceToInstanceRelation relation : pair.getIncomingRecordSet().getInstanceToInstanceRelations()) {
          if (relation.isCreating()) {
              list.add(relation);
          }
        }
      }
    }
    return list;
  }

  public List<InstanceToInstanceRelation> getInstanceRelationsToDelete () {
    List<InstanceToInstanceRelation> list = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      if (pair.hasExistingRecordSet()) {
        for (InstanceToInstanceRelation relation : pair.getExistingRecordSet().getInstanceToInstanceRelations()) {
          if (relation.isDeleting()) {
            list.add(relation);
          }
        }
      }
    }
    return list;
  }

}
