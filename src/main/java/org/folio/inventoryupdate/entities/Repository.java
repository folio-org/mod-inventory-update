package org.folio.inventoryupdate.entities;

import io.vertx.core.CompositeFuture;
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

import static org.folio.inventoryupdate.entities.InventoryRecordSet.INSTANCE;


public class Repository {


  private final Map<String,Instance> existingInstancesByHrid = new HashMap<>();
  private final Map<String,Instance> existingInstancesByUUID = new HashMap<>();

  private final Map<String,HoldingsRecord> existingHoldingsRecordsByHrid = new HashMap<>();
  private final Map<String,HoldingsRecord> existingHoldingsRecordsByUUID = new HashMap<>();

  private final Map<String, Map<String,HoldingsRecord>> existingHoldingsRecordsByInstanceId = new HashMap<>();

  private final Map<String,Item> existingItemsByHrid = new HashMap<>();
  private final Map<String,Item> existingItemsByUUID = new HashMap<>();
  private final Map<String,Map<String,Item>> existingItemsByHoldingsRecordId = new HashMap<>();

  // List of incoming record sets paired with existing record sets
  private final List<PairedRecordSets> pairsOfRecordSets = new ArrayList<>();

  public Repository setIncomingRecordSets (JsonArray incomingInventoryRecordSets) {
    for (Object inventoryRecordSet : incomingInventoryRecordSets) {
      PairedRecordSets pair = new PairedRecordSets();
      InventoryRecordSet recordSet = new InventoryRecordSet((JsonObject) inventoryRecordSet);
      pair.setIncomingRecordSet(recordSet);
      pairsOfRecordSets.add(pair);
    }
    return this;
  }

  public Future<Void> buildRepositoryFromStorage (RoutingContext routingContext) {
    Promise<Void> promise = Promise.promise();
    List<Future> existingRecordsFutures = new ArrayList<>();
    existingRecordsFutures.add(fetchExistingInstancesByIncomingHRIDs(routingContext));
    existingRecordsFutures.add(fetchExistingHoldingsRecordsByIncomingHRIDs(routingContext));
    existingRecordsFutures.add(fetchExistingItemsByIncomingHRIDs(routingContext));
    CompositeFuture.join(existingRecordsFutures).onComplete (allItemsDone -> {
      if (allItemsDone.succeeded()) {
        fetchExistingHoldingsRecordsByInstanceIds(routingContext).onComplete(void1 -> {
          if (void1.succeeded()) {
            fetchExistingItemsByHoldingsRecordIds(routingContext).onComplete(void2 -> {
              if (void2.succeeded()) {
                setExistingRecordSets();
                promise.complete();
              } else {
                promise.fail("There was an error fetching items by holdings record IDs: " + void2.cause().getMessage());
              }
            });
          } else {
            promise.fail("There was an error fetching holdings by instance IDs: " + void1.cause().getMessage());
          }
        });
      } else {
        promise.fail("There was an fetching inventory records by incoming HRIDss: " + allItemsDone.cause().getMessage());
      }
    });
    return promise.future();
  }

  private void setExistingRecordSets () {
    for (PairedRecordSets pair : pairsOfRecordSets) {
      String incomingInstanceHrid = pair.getIncomingRecordSet().getInstanceHRID();
      if (existingInstancesByHrid.containsKey(incomingInstanceHrid)) {
        JsonObject existingRecordSet = new JsonObject();
        Instance existingInstance = existingInstancesByHrid.get(incomingInstanceHrid);
        existingRecordSet.put(INSTANCE,existingInstance.asJson());
        if (existingHoldingsRecordsByInstanceId.containsKey(existingInstance.getUUID())) {
          JsonArray holdingsWithItems = new JsonArray();
          for (HoldingsRecord holdingsRecord : existingHoldingsRecordsByInstanceId.get(
                  existingInstance.getUUID()).values()) {
            JsonObject jsonRecord = holdingsRecord.asJson();
            if (existingItemsByHoldingsRecordId.containsKey(holdingsRecord.getUUID())) {
              jsonRecord.put("items", new JsonArray());
              for (Item item : existingItemsByHoldingsRecordId.get(holdingsRecord.getUUID()).values()) {
                jsonRecord.getJsonArray("items").add(item.asJson());
              }
            }
            holdingsWithItems.add(jsonRecord);
          }
          existingRecordSet.put("holdingsRecords", holdingsWithItems);
        }
        pair.setExistingRecordSet(new InventoryRecordSet(existingRecordSet));
      }
    }
  }

  private Future<Void> fetchExistingInstancesByIncomingHRIDs(RoutingContext routingContext) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupInstances(okapiClient,
                    new QueryByListOfIds("hrid", getIncomingInstanceHRIDs()))
            .onComplete(instances -> {
              if (instances.succeeded()) {
                for (Object o : instances.result()) {
                  Instance instance = new Instance((JsonObject) o);
                  existingInstancesByHrid.put(instance.getHRID(), instance);
                  existingInstancesByUUID.put(instance.getUUID(), instance);
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing Instances by incoming HRIDs " + instances.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> fetchExistingHoldingsRecordsByIncomingHRIDs(RoutingContext routingContext) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupHoldingsRecords(okapiClient,
                    new QueryByListOfIds("hrid", getIncomingHoldingsRecordHRIDs()))
            .onComplete(records -> {
              if (records.succeeded()) {
                for (Object o : records.result()) {
                  HoldingsRecord holdingsRecord = new HoldingsRecord((JsonObject) o);
                  existingHoldingsRecordsByHrid.put(holdingsRecord.getHRID(), holdingsRecord);
                  existingHoldingsRecordsByUUID.put(holdingsRecord.getUUID(), holdingsRecord);
                  if (!existingHoldingsRecordsByInstanceId.containsKey(holdingsRecord.getInstanceId())) {
                    existingHoldingsRecordsByInstanceId.put(holdingsRecord.getInstanceId(), new HashMap<>());
                  }
                  existingHoldingsRecordsByInstanceId
                          .get(holdingsRecord.getInstanceId())
                          .put(holdingsRecord.getUUID(), holdingsRecord);
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing holdings records by incoming HRIDs " + records.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> fetchExistingHoldingsRecordsByInstanceIds(RoutingContext routingContext) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupHoldingsRecords(okapiClient,
                    new QueryByListOfIds("instanceId", getExistingInstanceIds()))
            .onComplete(records -> {
              if (records.succeeded()) {
                for (Object o : records.result()) {
                  HoldingsRecord holdingsRecord = new HoldingsRecord((JsonObject) o);
                  existingHoldingsRecordsByHrid.put(holdingsRecord.getHRID(), holdingsRecord);
                  existingHoldingsRecordsByUUID.put(holdingsRecord.getUUID(), holdingsRecord);
                  if (!existingHoldingsRecordsByInstanceId.containsKey(holdingsRecord.getInstanceId())) {
                    existingHoldingsRecordsByInstanceId.put(holdingsRecord.getInstanceId(), new HashMap<>());
                  }
                  existingHoldingsRecordsByInstanceId
                          .get(holdingsRecord.getInstanceId())
                          .put(holdingsRecord.getUUID(), holdingsRecord);
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing holdings records by instance IDs " + records.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> fetchExistingItemsByIncomingHRIDs(RoutingContext routingContext) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupItems(okapiClient,
                    new QueryByListOfIds("hrid", getIncomingItemHRIDs()))
            .onComplete(records -> {
              if (records.succeeded()) {
                if (records.result() != null) {
                  for (Object o : records.result()) {
                    Item item = new Item((JsonObject) o);
                    existingItemsByHrid.put(item.getHRID(), item);
                    existingItemsByUUID.put(item.getUUID(), item);
                    if (!existingItemsByHoldingsRecordId.containsKey(item.getHoldingsRecordId())) {
                      existingItemsByHoldingsRecordId.put(item.getHoldingsRecordId(), new HashMap<>());
                    }
                    existingItemsByHoldingsRecordId.get(item.getHoldingsRecordId()).put(item.getUUID(),item);
                  }
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing items by incoming HRIDs " + records.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> fetchExistingItemsByHoldingsRecordIds(RoutingContext routingContext) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupItems(okapiClient,
                    new QueryByListOfIds("holdingsRecordId", getExistingHoldingsRecordIds()))
            .onComplete(records -> {
              if (records.succeeded()) {
                if (records.result() != null) {
                  for (Object o : records.result()) {
                    Item item = new Item((JsonObject) o);
                    existingItemsByHrid.put(item.getHRID(), item);
                    existingItemsByUUID.put(item.getUUID(), item);
                    if (!existingItemsByHoldingsRecordId.containsKey(item.getHoldingsRecordId())) {
                      existingItemsByHoldingsRecordId.put(item.getHoldingsRecordId(), new HashMap<>());
                    }
                    existingItemsByHoldingsRecordId
                            .get(item.getHoldingsRecordId())
                            .put(item.getUUID(), item);
                  }
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing items by holdings record IDs " + records.cause().getMessage());
              }

            });
    return promise.future();
  }

  private List<String> getIncomingInstanceHRIDs () {
    List<String> hrids = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      hrids.add(pair.getIncomingRecordSet().getInstanceHRID());
    }
    return hrids;
  }

  private List<String> getIncomingHoldingsRecordHRIDs () {
    List<String> hrids = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      List<HoldingsRecord> holdingsRecords = pair.getIncomingRecordSet().getHoldingsRecords();
      for (HoldingsRecord record : holdingsRecords) {
        hrids.add(record.getHRID());
      }
    }
    return hrids;
  }

  private List<String> getIncomingItemHRIDs () {
    List<String> hrids = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      List<Item> items = pair.getIncomingRecordSet().getItems();
      for (Item record : items) {
        hrids.add(record.getHRID());
      }
    }
    return hrids;
  }

  public List<InventoryRecordSet> getExistingRecordSets () {
    List<InventoryRecordSet> existingSets = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      existingSets.add(pair.getExistingRecordSet());
    }
    return existingSets;
  }

  public List<InventoryRecordSet> getIncomingRecordSets() {
    List<InventoryRecordSet> incomingSets = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      incomingSets.add(pair.getIncomingRecordSet());
    }
    return incomingSets;
  }

  private List<String> getExistingInstanceIds () {
    return new ArrayList<>(existingInstancesByUUID.keySet());
  }

  private List<String> getExistingHoldingsRecordIds () {
    return new ArrayList<>(existingHoldingsRecordsByUUID.keySet());
  }

  public List<PairedRecordSets> getPairsOfRecordSets() {
    return pairsOfRecordSets;
  }

}
