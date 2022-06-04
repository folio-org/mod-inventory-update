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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.folio.inventoryupdate.entities.InstanceRelations.*;
import static org.folio.inventoryupdate.entities.InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.*;


public class Repository {


  private final Map<String,Instance> existingInstancesByHrid = new HashMap<>();
  private final Map<String,Instance> existingInstancesByUUID = new HashMap<>();

  private final Map<String,Map<String,InstanceToInstanceRelation>> existingParentRelationsByChildId = new HashMap<>();
  private final Map<String,Map<String,InstanceToInstanceRelation>> existingChildRelationsByParentId = new HashMap<>();
  private final Map<String,Map<String,InstanceToInstanceRelation>> existingSucceedingRelationsByPrecedingId = new HashMap<>();
  private final Map<String,Map<String,InstanceToInstanceRelation>> existingPrecedingRelationsBySucceedingId = new HashMap<>();
  public final Map<String,Instance> referencedInstancesByHrid = new HashMap<>();
  private final Map<String,Instance> referencedInstancesByUUID = new HashMap<>();
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
      JsonObject recordSetJson = (JsonObject) inventoryRecordSet;
      if (recordSetJson.containsKey(INSTANCE_RELATIONS)
              && !((JsonObject) inventoryRecordSet).getJsonObject(INSTANCE_RELATIONS).isEmpty()) {
        logger.info("Incoming relations: " + ((JsonObject) inventoryRecordSet).getJsonObject(INSTANCE_RELATIONS).encodePrettily());
      }
      PairedRecordSets pair = new PairedRecordSets();
      InventoryRecordSet recordSet = new InventoryRecordSet((JsonObject) inventoryRecordSet);
      pair.setIncomingRecordSet(recordSet);
      pairsOfRecordSets.add(pair);
    }
    return this;
  }

  public Future<Void> buildRepositoryFromStorage (RoutingContext routingContext) {
    Promise<Void> promise = Promise.promise();
    List<Future> existingRecordsByHridsFutures = new ArrayList<>();
    for (List<String> idList : getSubListsOfFifty(getIncomingInstanceHRIDs())) {
      existingRecordsByHridsFutures.add(requestInstancesByHRIDs(routingContext, idList));
    }
    for (List<String> idList : getSubListsOfFifty(getIncomingHoldingsRecordHRIDs())) {
      existingRecordsByHridsFutures.add(requestHoldingsRecordsByHRIDs(routingContext, idList));
    }
    for (List<String> idList : getSubListsOfFifty(getIncomingItemHRIDs())) {
      existingRecordsByHridsFutures.add(requestItemsByHRIDs(routingContext, idList));
    }
    for (List<String> idList : getSubListsOfFifty(getIncomingReferencedInstanceHrids())) {
      existingRecordsByHridsFutures.add(requestReferencedInstancesByHRIDs(routingContext, idList));
    }
    CompositeFuture.join(existingRecordsByHridsFutures).onComplete(recordsByHrids -> {
      if (recordsByHrids.succeeded()) {
        List<Future> instanceRelationsFutures = new ArrayList<>();
        for (List<String> idList : getSubListsOfFifty(getExistingInstanceIds())) {
          instanceRelationsFutures.add(requestParentRelationsByChildInstanceIds(routingContext, idList));
        }
        for (List<String> idList : getSubListsOfFifty(getExistingInstanceIds())) {
          instanceRelationsFutures.add(requestChildRelationsByParentInstanceIds(routingContext, idList));
        }
        for (List<String> idList : getSubListsOfFifty(getExistingInstanceIds())) {
          instanceRelationsFutures.add(requestSucceedingByPrecedingIds(routingContext, idList));
        }
        for (List<String> idList : getSubListsOfFifty(getExistingInstanceIds())) {
          instanceRelationsFutures.add(requestPrecedingBySucceedingIds(routingContext, idList));
        }
        CompositeFuture.join(instanceRelationsFutures).onComplete(instanceRelations -> {
          List<Future> holdingsRecordsFutures = new ArrayList<>();
          for (List<String> idList : getSubListsOfFifty(getExistingInstanceIds())) {
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
                  promise.complete();
                } else {
                  promise.fail("There was an error fetching items by holdings record IDs: " + itemsByHoldingsRecordIds.cause().getMessage());
                }
              });
            } else {
              promise.fail("There was an error fetching holdings by instance IDs: " + recordsByHrids.cause().getMessage());
            }
          });
        });
      } else {
        promise.fail("There was an fetching inventory records by incoming HRIDss: " + recordsByHrids.cause().getMessage());
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
              jsonRecord.put(ITEMS, new JsonArray());
              for (Item item : existingItemsByHoldingsRecordId.get(holdingsRecord.getUUID()).values()) {
                jsonRecord.getJsonArray(ITEMS).add(item.asJson());
              }
            }
            holdingsWithItems.add(jsonRecord);
          }
          existingRecordSet.put(HOLDINGS_RECORDS, holdingsWithItems);
          InventoryRecordSet existingSet = new InventoryRecordSet(existingRecordSet);
          if (!existingParentRelationsByChildId.isEmpty() && existingParentRelationsByChildId.get(existingInstance.getUUID()) != null) {
            existingSet.parentRelations = new ArrayList<>(existingParentRelationsByChildId.get(existingInstance.getUUID()).values());
          }
          if (!existingChildRelationsByParentId.isEmpty() && existingChildRelationsByParentId.get(existingInstance.getUUID()) != null) {
            existingSet.childRelations = new ArrayList<>(existingChildRelationsByParentId.get(existingInstance.getUUID()).values());
          }
          if (!existingPrecedingRelationsBySucceedingId.isEmpty() && existingPrecedingRelationsBySucceedingId.get(existingInstance.getUUID()) != null) {
            existingSet.precedingTitles = new ArrayList<>(existingPrecedingRelationsBySucceedingId.get(existingInstance.getUUID()).values());
          }
          if (!existingSucceedingRelationsByPrecedingId.isEmpty() && existingSucceedingRelationsByPrecedingId.get(existingInstance.getUUID()) != null) {
            existingSet.succeedingTitles = new ArrayList<>(existingSucceedingRelationsByPrecedingId.get(existingInstance.getUUID()).values());
          }
          pair.setExistingRecordSet(existingSet);
        }
      }
    }
  }

  private Future<Void> requestInstancesByHRIDs(RoutingContext routingContext,
                                               List<String> hrids) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupInstances(okapiClient,
                    new QueryByListOfIds("hrid", hrids))
            .onComplete(instances -> {
              if (instances.succeeded()) {
                if (instances.result() != null) {
                  for (Object o : instances.result()) {
                    Instance instance = new Instance((JsonObject) o);
                    existingInstancesByHrid.put(instance.getHRID(), instance);
                    existingInstancesByUUID.put(instance.getUUID(), instance);
                  }
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing Instances by incoming HRIDs "
                        + instances.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> requestReferencedInstancesByHRIDs(RoutingContext routingContext,
                                                         List<String> hrids) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupInstances(okapiClient,
                    new QueryByListOfIds("hrid", hrids))
            .onComplete(instances -> {
              if (instances.succeeded()) {
                if (instances.result() != null) {
                  for (Object o : instances.result()) {
                    Instance instance = new Instance((JsonObject) o);
                    logger.info("Repository: Adding referenced instance " + instance.asJsonString());
                    referencedInstancesByHrid.put(instance.getHRID(), instance);
                    referencedInstancesByUUID.put(instance.getUUID(), instance);
                    logger.info("Referenced instance added to maps by hrid and uuid");
                  }
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing Instances by incoming HRIDs "
                        + instances.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> requestHoldingsRecordsByHRIDs(RoutingContext routingContext,
                                                     List<String> hrids) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupHoldingsRecords(okapiClient,
                    new QueryByListOfIds("hrid", hrids))
            .onComplete(records -> {
              if (records.succeeded()) {
                if (records.result() != null) {
                  for (Object o : records.result()) {
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
                promise.complete();
              } else {
                promise.fail(
                        "There was a problem fetching existing holdings records by incoming HRIDs "
                                + records.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> requestHoldingsRecordsByInstanceIds(RoutingContext routingContext,
                                                           List<String> instanceIds) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupHoldingsRecords(okapiClient,
                    new QueryByListOfIds("instanceId", instanceIds))
            .onComplete(records -> {
              if (records.succeeded()) {
                if (records.result() != null) {
                  for (Object o : records.result()) {
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
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing holdings records by instance IDs " + records.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> requestItemsByHRIDs(RoutingContext routingContext, List<String> hrids) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupItems(okapiClient,
                    new QueryByListOfIds("hrid", hrids))
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

  private Future<Void> requestItemsByHoldingsRecordIds(RoutingContext routingContext,
                                                       List<String> holdingsRecordIds) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupItems(okapiClient,
                    new QueryByListOfIds("holdingsRecordId", holdingsRecordIds))
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
                promise.fail("There was a problem fetching existing items by holdings record IDs "
                        + records.cause().getMessage());
              }

            });
    return promise.future();
  }


  private Future<Void> requestParentRelationsByChildInstanceIds(RoutingContext routingContext,
                                                                List<String> childInstanceIds) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupParentChildRelationships(okapiClient,
                    new QueryByListOfIds("subInstanceId", childInstanceIds))
            .onComplete(records -> {
              if (records.succeeded()) {
                if (records.result() != null) {
                  for (Object o : records.result()) {
                    JsonObject relation = (JsonObject) o;
                    InstanceRelationship relationship =
                            InstanceRelationship.makeRelationshipFromJsonRecord(
                                    relation.getString("subInstanceId"),
                                    relation);
                    if (!existingParentRelationsByChildId.containsKey(relationship.getSubInstanceId())) {
                       existingParentRelationsByChildId.put(relationship.getSubInstanceId(), new HashMap<>());
                    }
                    existingParentRelationsByChildId
                            .get(relationship.getSubInstanceId())
                            .put(relationship.getSuperInstanceId(), relationship);
                  }
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing relationships by child instance IDs "
                        + records.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> requestChildRelationsByParentInstanceIds(RoutingContext routingContext,
                                                                List<String> parentInstanceIds) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupParentChildRelationships(okapiClient,
                    new QueryByListOfIds("superInstanceId", parentInstanceIds))
            .onComplete(records -> {
              if (records.succeeded()) {
                if (records.result() != null) {
                  for (Object o : records.result()) {
                    JsonObject relation = (JsonObject) o;
                    InstanceRelationship relationship =
                            InstanceRelationship.makeRelationshipFromJsonRecord(
                                    relation.getString("superInstanceId"),
                                    relation);
                    if (!existingChildRelationsByParentId.containsKey(relationship.getSuperInstanceId())) {
                      existingChildRelationsByParentId.put(relationship.getSuperInstanceId(), new HashMap<>());
                    }
                    existingChildRelationsByParentId
                            .get(relationship.getSuperInstanceId())
                            .put(relationship.getSubInstanceId(), relationship);
                  }
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing relationships by parent instance IDs "
                        + records.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> requestSucceedingByPrecedingIds(RoutingContext routingContext,
                                                       List<String> precedingIds) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupTitleSuccessions(okapiClient,
                    new QueryByListOfIds("precedingInstanceId", precedingIds))
            .onComplete(records -> {
              if (records.succeeded()) {
                if (records.result() != null) {
                  for (Object o : records.result()) {
                    JsonObject relation = (JsonObject) o;
                    InstanceTitleSuccession relationship =
                            InstanceTitleSuccession.makeInstanceTitleSuccessionFromJsonRecord(
                                    relation.getString("precedingInstanceId"),
                                    relation);
                    if (!existingSucceedingRelationsByPrecedingId.containsKey(relationship.getPrecedingInstanceId())) {
                      existingSucceedingRelationsByPrecedingId.put(relationship.getPrecedingInstanceId(), new HashMap<>());
                    }
                    existingSucceedingRelationsByPrecedingId
                            .get(relationship.getPrecedingInstanceId())
                            .put(relationship.getSucceedingInstanceId(), relationship);
                  }
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing title successions by preceding instance IDs "
                        + records.cause().getMessage());
              }

            });
    return promise.future();
  }

  private Future<Void> requestPrecedingBySucceedingIds(RoutingContext routingContext,
                                                       List<String> succeedingIds) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupTitleSuccessions(okapiClient,
                    new QueryByListOfIds("succeedingInstanceId", succeedingIds))
            .onComplete(records -> {
              if (records.succeeded()) {
                if (records.result() != null) {
                  for (Object o : records.result()) {
                    JsonObject relation = (JsonObject) o;
                    InstanceTitleSuccession relationship =
                            InstanceTitleSuccession.makeInstanceTitleSuccessionFromJsonRecord(
                                    relation.getString("succeedingInstanceId"),
                                    relation);
                    if (!existingPrecedingRelationsBySucceedingId.containsKey(relationship.getSucceedingInstanceId())) {
                      existingPrecedingRelationsBySucceedingId.put(relationship.getSucceedingInstanceId(), new HashMap<>());
                    }
                    existingPrecedingRelationsBySucceedingId
                            .get(relationship.getSucceedingInstanceId())
                            .put(relationship.getPrecedingInstanceId(), relationship);
                  }
                }
                promise.complete();
              } else {
                promise.fail("There was a problem fetching existing title successions by succeeding instance IDs "
                        + records.cause().getMessage());
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

  private List<String> getIncomingReferencedInstanceHrids () {
    List<String> hrids = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      JsonObject instanceRelations = pair.getIncomingRecordSet().instanceRelationsJson;
      for (JsonArray array :
              Arrays.asList(
                      instanceRelations.getJsonArray(PARENT_INSTANCES),
                      instanceRelations.getJsonArray(CHILD_INSTANCES),
                      instanceRelations.getJsonArray(SUCCEEDING_TITLES),
                      instanceRelations.getJsonArray(PRECEDING_TITLES))) {
        if (array != null && ! array.isEmpty()) {
          for (Object o : array) {
            JsonObject rel = (JsonObject) o;
            JsonObject instanceIdentifier = rel.getJsonObject( INSTANCE_IDENTIFIER );
            if (instanceIdentifier != null && instanceIdentifier.containsKey( HRID_IDENTIFIER_KEY )) {
              hrids.add(instanceIdentifier.getString(HRID_IDENTIFIER_KEY));
            }
          }
        }
      }
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


}
