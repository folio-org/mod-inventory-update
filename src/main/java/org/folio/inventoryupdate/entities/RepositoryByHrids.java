package org.folio.inventoryupdate.entities;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.ErrorReport;
import org.folio.inventoryupdate.InventoryStorage;
import org.folio.inventoryupdate.QueryByListOfIds;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.folio.inventoryupdate.entities.InventoryRecordSet.*;

public class RepositoryByHrids extends Repository {

  protected final Map<String,Map<String,InstanceToInstanceRelation>> existingParentRelationsByChildId = new HashMap<>();
  protected final Map<String,Map<String,InstanceToInstanceRelation>> existingChildRelationsByParentId = new HashMap<>();
  protected final Map<String,Map<String,InstanceToInstanceRelation>> existingSucceedingRelationsByPrecedingId = new HashMap<>();
  protected final Map<String,Map<String,InstanceToInstanceRelation>> existingPrecedingRelationsBySucceedingId = new HashMap<>();
  public final Map<String,Instance> referencedInstancesByHrid = new HashMap<>();
  public final Map<String,Instance> referencedInstancesByUUID = new HashMap<>();

  public Future<Void> buildRepositoryFromStorage (RoutingContext routingContext) {
    List<Future<Void>> existingRecordsByHridsFutures = new ArrayList<>();
    for (List<String> idList : getSubListsOfTen(getIncomingInstanceHRIDs())) {
      existingRecordsByHridsFutures.add(requestInstanceSetsByHRIDs(routingContext, idList));
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
    for (List<String> idList : getSubListsOfFifty(getIncomingReferencedInstanceIds())) {
      existingRecordsByHridsFutures.add(requestReferencedInstancesByUUIDs(routingContext, idList));
    }
    return GenericCompositeFuture.join(existingRecordsByHridsFutures)
        .onSuccess(x -> setExistingRecordSets())
        .mapEmpty();
  }

  protected void setExistingRecordSets () {
    for (PairedRecordSets pair : pairsOfRecordSets) {
      String incomingInstanceHrid = pair.getIncomingRecordSet().getInstanceHRID();
      if (existingInstancesByHrid.containsKey(incomingInstanceHrid)) {
        Instance existingInstance = existingInstancesByHrid.get(incomingInstanceHrid);
        JsonObject existingRecordSetJson = assembleRecordSetJsonFromRepository(existingInstance);
        InventoryRecordSet existingSet = InventoryRecordSet.makeExistingRecordSet(existingRecordSetJson);
        if (existingParentRelationsByChildId.get(existingInstance.getUUID()) != null) {
          existingSet.parentRelations = new ArrayList<>(existingParentRelationsByChildId.get(existingInstance.getUUID()).values());
        }
        if (existingChildRelationsByParentId.get(existingInstance.getUUID()) != null) {
          existingSet.childRelations = new ArrayList<>(existingChildRelationsByParentId.get(existingInstance.getUUID()).values());
        }
        if (existingPrecedingRelationsBySucceedingId.get(existingInstance.getUUID()) != null) {
          existingSet.precedingTitles = new ArrayList<>(existingPrecedingRelationsBySucceedingId.get(existingInstance.getUUID()).values());
        }
        if (existingSucceedingRelationsByPrecedingId.get(existingInstance.getUUID()) != null) {
          existingSet.succeedingTitles = new ArrayList<>(existingSucceedingRelationsByPrecedingId.get(existingInstance.getUUID()).values());
        }
        pair.setExistingRecordSet(existingSet);

      }
    }
  }


  private Future<Void> requestInstanceSetsByHRIDs(RoutingContext routingContext,
                                               List<String> hrids) {
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    return InventoryStorage.lookupInstanceSets(okapiClient, new QueryByListOfIds("hrid", hrids))
        .onSuccess(this::stashInstanceSets)
        .mapEmpty();
  }

  private void stashInstanceSets(JsonArray instanceSets) {
    if (instanceSets == null) {
      return;
    }
    instanceSets.forEach(o -> stashInstanceSet((JsonObject) o));
  }

  private void stashInstanceSet(JsonObject instanceSet) {
    stashExistingInstance(instanceSet.getJsonObject("instance"));
    stashExistingHoldingsRecords(instanceSet.getJsonArray("holdingsRecords"));
    stashExistingItems(instanceSet.getJsonArray("items"));
    instanceSet.getJsonArray("superInstanceRelationships")
    .forEach(o -> stashRelationByChildId((JsonObject) o));
    instanceSet.getJsonArray("subInstanceRelationships")
    .forEach(o -> stashRelationByParentId((JsonObject) o));
    instanceSet.getJsonArray("succeedingTitles")
    .forEach(o -> stashRelationByPrecedingId((JsonObject) o));
    instanceSet.getJsonArray("precedingTitles")
    .forEach(o -> stashRelationBySucceedingId((JsonObject) o));
  }

  private void stashRelationByChildId(JsonObject instanceRelationshipObject) {
    var subInstanceId = instanceRelationshipObject.getString("subInstanceId");
    InstanceRelationship relationship = InstanceRelationship.makeRelationshipFromJsonRecord(
            subInstanceId, instanceRelationshipObject);
    existingParentRelationsByChildId.computeIfAbsent(subInstanceId, k -> new HashMap<>())
    .put(relationship.getSuperInstanceId(), relationship);
  }

  private void stashRelationByParentId(JsonObject instanceRelationshipObject) {
    var superInstanceId = instanceRelationshipObject.getString("superInstanceId");
    InstanceRelationship relationship = InstanceRelationship.makeRelationshipFromJsonRecord(
            superInstanceId, instanceRelationshipObject);
    existingChildRelationsByParentId.computeIfAbsent(superInstanceId, k -> new HashMap<>())
    .put(relationship.getSubInstanceId(), relationship);
  }

  private void stashRelationByPrecedingId(JsonObject succeedingRelationshipObject) {
    var precedingInstanceId = succeedingRelationshipObject.getString("precedingInstanceId");
    InstanceTitleSuccession relationship = InstanceTitleSuccession.makeInstanceTitleSuccessionFromJsonRecord(
            precedingInstanceId, succeedingRelationshipObject);
    existingSucceedingRelationsByPrecedingId.computeIfAbsent(precedingInstanceId, k -> new HashMap<>())
    .put(relationship.getSucceedingInstanceId(), relationship);
  }

  private void stashRelationBySucceedingId(JsonObject precedingRelationshipObject) {
    var succeecedingInstanceId = precedingRelationshipObject.getString("succeedingInstanceId");
    InstanceTitleSuccession relationship = InstanceTitleSuccession.makeInstanceTitleSuccessionFromJsonRecord(
            succeecedingInstanceId, precedingRelationshipObject);
    existingPrecedingRelationsBySucceedingId.computeIfAbsent(succeecedingInstanceId, k -> new HashMap<>())
    .put(relationship.getPrecedingInstanceId(), relationship);
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
                  stashReferencedInstances(instances);
                }
                promise.complete();
              } else {
                promise.fail(instances.cause().getMessage());
              }

            });
    return promise.future();
  }

  private void stashReferencedInstances(AsyncResult<JsonArray> instances) {
    for (Object o : instances.result()) {
      Instance instance = new Instance((JsonObject) o);
      referencedInstancesByHrid.put(instance.getHRID(), instance);
      referencedInstancesByUUID.put(instance.getUUID(), instance);
    }
  }

  private Future<Void> requestReferencedInstancesByUUIDs(RoutingContext routingContext,
                                                         List<String> uuids) {
    Promise<Void> promise = Promise.promise();
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
    InventoryStorage.lookupInstances(okapiClient,
                    new QueryByListOfIds("id", uuids))
            .onComplete(instances -> {
              if (instances.succeeded()) {
                if (instances.result() != null) {
                  stashReferencedInstances(instances);
                }
                promise.complete();
              } else {
                promise.fail(instances.cause().getMessage());
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
                  stashExistingHoldingsRecords(records);
                }
                promise.complete();
              } else {
                promise.fail(ErrorReport.makeErrorReportFromJsonString(
                        records.cause().getMessage())
                        .setShortMessage("Problem fetching holdings records by HRIDs before upsert")
                        .addDetail("context", "fetching holdings records by HRIDs before upsert")
                        .asJsonString());
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
                  stashExistingItems(records);
                }
                promise.complete();
              } else {
                promise.fail(records.cause().getMessage());
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
                      instanceRelations.getJsonArray(InstanceReferences.PARENT_INSTANCES),
                      instanceRelations.getJsonArray(InstanceReferences.CHILD_INSTANCES),
                      instanceRelations.getJsonArray(InstanceReferences.SUCCEEDING_TITLES),
                      instanceRelations.getJsonArray(InstanceReferences.PRECEDING_TITLES))) {
        if (array != null && ! array.isEmpty()) {
          for (Object o : array) {
            JsonObject rel = (JsonObject) o;
            JsonObject instanceIdentifier = rel.getJsonObject( InstanceReference.INSTANCE_IDENTIFIER );
            if (instanceIdentifier != null && instanceIdentifier.containsKey( HRID_IDENTIFIER_KEY )) {
              hrids.add(instanceIdentifier.getString(HRID_IDENTIFIER_KEY));
            }
          }
        }
      }
    }
    return hrids;
  }

  private List<String> getIncomingReferencedInstanceIds () {
    List<String> uuids = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      JsonObject instanceRelations = pair.getIncomingRecordSet().instanceRelationsJson;
      for (JsonArray array :
              Arrays.asList(
                      instanceRelations.getJsonArray(InstanceReferences.PARENT_INSTANCES),
                      instanceRelations.getJsonArray(InstanceReferences.CHILD_INSTANCES),
                      instanceRelations.getJsonArray(InstanceReferences.SUCCEEDING_TITLES),
                      instanceRelations.getJsonArray(InstanceReferences.PRECEDING_TITLES))) {
        if (array != null && ! array.isEmpty()) {
          for (Object o : array) {
            JsonObject rel = (JsonObject) o;
            JsonObject instanceIdentifier = rel.getJsonObject( InstanceReference.INSTANCE_IDENTIFIER );
            if (instanceIdentifier != null && instanceIdentifier.containsKey( UUID_IDENTIFIER_KEY )) {
              uuids.add(instanceIdentifier.getString(UUID_IDENTIFIER_KEY));
            }
          }
        }
      }
    }
    return uuids;

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


}
