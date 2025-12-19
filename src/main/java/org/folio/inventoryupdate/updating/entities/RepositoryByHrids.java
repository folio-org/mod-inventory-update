package org.folio.inventoryupdate.updating.entities;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.updating.ErrorReport;
import org.folio.inventoryupdate.updating.InventoryStorage;
import org.folio.inventoryupdate.updating.QueryByListOfIds;
import org.folio.inventoryupdate.updating.UpdateRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.folio.inventoryupdate.updating.entities.InventoryRecordSet.*;

public class RepositoryByHrids extends Repository {

  protected final Map<String,Map<String,InstanceToInstanceRelation>> existingParentRelationsByChildId = new HashMap<>();
  protected final Map<String,Map<String,InstanceToInstanceRelation>> existingChildRelationsByParentId = new HashMap<>();
  protected final Map<String,Map<String,InstanceToInstanceRelation>> existingSucceedingRelationsByPrecedingId = new HashMap<>();
  protected final Map<String,Map<String,InstanceToInstanceRelation>> existingPrecedingRelationsBySucceedingId = new HashMap<>();
  public final Map<String,Instance> referencedInstancesByHrid = new HashMap<>();
  public final Map<String,Instance> referencedInstancesByUUID = new HashMap<>();
  public final Map<String,Instance> provisionalInstancesByHrid = new HashMap<>();

  public Future<Void> buildRepositoryFromStorage (UpdateRequest request) {
    List<Future<Void>> existingRecordsByHridsFutures = new ArrayList<>();
    for (List<String> idList : getSubListsOfTen(getIncomingInstanceHRIDs())) {
      existingRecordsByHridsFutures.add(requestInstanceSetsByHRIDs(request, idList));
    }
    for (List<String> idList : getSubListsOfFifty(getIncomingHoldingsRecordHRIDs())) {
      existingRecordsByHridsFutures.add(requestHoldingsRecordsByHRIDs(request, idList));
    }
    for (List<String> idList : getSubListsOfFifty(getIncomingItemHRIDs())) {
      existingRecordsByHridsFutures.add(requestItemsByHRIDs(request, idList));
    }
    for (List<String> idList : getSubListsOfFifty(getIncomingReferencedInstanceHrids())) {
      existingRecordsByHridsFutures.add(requestReferencedInstancesByHRIDs(request, idList));
    }
    for (List<String> idList : getSubListsOfFifty(getIncomingReferencedInstanceIds())) {
      existingRecordsByHridsFutures.add(requestReferencedInstancesByUUIDs(request, idList));
    }
    return Future.join(existingRecordsByHridsFutures)
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


  private Future<Void> requestInstanceSetsByHRIDs(UpdateRequest updateRequest,
                                                  List<String> hrids) {
    return InventoryStorage.lookupInstanceSets(updateRequest.getOkapiClient(), new QueryByListOfIds("hrid", hrids))
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
    var succeedingInstanceId = precedingRelationshipObject.getString("succeedingInstanceId");
    InstanceTitleSuccession relationship = InstanceTitleSuccession.makeInstanceTitleSuccessionFromJsonRecord(
            succeedingInstanceId, precedingRelationshipObject);
    existingPrecedingRelationsBySucceedingId.computeIfAbsent(succeedingInstanceId, k -> new HashMap<>())
    .put(relationship.getPrecedingInstanceId(), relationship);
  }

  private Future<Void> requestReferencedInstancesByHRIDs(UpdateRequest request,
                                                         List<String> hrids) {
    Promise<Void> promise = Promise.promise();
    InventoryStorage.lookupInstances(request.getOkapiClient(),
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

  private Future<Void> requestReferencedInstancesByUUIDs(UpdateRequest request,
                                                         List<String> uuids) {
    Promise<Void> promise = Promise.promise();
    InventoryStorage.lookupInstances(request.getOkapiClient(),
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

  private Future<Void> requestHoldingsRecordsByHRIDs(UpdateRequest request,
                                                     List<String> hrids) {
    Promise<Void> promise = Promise.promise();
    InventoryStorage.lookupHoldingsRecords(request.getOkapiClient(),
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

  private Future<Void> requestItemsByHRIDs(UpdateRequest request, List<String> hrids) {
    Promise<Void> promise = Promise.promise();
    InventoryStorage.lookupItems(request.getOkapiClient(),
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

  public Instance getCreatingInstanceByHrid (String hrid) {
    for (Instance instance : getInstancesToCreate()) {
      if (instance.getHRID().equals(hrid) && !instance.failed()) {
        return instance;
      }
    }
    return null;
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
      for (HoldingsRecord holdingsRecord : holdingsRecords) {
        hrids.add(holdingsRecord.getHRID());
      }
    }
    return hrids;
  }

  private List<String> getIncomingItemHRIDs () {
    List<String> hrids = new ArrayList<>();
    for (PairedRecordSets pair : pairsOfRecordSets) {
      List<Item> items = pair.getIncomingRecordSet().getItems();
      for (Item item : items) {
        hrids.add(item.getHRID());
      }
    }
    return hrids;
  }


}
