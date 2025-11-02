package org.folio.inventoryupdate.updating.service;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.updating.InventoryQuery;
import org.folio.inventoryupdate.updating.InventoryStorage;
import org.folio.inventoryupdate.updating.QueryByHrid;
import org.folio.inventoryupdate.updating.QueryByUUID;
import org.folio.inventoryupdate.updating.UpdateRequest;
import org.folio.inventoryupdate.updating.entities.InstanceReferences;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.folio.inventoryupdate.updating.entities.HoldingsRecord.INSTANCE_ID;
import static org.folio.inventoryupdate.updating.entities.Instance.MATCH_KEY;
import static org.folio.inventoryupdate.updating.entities.InstanceReference.INSTANCE_IDENTIFIER;
import static org.folio.inventoryupdate.updating.entities.InstanceReferences.INSTANCE_RELATIONS;
import static org.folio.inventoryupdate.updating.entities.InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID;
import static org.folio.inventoryupdate.updating.entities.InstanceRelationship.SUB_INSTANCE_ID;
import static org.folio.inventoryupdate.updating.entities.InstanceRelationship.SUPER_INSTANCE_ID;
import static org.folio.inventoryupdate.updating.entities.InstanceTitleSuccession.PRECEDING_INSTANCE_ID;
import static org.folio.inventoryupdate.updating.entities.InstanceTitleSuccession.SUCCEEDING_INSTANCE_ID;
import static org.folio.inventoryupdate.updating.entities.InventoryRecordSet.HOLDINGS_RECORDS;
import static org.folio.inventoryupdate.updating.entities.InventoryRecordSet.HRID_IDENTIFIER_KEY;
import static org.folio.inventoryupdate.updating.entities.InventoryRecordSet.INSTANCE;
import static org.folio.inventoryupdate.updating.entities.InventoryRecordSet.ITEMS;
import static org.folio.inventoryupdate.updating.entities.Item.HOLDINGS_RECORD_ID;
import static org.folio.okapi.common.HttpResponse.responseJson;

public class HandlersFetching {
  private static final String PK = "id";

  /**
   * Handles GET request to inventory upsert by HRID fetch API
   */
  public void handleInventoryRecordSetFetchHrid( UpdateRequest request) {
    String id = request.requestParam("id");
    InventoryQuery instanceQuery = getInstanceQuery( id );
    InventoryStorage.lookupSingleInventoryRecordSet( request.getOkapiClient(), instanceQuery )
        .onComplete( lookup -> {
          if (lookup.succeeded()) {
            if (lookup.result() == null) {
              responseJson(request.routingContext(), 404).end("Instance with ID " + id + " not found.");
            } else {
              JsonObject inventoryRecordSet = lookup.result();
              transformInstanceRelations (inventoryRecordSet, request).onComplete(  transform -> {
                if (transform.succeeded())
                {
                  trimPkFk( inventoryRecordSet );
                  responseJson(request.routingContext(), 200 ).end( inventoryRecordSet.encodePrettily() );
                } else {
                  responseJson(request.routingContext(), 500).end("Could not look up related instances for the Inventory record set " + transform.cause().getMessage());
                }
              });
            }
          } else {
            responseJson(request.routingContext(), 500).end("Could not look-up Inventory record set: " + lookup.cause().getMessage());
          }
        }  );
  }

  private InventoryQuery getInstanceQuery( String id )
  {
    InventoryQuery instanceQuery;
    try {
      UUID uuid = UUID.fromString( id );
      instanceQuery = new QueryByUUID( uuid );
    } catch ( IllegalArgumentException iae ) {
      instanceQuery = new QueryByHrid( id );
    }
    return instanceQuery;
  }

  /**
   * Handles GET request to shared inventory fetch API
   */
  public void handleSharedInventoryRecordSetFetch( UpdateRequest request) {
    String id = request.requestParam( "id" );
    InventoryQuery instanceQuery = getInstanceQuery( id );

    InventoryStorage.lookupSingleInventoryRecordSet(request.getOkapiClient(), instanceQuery )
        .onComplete( lookup -> {
          if (lookup.succeeded()) {
            if (lookup.result() == null) {
              responseJson(request.routingContext(), 404).end("Instance with ID " + id + " not found.");
            } else {
              JsonObject inventoryRecordSet = lookup.result();
              if (inventoryRecordSet.getJsonObject( INSTANCE ).getString( MATCH_KEY ) == null ||
                  inventoryRecordSet.getJsonObject( INSTANCE).getString( MATCH_KEY ).isEmpty() ) {
                responseJson (request.routingContext(), 400)
                    .end("Requested Inventory record set Instance has no 'matchKey' property." +
                        " It was likely not created through the shared inventory upsert API but" +
                        " it could be retrieved through some of the other fetch APIs");
              } else {
                trimPksFksHrids( inventoryRecordSet );
                responseJson(request.routingContext(), 200 ).end( inventoryRecordSet.encodePrettily() );
              }
            }
          } else {
            responseJson(request.routingContext(), 500).end("Could not look-up Inventory record set: " + lookup.cause().getMessage());
          }
        }  );
  }

  /**
   * Removes primary keys ('id') and foreign keys ('instanceId', 'holdingsRecordId') from instance, holdingsRecord and item
   * @param inventoryRecordSet The record set being mutated
   */
  private void trimPkFk(JsonObject inventoryRecordSet) {
    inventoryRecordSet.getJsonObject( INSTANCE ).remove( PK );
    if (inventoryRecordSet.getJsonArray( HOLDINGS_RECORDS ) != null) {
      for (Object hr : inventoryRecordSet.getJsonArray( HOLDINGS_RECORDS )) {
        JsonObject holdingsRecord = (JsonObject) hr;
        holdingsRecord.remove( PK );
        holdingsRecord.remove( INSTANCE_ID );
        if (holdingsRecord.containsKey( ITEMS )) {
          for (Object i : holdingsRecord.getJsonArray( ITEMS )) {
            JsonObject item = (JsonObject) i;
            item.remove( PK );
            item.remove(HOLDINGS_RECORD_ID);

          }
        }
      }
    }
  }

  /**
   * Removes primary keys ('id'), foreign keys ('instanceId', 'holdingsRecordId') and HRIDs ('hrid') from
   * instance, holdingsRecord and item.
   * @param inventoryRecordSet The record set being mutated
   */
  private void trimPksFksHrids (JsonObject inventoryRecordSet) {
    inventoryRecordSet.getJsonObject( INSTANCE ).remove( PK );
    inventoryRecordSet.getJsonObject( INSTANCE).remove( HRID_IDENTIFIER_KEY );
    if (inventoryRecordSet.getJsonArray( HOLDINGS_RECORDS ) != null) {
      for (Object hr : inventoryRecordSet.getJsonArray( HOLDINGS_RECORDS )) {
        JsonObject holdingsRecord = (JsonObject) hr;
        holdingsRecord.remove( PK );
        holdingsRecord.remove( INSTANCE_ID );
        holdingsRecord.remove( HRID_IDENTIFIER_KEY );
        if (holdingsRecord.containsKey( ITEMS )) {
          for (Object i : holdingsRecord.getJsonArray( ITEMS )) {
            JsonObject item = (JsonObject) i;
            item.remove( PK );
            item.remove(HOLDINGS_RECORD_ID);
            item.remove(HRID_IDENTIFIER_KEY);
          }
        }
      }
    }
  }

  /**
   * Transform instance relations retrieved from Inventory storage into a format that can be used for a PUT request
   * to the inventory-upsert-hrid API
   * @param inventoryRecordSet the record set being mutated
   */
  private Future<Void> transformInstanceRelations (JsonObject inventoryRecordSet, UpdateRequest request) {
    Promise<Void> promise = Promise.promise();
    String instanceId = inventoryRecordSet.getJsonObject(INSTANCE).getString("id");
    JsonObject instanceRelations = inventoryRecordSet.getJsonObject( InstanceReferences.INSTANCE_RELATIONS );
    JsonArray existingParentChildRelations = instanceRelations.getJsonArray( InstanceReferences.EXISTING_PARENT_CHILD_RELATIONS );
    JsonArray existingPrecedingSucceedingTitles = instanceRelations.getJsonArray( InstanceReferences.EXISTING_PRECEDING_SUCCEEDING_TITLES );
    if (existingParentChildRelations.size() + existingPrecedingSucceedingTitles.size() == 0) {
      promise.complete();
    } else {
      createInstanceUuidToHridMap( inventoryRecordSet, request ).onComplete( idToHridMap -> {
        if (idToHridMap.succeeded())
        {
          Map<String,String> uuidToHrid = idToHridMap.result();
          JsonArray parentInstances = new JsonArray();
          JsonArray childInstances = new JsonArray();
          if ( !existingParentChildRelations.isEmpty() )
          {
            for ( Object o : existingParentChildRelations )
            {
              JsonObject existingParentChildRelation = (JsonObject) o;
              JsonObject parentChildRelation = new JsonObject();
              JsonObject instanceIdentifier = new JsonObject();
              parentChildRelation.put( INSTANCE_IDENTIFIER, instanceIdentifier );
              parentChildRelation.put( INSTANCE_RELATIONSHIP_TYPE_ID, existingParentChildRelation.getString( INSTANCE_RELATIONSHIP_TYPE_ID ) );
              if ( instanceId.equals( existingParentChildRelation.getString( SUPER_INSTANCE_ID ) ) )
              {
                instanceIdentifier.put( HRID_IDENTIFIER_KEY, uuidToHrid.get(existingParentChildRelation.getString( SUB_INSTANCE_ID )));
                childInstances.add( parentChildRelation );
              }
              else if ( instanceId.equals( existingParentChildRelation.getString( SUB_INSTANCE_ID ) ) )
              {
                instanceIdentifier.put( HRID_IDENTIFIER_KEY, uuidToHrid.get(existingParentChildRelation.getString( SUPER_INSTANCE_ID )));
                parentInstances.add( parentChildRelation );
              }
            }
          }
          instanceRelations.remove( InstanceReferences.EXISTING_PARENT_CHILD_RELATIONS );
          instanceRelations.put( InstanceReferences.PARENT_INSTANCES, parentInstances );
          instanceRelations.put( InstanceReferences.CHILD_INSTANCES, childInstances );

          JsonArray precedingTitles = new JsonArray();
          JsonArray succeedingTitles = new JsonArray();
          if ( !existingPrecedingSucceedingTitles.isEmpty() )
          {
            for ( Object o : existingPrecedingSucceedingTitles )
            {
              JsonObject existingPrecedingSucceeding = (JsonObject) o;
              JsonObject precedingSucceeding = new JsonObject();
              JsonObject instanceIdentifier = new JsonObject();
              precedingSucceeding.put( INSTANCE_IDENTIFIER, instanceIdentifier );
              precedingSucceeding.put( INSTANCE_RELATIONSHIP_TYPE_ID, existingPrecedingSucceeding.getString( INSTANCE_RELATIONSHIP_TYPE_ID ) );
              if ( instanceId.equals( existingPrecedingSucceeding.getString( PRECEDING_INSTANCE_ID ) ) )
              {
                instanceIdentifier.put( HRID_IDENTIFIER_KEY, uuidToHrid.get(existingPrecedingSucceeding.getString( SUCCEEDING_INSTANCE_ID )));
                succeedingTitles.add( precedingSucceeding );
              }
              else if ( instanceId.equals( existingPrecedingSucceeding.getString( SUCCEEDING_INSTANCE_ID ) ) )
              {
                instanceIdentifier.put( HRID_IDENTIFIER_KEY, uuidToHrid.get(existingPrecedingSucceeding.getString( PRECEDING_INSTANCE_ID )));
                precedingTitles.add( precedingSucceeding );
              }
            }
          }
          instanceRelations.remove( InstanceReferences.EXISTING_PRECEDING_SUCCEEDING_TITLES );
          instanceRelations.put( InstanceReferences.PRECEDING_TITLES, precedingTitles );
          instanceRelations.put( InstanceReferences.SUCCEEDING_TITLES, succeedingTitles );
          promise.complete();
        }
      } );
    }
    return promise.future();
  }

  /**
   * Creates a map of instance HRIDs by instance UUIDs
   * @param inventoryRecordSet the record set containing the UUIDs to map if any
   * @return map of instance HRIDs by instance UUIDs
   */
  private Future<Map<String,String>>  createInstanceUuidToHridMap (JsonObject inventoryRecordSet, UpdateRequest request) {
    Promise<Map<String,String>> promise = Promise.promise();
    OkapiClient client = request.getOkapiClient();
    List<String> relatedIds = new ArrayList<>();
    JsonObject instanceRelations = inventoryRecordSet.getJsonObject( INSTANCE_RELATIONS );
    String instanceId = inventoryRecordSet.getJsonObject( INSTANCE ).getString( PK );
    JsonArray parentChildRelations = instanceRelations.getJsonArray(InstanceReferences.EXISTING_PARENT_CHILD_RELATIONS);
    for (Object o : parentChildRelations) {
      JsonObject relation = ((JsonObject) o);
      if (! instanceId.equals(relation.getString( SUB_INSTANCE_ID ))) {
        relatedIds.add(relation.getString( SUB_INSTANCE_ID ));
      } else if ( ! instanceId.equals( relation.getString( SUPER_INSTANCE_ID ))) {
        relatedIds.add(relation.getString( SUPER_INSTANCE_ID ));
      }
    }
    JsonArray titleSuccessions = instanceRelations.getJsonArray( InstanceReferences.EXISTING_PRECEDING_SUCCEEDING_TITLES );
    for (Object o : titleSuccessions) {
      JsonObject relation = ((JsonObject) o);
      if (! instanceId.equals(relation.getString( PRECEDING_INSTANCE_ID ))) {
        relatedIds.add(relation.getString( PRECEDING_INSTANCE_ID ));
      } else if ( ! instanceId.equals( relation.getString( SUCCEEDING_INSTANCE_ID ))) {
        relatedIds.add(relation.getString( SUCCEEDING_INSTANCE_ID ));
      }
    }
    List<Future<JsonObject>> instanceFutures = new ArrayList<>();
    for (String relatedInstanceId : relatedIds) {
      QueryByUUID query = new QueryByUUID( relatedInstanceId );
      instanceFutures.add(InventoryStorage.lookupInstance( client, query ));
    }
    if (instanceFutures.isEmpty()) {
      promise.complete(new HashMap<>());
    } else
    {
      Future.all( instanceFutures ).onComplete( relatedInstances -> {
        if ( relatedInstances.succeeded() )
        {
          if ( relatedInstances.result().list() != null )
          {
            Map<String,String> uuidToHridMap = new HashMap<>();
            for ( Object o : relatedInstances.result().list()) {
              JsonObject instance = (JsonObject) o;
              uuidToHridMap.put(instance.getString( PK ), instance.getString( HRID_IDENTIFIER_KEY ));
            }
            promise.complete(uuidToHridMap);
          }
        } else {
          promise.fail( "Failed to look up some of the Instance's relations" );
        }
      } );
    }
    return promise.future();
  }

}
