package org.folio.inventoryupdate;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.InstanceRelationsManager;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.folio.inventoryupdate.entities.InstanceRelationsManager.INSTANCE_RELATIONS;
import static org.folio.inventoryupdate.entities.InventoryRecordSet.*;
import static org.folio.inventoryupdate.entities.Instance.MATCH_KEY;
import static org.folio.inventoryupdate.entities.Item.HOLDINGS_RECORD_ID;
import static org.folio.inventoryupdate.entities.HoldingsRecord.INSTANCE_ID;
import static org.folio.okapi.common.HttpResponse.responseJson;
import static org.folio.inventoryupdate.entities.InstanceRelationsManager.INSTANCE_IDENTIFIER;
import static org.folio.inventoryupdate.entities.InstanceRelationship.INSTANCE_RELATIONSHIP_TYPE_ID;
import static org.folio.inventoryupdate.entities.InstanceRelationship.SUB_INSTANCE_ID;
import static org.folio.inventoryupdate.entities.InstanceRelationship.SUPER_INSTANCE_ID;
import static org.folio.inventoryupdate.entities.InstanceTitleSuccession.PRECEDING_INSTANCE_ID;
import static org.folio.inventoryupdate.entities.InstanceTitleSuccession.SUCCEEDING_INSTANCE_ID;


public class InventoryFetchService
{
    private static final String PK = "id";

    /**
     * Handles GET request to inventory upsert by HRID fetch API
     */
    public void handleInventoryRecordSetFetchHrid( RoutingContext routingContext) {
        String id = routingContext.request().getParam( "id" );
        InventoryQuery instanceQuery = getInstanceQuery( id );
        InventoryStorage.lookupSingleInventoryRecordSet( InventoryStorage.getOkapiClient(routingContext), instanceQuery )
                .onComplete( lookup -> {
                    if (lookup.succeeded()) {
                        if (lookup.result() == null) {
                            responseJson(routingContext, 404).end("Instance with ID " + id + " not found.");
                        } else {
                            JsonObject inventoryRecordSet = lookup.result();
                            transformInstanceRelations (inventoryRecordSet, routingContext).onComplete(  transform -> {
                                if (transform.succeeded())
                                {
                                    trimPkFk( inventoryRecordSet );
                                    responseJson( routingContext, 200 ).end( inventoryRecordSet.encodePrettily() );
                                } else {
                                    responseJson(routingContext, 500).end("Could not look up related instances for the Inventory record set " + transform.cause().getMessage());
                                }
                            });
                        }
                    } else {
                        responseJson(routingContext, 500).end("Could not look-up Inventory record set: " + lookup.cause().getMessage());
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
            instanceQuery = new HridQuery( id );
        }
        return instanceQuery;
    }

    /**
     * Handles GET request to shared inventory fetch API
     */
    public void handleSharedInventoryRecordSetFetch( RoutingContext routingContext) {
        String id = routingContext.request().getParam( "id" );
        InventoryQuery instanceQuery = getInstanceQuery( id );

        InventoryStorage.lookupSingleInventoryRecordSet( InventoryStorage.getOkapiClient(routingContext), instanceQuery )
                .onComplete( lookup -> {
                    if (lookup.succeeded()) {
                        if (lookup.result() == null) {
                            responseJson(routingContext, 404).end("Instance with ID " + id + " not found.");
                        } else {
                            JsonObject inventoryRecordSet = lookup.result();
                            if (inventoryRecordSet.getJsonObject( INSTANCE ).getString( MATCH_KEY ) == null ||
                                inventoryRecordSet.getJsonObject( INSTANCE).getString( MATCH_KEY ).isEmpty() ) {
                                responseJson (routingContext, 400)
                                        .end("Requested Inventory record set Instance has no 'matchKey' property." +
                                                " It was likely not created through the shared inventory upsert API but" +
                                                " it could be retrieved through some of the other fetch APIs");
                            } else {
                                trimPksFksHrids( inventoryRecordSet );
                                responseJson( routingContext, 200 ).end( inventoryRecordSet.encodePrettily() );
                            }
                        }
                    } else {
                        responseJson(routingContext, 500).end("Could not look-up Inventory record set: " + lookup.cause().getMessage());
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
     * @param routingContext context for looking up instance HRIDs with instance UUIDs
     */
    private Future<Void> transformInstanceRelations ( JsonObject inventoryRecordSet, RoutingContext routingContext) {
        Promise<Void> promise = Promise.promise();
        String instanceId = inventoryRecordSet.getJsonObject(INSTANCE).getString("id");
        JsonObject instanceRelations = inventoryRecordSet.getJsonObject( InstanceRelationsManager.INSTANCE_RELATIONS );
        JsonArray existingParentChildRelations = instanceRelations.getJsonArray( InstanceRelationsManager.EXISTING_PARENT_CHILD_RELATIONS );
        JsonArray existingPrecedingSucceedingTitles = instanceRelations.getJsonArray( InstanceRelationsManager.EXISTING_PRECEDING_SUCCEEDING_TITLES );
        if (existingParentChildRelations.size() + existingPrecedingSucceedingTitles.size() == 0) {
            promise.complete();
        } else {
            createInstanceUuidToHridMap( inventoryRecordSet, routingContext ).onComplete( idToHridMap -> {
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
                    instanceRelations.remove( InstanceRelationsManager.EXISTING_PARENT_CHILD_RELATIONS );
                    instanceRelations.put( InstanceRelationsManager.PARENT_INSTANCES, parentInstances );
                    instanceRelations.put( InstanceRelationsManager.CHILD_INSTANCES, childInstances );

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
                    instanceRelations.remove( InstanceRelationsManager.EXISTING_PRECEDING_SUCCEEDING_TITLES );
                    instanceRelations.put( InstanceRelationsManager.PRECEDING_TITLES, precedingTitles );
                    instanceRelations.put( InstanceRelationsManager.SUCCEEDING_TITLES, succeedingTitles );
                    promise.complete();
                }
            } );
        }
        return promise.future();
    }

    /**
     * Creates a map of instance HRIDs by instance UUIDs
     * @param inventoryRecordSet the record set containing the UUIDs to map if any
     * @param routingContext context used for looking up instances by UUID
     * @return map of instance HRIDs by instance UUIDs
     */
    private Future<Map<String,String>>  createInstanceUuidToHridMap (JsonObject inventoryRecordSet, RoutingContext routingContext) {
        Promise<Map<String,String>> promise = Promise.promise();
        OkapiClient client = InventoryStorage.getOkapiClient( routingContext );
        List<String> relatedIds = new ArrayList<>();
        JsonObject instanceRelations = inventoryRecordSet.getJsonObject( INSTANCE_RELATIONS );
        String instanceId = inventoryRecordSet.getJsonObject( INSTANCE ).getString( PK );
        JsonArray parentChildRelations = instanceRelations.getJsonArray(InstanceRelationsManager.EXISTING_PARENT_CHILD_RELATIONS);
        for (Object o : parentChildRelations) {
            JsonObject relation = ((JsonObject) o);
            if (! instanceId.equals(relation.getString( SUB_INSTANCE_ID ))) {
                relatedIds.add(relation.getString( SUB_INSTANCE_ID ));
            } else if ( ! instanceId.equals( relation.getString( SUPER_INSTANCE_ID ))) {
                relatedIds.add(relation.getString( SUPER_INSTANCE_ID ));
            }
        }
        JsonArray titleSuccessions = instanceRelations.getJsonArray( InstanceRelationsManager.EXISTING_PRECEDING_SUCCEEDING_TITLES );
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
            GenericCompositeFuture.all( instanceFutures ).onComplete( relatedInstances -> {
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
