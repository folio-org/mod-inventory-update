package org.folio.inventoryupdate;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.InstanceRelationsManager;

import java.util.UUID;

import static org.folio.okapi.common.HttpResponse.responseJson;

public class InventoryFetchService
{
    private final Logger logger = LoggerFactory.getLogger("inventory-update");

    public void handleGetInventoryRecordSet( RoutingContext routingContext) {
        String id = routingContext.request().getParam( "id" );
        InventoryQuery instanceQuery;
        try
        {
            UUID uuid = UUID.fromString( id );
            instanceQuery = new QueryByUUID( uuid );
        } catch (IllegalArgumentException iae) {
            instanceQuery = new HridQuery( id );
        }

        InventoryStorage.lookupSingleInventoryRecordSet( InventoryStorage.getOkapiClient(routingContext), instanceQuery )
                .onComplete( lookup -> {
                    if (lookup.succeeded()) {
                        if (lookup.result() == null) {
                            responseJson(routingContext, 404).end("Instance with ID " + id + " not found.");
                        } else {
                            JsonObject inventoryRecordSet = lookup.result();
                            transformInstanceRelations (inventoryRecordSet);
                            responseJson( routingContext, 200 ).end( inventoryRecordSet.encodePrettily() );
                        }
                    } else {
                        responseJson(routingContext, 500).end("Could not look-up Inventory record set: "
                        + lookup.cause().getMessage());
                    }
                }  );
    }

    private void transformInstanceRelations ( JsonObject inventoryRecordSet) {
        String instanceId = inventoryRecordSet.getJsonObject("instance").getString("id");
        JsonObject instanceRelations = inventoryRecordSet.getJsonObject( InstanceRelationsManager.INSTANCE_RELATIONS );
        JsonArray existingParentChildRelations = instanceRelations.getJsonArray( InstanceRelationsManager.EXISTING_PARENT_CHILD_RELATIONS );
        JsonArray parentInstances = new JsonArray();
        JsonArray childInstances = new JsonArray();
        if (!existingParentChildRelations.isEmpty()) {
            for (Object o : existingParentChildRelations) {
                JsonObject existingParentChildRelation = (JsonObject) o;
                JsonObject parentChildRelation = new JsonObject();
                JsonObject instanceIdentifier = new JsonObject();
                parentChildRelation.put("instanceIdentifier", instanceIdentifier);
                parentChildRelation.put("instanceRelationshipTypeId", existingParentChildRelation.getString( "instanceRelationshipTypeId" ));
                if (instanceId.equals( existingParentChildRelation.getString("superInstanceId"))) {
                    instanceIdentifier.put("uuid", existingParentChildRelation.getString("subInstanceId"));
                    childInstances.add(parentChildRelation);
                } else if (instanceId.equals(existingParentChildRelation.getString( "subInstanceId" ))) {
                    instanceIdentifier.put("uuid", existingParentChildRelation.getString("superInstanceId"));
                    parentInstances.add(parentChildRelation);
                }
            }
        }
        instanceRelations.remove( InstanceRelationsManager.EXISTING_PARENT_CHILD_RELATIONS );
        instanceRelations.put(InstanceRelationsManager.PARENT_INSTANCES, parentInstances);
        instanceRelations.put(InstanceRelationsManager.CHILD_INSTANCES, childInstances);

        JsonArray existingPrecedingSucceedingTitles = instanceRelations.getJsonArray( InstanceRelationsManager.EXISTING_PRECEDING_SUCCEEDING_TITLES );
        JsonArray precedingTitles = new JsonArray();
        JsonArray succeedingTitles = new JsonArray();
        if (!existingPrecedingSucceedingTitles.isEmpty()) {
            for (Object o : existingPrecedingSucceedingTitles) {
                JsonObject existingPrecedingSucceeding = (JsonObject) o;
                JsonObject precedingSucceeding = new JsonObject();
                JsonObject instanceIdentifier = new JsonObject();
                precedingSucceeding.put("instanceIdentifier", instanceIdentifier);
                precedingSucceeding.put("instanceRelationshipTypeId", existingPrecedingSucceeding.getString( "instanceRelationshipTypeId" ));
                if (instanceId.equals( existingPrecedingSucceeding.getString("precedingInstanceId"))) {
                    instanceIdentifier.put("uuid", existingPrecedingSucceeding.getString("succeedingInstanceId"));
                    succeedingTitles.add(precedingSucceeding);
                } else if (instanceId.equals(existingPrecedingSucceeding.getString( "succeedingInstanceId" ))) {
                    instanceIdentifier.put("uuid", existingPrecedingSucceeding.getString("precedingInstanceId"));
                    precedingTitles.add(precedingSucceeding);
                }
            }
        }
        instanceRelations.remove(InstanceRelationsManager.EXISTING_PRECEDING_SUCCEEDING_TITLES);
        instanceRelations.put(InstanceRelationsManager.PRECEDING_TITLES, precedingTitles);
        instanceRelations.put(InstanceRelationsManager.SUCCEEDING_TITLES, succeedingTitles);

    }
 }
