package org.folio.inventoryupdate;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.*;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class InstanceRelationshipsManager {

    UpdatePlan plan;
    List<InstanceRelationship> existingInstanceRelationships = new ArrayList<>();
    List<InstanceRelationship> updatingInstanceRelationships = new ArrayList<>();
    private InstanceRelations instanceRelations;
    OkapiClient client;

    public InstanceRelationshipsManager (UpdatePlan plan) {
        this.plan = plan;
    }

    public void setOkapiClient (OkapiClient client) {
        this.client = client;
    }

    public Future planInstanceDelete() {
        return lookupExistingInstanceRelationships(client, plan.getExistingInstance().getUUID()).onComplete( relationships -> {
            if (relationships.succeeded()) {
                for (InstanceRelationship relationship : relationships.result()) {
                    relationship.setTransition(InventoryRecord.Transaction.DELETE);
                }
            }
        });
    }

    public Future<Void> planInstanceUpsert() {
        if (plan.getExistingRecordSet() == null) {
            return planInstanceCreate();
        } else {
            return planInstanceUpdate();
        }
    }

    public Future planInstanceUpdate () {
        return lookupExistingInstanceRelationships(client, plan.getExistingRecordSet().getInstanceUUID()).onComplete( relationships -> {
           if (relationships.succeeded()) {

           }
        });
    }

    public Future<Void> planInstanceCreate () {
        Promise promise = Promise.promise();
        return promise.future();
    }

    public Future<InstanceRelationship> constructInstanceRelationshipFromParentHRID(OkapiClient client, String instanceId, String parentHrid, String instanceRelationshipTypeId) {
        Promise<InstanceRelationship> promise = Promise.promise();
        InventoryQuery hridQuery = new HridQuery(parentHrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete( instanceResponse -> {
            if (instanceResponse.succeeded()) {
                if (instanceResponse.result() != null) {
                    Instance instance = new Instance(instanceResponse.result());
                    InstanceRelationship relationship = new InstanceRelationship(UUID.randomUUID().toString(), instanceId, instance.getUUID(), instanceRelationshipTypeId);
                    promise.complete(relationship);
                }
            }
        });
        return promise.future();
    }

    public Future<List<InstanceRelationship>> lookupExistingInstanceRelationships (OkapiClient okapiClient, String instanceId) {
        Promise<List<InstanceRelationship>> promise = Promise.promise();
        InventoryStorage.lookupExistingInstanceRelationshipsByInstanceUUID(okapiClient, instanceId).onComplete( relations -> {
            if (relations.succeeded()) {
                List<InstanceRelationship> relationships = new ArrayList<>();
                JsonArray existingInstanceRelationshipsJson = relations.result();
                for (Object o : existingInstanceRelationshipsJson) {
                    JsonObject relation = (JsonObject) o;
                    InstanceRelationship relationship = new InstanceRelationship(relation);
                    relationships.add(relationship);
                }
                promise.complete(relationships);
            } else {
                promise.fail("Error looking up existing instance relationships: " + relations.cause().getMessage());
            }
        });
        return promise.future();
    }
}
