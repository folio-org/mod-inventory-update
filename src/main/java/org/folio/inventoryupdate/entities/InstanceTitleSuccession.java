package org.folio.inventoryupdate.entities;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.HridQuery;
import org.folio.inventoryupdate.InventoryQuery;
import org.folio.inventoryupdate.InventoryStorage;
import org.folio.okapi.common.OkapiClient;

public class InstanceTitleSuccession extends InstanceToInstanceRelation {

    public static final String ID = "id";
    public static final String PRECEDING_INSTANCE_ID = "precedingInstanceId";
    public static final String SUCCEEDING_INSTANCE_ID = "succeedingInstanceId";
    private String instanceId;

    public static InstanceTitleSuccession makeInstanceTitleSuccessionFromJsonRecord(String instanceId, JsonObject precedingSucceedingJson) {
        InstanceTitleSuccession titleSuccession = new InstanceTitleSuccession();
        titleSuccession.jsonRecord = precedingSucceedingJson;
        titleSuccession.type = Entity.INSTANCE_TITLE_SUCCESSION;
        titleSuccession.instanceId = instanceId;
        return titleSuccession;
    }

    public static InstanceTitleSuccession makeInstanceTitleSuccession(String precedingInstanceId, String succeedingInstanceId) {
        InstanceTitleSuccession instanceTitleSuccession = new InstanceTitleSuccession();
        instanceTitleSuccession.jsonRecord = new JsonObject();
        instanceTitleSuccession.jsonRecord.put(SUCCEEDING_INSTANCE_ID, succeedingInstanceId);
        instanceTitleSuccession.jsonRecord.put(PRECEDING_INSTANCE_ID, precedingInstanceId);
        instanceTitleSuccession.type = Entity.INSTANCE_TITLE_SUCCESSION;
        return instanceTitleSuccession;
    }

    public String getSucceedingInstanceId () {
        return jsonRecord.getString(SUCCEEDING_INSTANCE_ID);
    }

    public String getPrecedingInstanceId() {
        return jsonRecord.getString(PRECEDING_INSTANCE_ID);
    }

    public boolean isSucceedingTitle () {
        return getPrecedingInstanceId().equals(instanceId);
    }

    public boolean isPrecedingTitle() {
        return getSucceedingInstanceId().equals(instanceId);
    }


    //Todo: DRY  makePreceingTitleWithInstanceIdentifier and makeSucceedingTitleWithInstanceIdentifier
    static Future<InstanceTitleSuccession> makePrecedingTitleWithInstanceIdentifier(OkapiClient client, String instanceId, JsonObject precedingJson, String identifierKey) {
        Promise<InstanceTitleSuccession> promise = Promise.promise();
        String hrid = precedingJson.getJsonObject(InstanceRelationsController.INSTANCE_IDENTIFIER).getString(identifierKey);
        InventoryQuery hridQuery = new HridQuery(hrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete(instance -> {
            if (instance.succeeded()) {
                if (instance.result() != null) {
                    JsonObject instanceJson = instance.result();
                    InstanceTitleSuccession preceding = makeInstanceTitleSuccession(
                            instanceJson.getString(ID),
                            instanceId);
                    preceding.requiresProvisionalInstanceToBeCreated(false);
                    promise.complete(preceding);
                } else {
                    JsonObject provisionalInstanceJson = precedingJson.getJsonObject(InstanceToInstanceRelation.PROVISIONAL_INSTANCE);
                    if (provisionalInstanceJson == null) {
                        //todo: don't here now, create succession record with no provisional instance fail during update
                        promise.fail(" Referenced preceding title not found and no provisional Instance info provided; cannot create relation to non-existing Instance [" + hrid + "], got:" + InstanceRelationsController.LF + precedingJson.encodePrettily());
                    } else {
                        String title = provisionalInstanceJson.getString(InstanceRelationsController.TITLE);
                        String source = provisionalInstanceJson.getString(InstanceRelationsController.SOURCE);
                        String instanceTypeId = provisionalInstanceJson.getString(InstanceRelationsController.INSTANCE_TYPE_ID);
                        if (title == null || source == null || instanceTypeId == null) {
                            //todo: don't fail here, create succession record with no provisional instance fail during update
                            promise.fail(" Cannot create relation to non-existing Instance [" + hrid + "] unless both title, source and resource type is provided for creating a provisional Instance, got:" + InstanceRelationsController.LF + precedingJson.encodePrettily());
                        } else {
                            Instance provisionalInstance = prepareProvisionalInstance(hrid, provisionalInstanceJson);
                            InstanceTitleSuccession preceding = makeInstanceTitleSuccession(
                                    provisionalInstance.getUUID(),
                                    instanceId);
                            preceding.requiresProvisionalInstanceToBeCreated(true);
                            preceding.setProvisionalInstance(provisionalInstance);
                            promise.complete(preceding);
                        }
                    }
                }
            }
        });
        return promise.future();
    }

    static Future<InstanceTitleSuccession> makeSucceedingTitleWithInstanceIdentifier(OkapiClient client, String instanceId, JsonObject succeedingJson, String identifierKey) {
        Promise<InstanceTitleSuccession> promise = Promise.promise();
        String hrid = succeedingJson.getJsonObject(InstanceRelationsController.INSTANCE_IDENTIFIER).getString(identifierKey);
        InventoryQuery hridQuery = new HridQuery(hrid);
        InventoryStorage.lookupInstance(client, hridQuery).onComplete(instance -> {
            if (instance.succeeded()) {
                if (instance.result() != null) {
                    JsonObject succeedingInstanceJson = instance.result();
                    InstanceTitleSuccession succeeding = makeInstanceTitleSuccession(
                            instanceId,
                            succeedingInstanceJson.getString(ID));
                    succeeding.requiresProvisionalInstanceToBeCreated(false);
                    promise.complete(succeeding);
                } else {
                    JsonObject provisionalInstanceJson = succeedingJson.getJsonObject(InstanceToInstanceRelation.PROVISIONAL_INSTANCE);
                    if (provisionalInstanceJson == null) {
                        promise.fail(" Referenced preceding title not found and no provisional Instance info provided; cannot create relation to non-existing Instance [" + hrid + "], got:" + InstanceRelationsController.LF + succeedingJson.encodePrettily());
                    } else {
                        String title = provisionalInstanceJson.getString(InstanceRelationsController.TITLE);
                        String source = provisionalInstanceJson.getString(InstanceRelationsController.SOURCE);
                        String instanceTypeId = provisionalInstanceJson.getString(InstanceRelationsController.INSTANCE_TYPE_ID);
                        if (title == null || source == null || instanceTypeId == null) {
                            promise.fail(" Cannot create relation to non-existing Instance [" + hrid + "] unless both title, source and resource type is provided for creating a provisional Instance, got:" + InstanceRelationsController.LF + succeedingJson.encodePrettily());
                        } else {
                            Instance provisionalInstance = prepareProvisionalInstance(hrid, provisionalInstanceJson);
                            InstanceTitleSuccession succeeding = makeInstanceTitleSuccession(
                                    instanceId,
                                    provisionalInstance.getUUID());
                            succeeding.setProvisionalInstance(provisionalInstance);
                            promise.complete(succeeding);
                        }
                    }
                }
            }
        });
        return promise.future();
    }

    @Override
    public void skipDependants() {}

    @Override
    public boolean equals (Object o) {
        if (o instanceof InstanceTitleSuccession) {
            InstanceTitleSuccession other = (InstanceTitleSuccession) o;
            return (other.getSucceedingInstanceId().equals(this.getSucceedingInstanceId()) &&
                    other.getPrecedingInstanceId().equals(this.getPrecedingInstanceId()));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public String toString () {
        StringBuilder str = new StringBuilder();
        str.append("// Preceding: ").append(jsonRecord.getString(PRECEDING_INSTANCE_ID))
                .append(" Succeeding: ").append(jsonRecord.getString(SUCCEEDING_INSTANCE_ID));
        return str.toString();
    }

}
