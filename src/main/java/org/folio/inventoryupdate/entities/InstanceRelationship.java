package org.folio.inventoryupdate.entities;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.HridQuery;
import org.folio.inventoryupdate.InventoryQuery;
import org.folio.inventoryupdate.InventoryStorage;
import org.folio.okapi.common.OkapiClient;

public class InstanceRelationship extends InstanceToInstanceRelation {

    private String instanceId;
    public static final String SUB_INSTANCE_ID = "subInstanceId";
    public static final String SUPER_INSTANCE_ID = "superInstanceId";
    public static final String INSTANCE_RELATIONSHIP_TYPE_ID = "instanceRelationshipTypeId";

    protected static final Logger logger = LoggerFactory.getLogger("inventory-update");

    public static InstanceRelationship makeRelationshipFromJsonRecord(String instanceId, JsonObject instanceRelationJson) {
        InstanceRelationship relation = new InstanceRelationship();
        relation.instanceId = instanceId;
        relation.jsonRecord = instanceRelationJson;
        relation.type = Entity.INSTANCE_RELATIONSHIP;
        return relation;
    }

    public static InstanceRelationship makeRelationship(String instanceId, String subInstanceId, String superInstanceId, String instanceRelationshipTypeId) {
        InstanceRelationship relation = new InstanceRelationship();
        relation.instanceId = instanceId;
        relation.jsonRecord = new JsonObject();
        relation.jsonRecord.put(SUB_INSTANCE_ID, subInstanceId);
        relation.jsonRecord.put(SUPER_INSTANCE_ID, superInstanceId);
        relation.jsonRecord.put(INSTANCE_RELATIONSHIP_TYPE_ID, instanceRelationshipTypeId);
        relation.type = Entity.INSTANCE_RELATIONSHIP;
        return relation;
    }

    public boolean isRelationToChild () {
        return instanceId.equals(getSuperInstanceId());
    }

    public String getSubInstanceId () {
        return jsonRecord.getString(SUB_INSTANCE_ID);
    }

    public String getSuperInstanceId() {
        return jsonRecord.getString(SUPER_INSTANCE_ID);
    }

    public String getInstanceRelationshipTypeId () {
        return jsonRecord.getString(INSTANCE_RELATIONSHIP_TYPE_ID);
    }

    public void skipDependants() {
        // has none
    }

    @Override
    public boolean equals (Object o) {
        if (o instanceof InstanceRelationship) {
            InstanceRelationship other = (InstanceRelationship) o;
            return (other.getSubInstanceId() != null && other.getSubInstanceId().equals(this.getSubInstanceId()) &&
                    other.getSuperInstanceId() != null && other.getSuperInstanceId().equals(this.getSuperInstanceId()) &&
                    other.getInstanceRelationshipTypeId().equals(this.getInstanceRelationshipTypeId()));
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
        str.append("// Sub: ").append(jsonRecord.getString(SUB_INSTANCE_ID))
           .append(" Super: ").append(jsonRecord.getString(SUPER_INSTANCE_ID))
           .append(" TypeId: ").append(jsonRecord.getString(INSTANCE_RELATIONSHIP_TYPE_ID));
        return str.toString();
    }

}
