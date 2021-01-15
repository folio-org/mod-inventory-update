package org.folio.inventoryupdate.entities;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.HridQuery;
import org.folio.inventoryupdate.InventoryQuery;
import org.folio.inventoryupdate.InventoryStorage;
import org.folio.okapi.common.OkapiClient;

import java.util.UUID;

import static org.folio.inventoryupdate.entities.InventoryRecordSet.HRID;

public abstract class InstanceToInstanceRelation extends InventoryRecord {
    public static final String PROVISIONAL_INSTANCE = "provisionalInstance";

    protected Instance provisionalInstance = null;

    public void setProvisionalInstance (Instance provisionalInstance) {
        this.provisionalInstance = provisionalInstance;
    }

    public boolean requiresProvisionalInstanceToBeCreated () {
        return provisionalInstance != null;
    }

    public Instance getProvisionalInstance () {
        return provisionalInstance;
    }

    /**
     * Create a temporary Instance to create a relationship to.
     * @param hrid human readable ID of the temporary Instance to create
     * @param provisionalInstanceJson other properties of the Instance to create
     * @return Instance POJO
     */
    protected static Instance prepareProvisionalInstance (String hrid, JsonObject provisionalInstanceJson) {
        JsonObject json = new JsonObject(provisionalInstanceJson.toString());
        if (! json.containsKey(HRID)) {
            json.put(HRID, hrid);
        }
        if (! json.containsKey(InstanceToInstanceRelations.ID)) {
            json.put(InstanceToInstanceRelations.ID, UUID.randomUUID().toString());
        }
        return new Instance(json);
    }

}
