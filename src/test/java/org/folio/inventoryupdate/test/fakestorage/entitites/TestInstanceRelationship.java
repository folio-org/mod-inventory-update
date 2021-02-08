package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonObject;

public class TestInstanceRelationship extends InventoryRecord {
    public TestInstanceRelationship(JsonObject record) {
        super(record);
    }

    public TestInstanceRelationship() {
        super();
    }

}
