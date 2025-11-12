package org.folio.inventoryupdate.importing.test.fakestorage.entities;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.importing.test.fakestorage.FolioApiRecord;

public class InputInstanceTitleSuccession extends FolioApiRecord {

    public static final String PRECEDING_INSTANCE_ID = "precedingInstanceId";
    public static final String SUCCEEDING_INSTANCE_ID = "succeedingInstanceId";
    public static final String INSTANCE_IDENTIFIER = "instanceIdentifier";

    public InputInstanceTitleSuccession setPrecedingInstanceId (String id) {
        recordJson.put(PRECEDING_INSTANCE_ID, id);
        return this;
    }

    public InputInstanceTitleSuccession setSucceedingInstanceId (String id) {
        recordJson.put(SUCCEEDING_INSTANCE_ID, id);
        return this;
    }

    public InputInstanceTitleSuccession setInstanceIdentifierHrid(String hrid) {
        recordJson.put(INSTANCE_IDENTIFIER, new JsonObject().put("hrid", hrid));
        return this;
    }

}
