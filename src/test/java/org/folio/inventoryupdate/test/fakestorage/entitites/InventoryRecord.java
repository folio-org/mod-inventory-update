package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

public abstract class InventoryRecord {

    public static String ID = "id";
    protected JsonObject recordJson;

    private Logger logger = io.vertx.core.impl.logging.LoggerFactory.getLogger("InventoryRecord");

    public InventoryRecord() {
        recordJson = new JsonObject();
    }

    public InventoryRecord(JsonObject record) {
        recordJson = record;
    }

    public JsonObject getJson() {
        return recordJson;
    }

    public InventoryRecord setId (String id) {
        recordJson.put(ID, id);
        return this;
    }

    public boolean hasId() {
        return recordJson.getString(ID) != null;
    }

    public InventoryRecord generateId () {
        recordJson.put(ID,UUID.randomUUID().toString());
        return this;
    }

    public String getId () {
        return recordJson.getString(ID);
    }

    public boolean match(String query) {
        String trimmed = query.replace("(","").replace(")", "");
        String[] orSections = trimmed.split(" or ");
        logger.debug("orSections: " + (orSections.length>1 ? orSections[0] + ", " + orSections[1] : orSections[0]));

        for (int i=0; i<orSections.length; i++) {
            String[] queryParts = orSections[i].split("==");
            logger.debug("query: " +query);
            logger.debug("queryParts[0]: " + queryParts[0]);
            String key = queryParts[0];
            String value = queryParts.length > 1 ?  queryParts[1].replace("\"", "") : "";
            logger.debug("key: "+key);
            logger.debug("value: "+value);
            logger.debug("recordJson.getString(key): " + recordJson.getString(key));
            if (logger.isDebugEnabled()) {
                logger.debug("Query parameter [" + value + "] matches record property [" + key + "("+ recordJson.getString(key)+")] ?: "
                        +(recordJson.getString(key) != null && recordJson.getString(key).equals(value)));
            }
            if  (recordJson.getString(key) != null && recordJson.getString(key).equals(value)) {
                return true;
            }
        }
        return false;
    }

}
