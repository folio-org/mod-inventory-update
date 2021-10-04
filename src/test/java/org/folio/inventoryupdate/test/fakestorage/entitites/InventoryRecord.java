package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

public class InventoryRecord {

    public static final String ID = "id";
    public static final String VERSION = "_version";
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

    public InventoryRecord setFirstVersion () {
        recordJson.put(VERSION,1);
        return this;
    }

    public Integer getVersion () {
        return recordJson.containsKey( VERSION ) ? recordJson.getInteger( VERSION ) : 0;
    }

    public InventoryRecord setVersion (Integer version) {
        recordJson.put( VERSION,  version);
        return this;
    }

    public boolean match(String query) {
        String trimmed = query.replace("(","").replace(")", "");
        String[] orSections = trimmed.split(" or ");
        logger.debug("orSections: " + (orSections.length>1 ? orSections[0] + ", " + orSections[1] : orSections[0]));

        for (int i=0; i<orSections.length; i++) {
            if (orSections[i].contains("@identifierTypeId")) {
                return matchIdentifierQuery(orSections[i]);
            }
            String[] queryParts = orSections[i].split("==");
            logger.debug("query: " +query);
            logger.debug("queryParts[0]: " + queryParts[0]);
            String key = queryParts[0];
            String value = queryParts.length > 1 ?  queryParts[1].replace("\"", "") : "";
            logger.debug("key: "+key);
            logger.debug("value: "+value);
            logger.debug("recordJson.getString(key): " + recordJson.getString(key));
            logger.debug("Query parameter [" + value + "] matches record property [" + key + "("+ recordJson.getString(key)+")] ?: "
                        +(recordJson.getString(key) != null && recordJson.getString(key).equals(value)));
            if  (recordJson.getString(key) != null && recordJson.getString(key).equals(value)) {
                return true;
            }
        }
        return false;
    }

    public boolean matchIdentifierQuery (String identifierQuery) {
        String[] sections = identifierQuery.split(" ");
        String[] typeCriterion = sections[1].split("=");
        String identifierType = typeCriterion[2].replace("\"","");
        String value = sections[2].replace("\"","");
        logger.debug("Identifier query received, identifierType: [" + identifierType + "], value: [" + value + "]");
        if (recordJson.containsKey("identifiers")) {
            for (Object o : recordJson.getJsonArray("identifiers")) {
               JsonObject identifier = (JsonObject) o;
               if (identifier.getString("identifierTypeId").equals(identifierType)
                       &&
                   identifier.getString("value").equals(value)) {
                   return true;
               }
            }
        }
        return false;
    }

}
