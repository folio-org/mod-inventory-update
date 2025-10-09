package org.folio.inventoryimport.moduledata;

import io.vertx.core.json.JsonObject;

import java.util.UUID;

public class Util {

    public static UUID getUUID (JsonObject json, String propertyName, UUID def) {
        if (json.containsKey(propertyName)) {
            try {
                return UUID.fromString(json.getString(propertyName));
            } catch (IllegalArgumentException iae) {
                return def;
            }
        } else {
            return def;
        }
    }
}
