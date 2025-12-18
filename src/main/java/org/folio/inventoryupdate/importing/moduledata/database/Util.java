package org.folio.inventoryupdate.importing.moduledata.database;

import io.vertx.core.json.JsonObject;
import java.util.UUID;

public final class Util {

  private Util() {
    throw new UnsupportedOperationException("Utility class");
  }

  public static UUID getUuid(JsonObject json, String propertyName, UUID def) {
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
