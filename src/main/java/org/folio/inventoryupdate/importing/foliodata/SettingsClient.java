package org.folio.inventoryupdate.importing.foliodata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class SettingsClient {
  public static final String SETTINGS_PATH = "/settings/entries";
  public static final String RECORDS = "items";

  private static final Logger logger = LogManager.getLogger(SettingsClient.class);

  private SettingsClient() {
    throw new UnsupportedOperationException("Utility class");
  }

  public static Future<String> getStringValue(RoutingContext routingContext, String scope, String key) {
    String query = "scope==\"" + scope + "\" and key==\"" + key + "\"";
    return Folio.okapiClient(routingContext).get(SETTINGS_PATH
            + "?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8))
        .map(response ->
            new JsonObject(response).getJsonArray(RECORDS).isEmpty() ? null :
                new JsonObject(response).getJsonArray(RECORDS).getJsonObject(0).getString("value"))
        .onFailure(e ->
            logger.error("Could not obtain settings by scope {} and key {}: {}",
                scope, key, e.getMessage()));
  }
}
