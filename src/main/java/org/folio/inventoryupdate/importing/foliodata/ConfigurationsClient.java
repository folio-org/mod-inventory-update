package org.folio.inventoryupdate.importing.foliodata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ConfigurationsClient {

  public static final String CONFIGURATIONS_PATH = "/configurations/entries";
  public static final String RECORDS = "configs";

  private static final Logger logger = LogManager.getLogger(ConfigurationsClient.class);

  private ConfigurationsClient() {
    throw new IllegalStateException("Utility class");
  }

  public static Future<String> getStringValue(RoutingContext routingContext, String moduleName, String configName) {
    String query = "module==\"" + moduleName + "\" and configName==\"" + configName + "\" and enabled=true";
    return Folio.okapiClient(routingContext).get(CONFIGURATIONS_PATH
            + "?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8))
        .map(response ->
            new JsonObject(response).getJsonArray(RECORDS).getJsonObject(0).getString("value"))
        .onFailure(e -> logger.error(
            "Could not obtain settings by module {} and config {}: {}",
            moduleName, configName, e.getMessage()));
  }
}
