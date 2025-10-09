package org.folio.inventoryimport.foliodata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class SettingsClient {
    public static final String SETTINGS_PATH = "/settings/entries";
    public static final String RECORDS = "items";

    protected static final Logger logger =
            LogManager.getLogger(SettingsClient.class);

    public static Future<String> getStringValue(RoutingContext routingContext, String scope, String key) {
        String query = "scope==\"" + scope + "\" and key==\"" + key + "\"";
        return Folio.okapiClient(routingContext).get(SETTINGS_PATH +
                        "?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8))
                .map(response ->
                        new JsonObject(response).getJsonArray(RECORDS).getJsonObject(0).getString("value"))
                .onFailure(e ->
                        logger.error("Could not obtain settings by scope " + scope
                                + " and key " + key + ": " + e.getMessage()));
    }

}
