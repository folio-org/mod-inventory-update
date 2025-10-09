package org.folio.inventoryimport.foliodata;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.OkapiClient;

import java.nio.charset.StandardCharsets;

public class InventoryUpdateClient {
    public static final String INVENTORY_UPSERT_PATH = "/inventory-batch-upsert-hrid";
    public static final String INVENTORY_DELETION_PATH = "/inventory-upsert-hrid";
    private final OkapiClient okapiClient;
    private static InventoryUpdateClient inventoryUpdateClient;
    public static final Logger logger = LogManager.getLogger("InventoryUpdateClient");


    private InventoryUpdateClient (RoutingContext routingContext) {
        okapiClient = Folio.okapiClient(routingContext);
    }

    public static InventoryUpdateClient getClient (RoutingContext routingContext) {
        if (inventoryUpdateClient == null) {
            inventoryUpdateClient = new InventoryUpdateClient(routingContext);
        }
        return inventoryUpdateClient;
    }

    public Future<UpdateResponse> inventoryUpsert (JsonObject recordSets) {
        Buffer records = Buffer.buffer(recordSets.encode().getBytes(StandardCharsets.UTF_8));
        okapiClient.disableInfoLog();
        return okapiClient
                .request(HttpMethod.PUT,INVENTORY_UPSERT_PATH, records)
                .map(JsonObject::new)
                .compose(responseJson -> {
                    if (okapiClient.getStatusCode() == 207) {
                        logger.error(responseJson.getJsonArray("errors").getJsonObject(0).getString("shortMessage"));
                    }
                    return Future.succeededFuture(new UpdateResponse(okapiClient.getStatusCode(), responseJson));
                })
                .onFailure(e -> logger.error("Could not upsert batch: " + e.getMessage()));
    }

    public Future<UpdateResponse> inventoryDeletion (JsonObject record) {
        Buffer buffer = Buffer.buffer(record.encode().getBytes(StandardCharsets.UTF_8));

        return okapiClient
                .request(HttpMethod.DELETE, INVENTORY_DELETION_PATH, buffer)
                .map(JsonObject::new)
                .compose(responseJson -> {
                    if (okapiClient.getStatusCode() == 404) {
                        // TODO: handle this
                        logger.warn("Inventory records set to delete not found");
                    }
                    return Future.succeededFuture(new UpdateResponse(okapiClient.getStatusCode(), responseJson));
                })
                .onFailure(e -> logger.error("Could not delete inventory record set: " + e.getMessage()));
    }

    public record UpdateResponse(int statusCode, JsonObject json) {
        public JsonObject getMetrics() {
            if (json != null) {
                return json().getJsonObject("metrics");
            } else {
                return new JsonObject();
            }
        }

        public JsonArray getErrors() {
            if (json != null && json.containsKey("errors")) {
                return json.getJsonArray("errors");
            } else {
                return new JsonArray();
            }
        }
    }
}
