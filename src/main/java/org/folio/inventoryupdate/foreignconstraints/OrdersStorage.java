package org.folio.inventoryupdate.foreignconstraints;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.okapi.common.OkapiClient;

public class OrdersStorage {

  private static final String ORDER_LINES_STORAGE_PATH = "/orders-storage/po-lines";
  private static final String PURCHASE_ORDER_LINES = "poLines";
  private static final Logger logger = LoggerFactory.getLogger("inventory-update");

  public static Future<JsonArray> lookupPurchaseOrderLines (OkapiClient okapiClient, String instanceId) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(ORDER_LINES_STORAGE_PATH + "?query=instanceId==" + instanceId)
        .onComplete(response -> {
          if (response.succeeded()) {
            logger.info("ID-NE: got " + response.result());
            promise.complete(new JsonObject(response.result()).getJsonArray(PURCHASE_ORDER_LINES));
          } else {
            if (!response.cause().getMessage().equalsIgnoreCase("404: No suitable module found for path and tenant")) {
              logger.error("Could not look up purchase order lines at " + ORDER_LINES_STORAGE_PATH + ": " + response.cause().getMessage());
            }
            promise.complete(new JsonArray());
          }
        });
    return promise.future();
  }

}

