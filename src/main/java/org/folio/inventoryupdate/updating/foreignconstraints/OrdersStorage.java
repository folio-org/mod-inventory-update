package org.folio.inventoryupdate.updating.foreignconstraints;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.okapi.common.OkapiClient;

public class OrdersStorage {

  private static final String ORDER_LINES_STORAGE_PATH = "/orders-storage/po-lines";
  private static final String PURCHASE_ORDER_LINES = "poLines";

  private OrdersStorage () {
    throw new IllegalStateException("Utility class");
  }

  public static Future<JsonArray> lookupPurchaseOrderLines (OkapiClient okapiClient, String instanceId) {
    return okapiClient.get(ORDER_LINES_STORAGE_PATH + "?query=instanceId==" + instanceId)
        .map(body -> new JsonObject(body).getJsonArray(PURCHASE_ORDER_LINES))
        .recover(e -> Future.succeededFuture(new JsonArray()));
  }
}
