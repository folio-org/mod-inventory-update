package org.folio.inventoryupdate.remotereferences;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.okapi.common.OkapiClient;

public class OrdersStorage {

  private static final String ORDER_LINES_STORAGE_PATH = "/orders-storage/po-lines";
  private static final String PURCHASE_ORDER_LINES = "poLines";

  public static Future<JsonArray> lookupPurchaseOrderLinesByInstanceId(OkapiClient okapiClient, String instanceId) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(ORDER_LINES_STORAGE_PATH + "?query=instanceId==" + instanceId)
        .onComplete(response -> {
          if (response.succeeded()) {
            promise.complete(new JsonObject(response.result()).getJsonArray(PURCHASE_ORDER_LINES));
          } else {
            promise.complete(new JsonArray());
          }
        });
    return promise.future();
  }
}
