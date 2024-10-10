package org.folio.inventoryupdate.remotereferences;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import org.folio.okapi.common.OkapiClient;

public class Orders {
  private static final Logger logger = LoggerFactory.getLogger("inventory-update");
  private static final String ORDER_LINES_PATH = "/orders/order-lines";

  public Orders() {}

  public static Future<Void> patchOrderLine(OkapiClient okapiClient, String purchaseOrderLineId, JsonObject patchBody) {
    Promise<Void> promise = Promise.promise();
    okapiClient.request(HttpMethod.PATCH, ORDER_LINES_PATH + "/" + purchaseOrderLineId, patchBody.toString())
        .onComplete(response -> {
          if (response.failed()) {
            logger.error("Problem patching order line: " + response.cause());
          }
          promise.complete();
        });
    return promise.future();
  }


}
