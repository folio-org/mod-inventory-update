package org.folio.inventoryupdate.remotereferences;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import org.folio.okapi.common.OkapiClient;

public class Orders {

  private final OrdersConfiguration ordersConfiguration;
  static class OrdersConfiguration {
    public OrdersConfiguration() {
      // SC
    }
    public String ordersPath() {
      return "/orders/order-lines";
    }
  }
  private static final Logger logger = LoggerFactory.getLogger("inventory-update");

  public Orders() {
    ordersConfiguration = new OrdersConfiguration();
  }

  public Future<Void> patchOrderLine(OkapiClient okapiClient, String purchaseOrderLineId, JsonObject patchBody) {
    Promise<Void> promise = Promise.promise();
    okapiClient.request(HttpMethod.PATCH, ordersConfiguration.ordersPath() + "/" + purchaseOrderLineId, patchBody.toString())
        .onComplete(response -> {
          if (response.failed()) {
            logger.error("Problem patching order line: " + response.cause());
          }
          promise.complete();
        });
    return promise.future();
  }
}
