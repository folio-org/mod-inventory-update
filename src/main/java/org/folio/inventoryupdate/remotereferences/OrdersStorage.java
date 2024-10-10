package org.folio.inventoryupdate.remotereferences;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.okapi.common.OkapiClient;

public class OrdersStorage {

  private final OrdersStorageConfiguration config;
  static class OrdersStorageConfiguration {
    public OrdersStorageConfiguration () {
      // SC
    }
    public String ordersStoragePath() {
      return "/orders-storage/po-lines";
    }
    public String purchaseOrderLines() {
      return "poLines";
    }
  }

  public OrdersStorage() {
    config = new OrdersStorageConfiguration();
  }

  public Future<JsonArray> lookupPurchaseOrderLinesByInstanceId(OkapiClient okapiClient, String instanceId) {
    Promise<JsonArray> promise = Promise.promise();
    okapiClient.get(config.ordersStoragePath() + "?query=instanceId==" + instanceId)
        .onComplete(response -> {
          if (response.succeeded()) {
            promise.complete(new JsonObject(response.result()).getJsonArray(config.purchaseOrderLines()));
          } else {
            promise.complete(new JsonArray());
          }
        });
    return promise.future();
  }
}

