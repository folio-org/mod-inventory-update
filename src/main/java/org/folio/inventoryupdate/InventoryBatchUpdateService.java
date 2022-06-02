package org.folio.inventoryupdate;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.InventoryRecordSet;
import org.folio.inventoryupdate.entities.Repository;
import org.folio.okapi.common.OkapiClient;

import static org.folio.okapi.common.HttpResponse.responseError;
import static org.folio.okapi.common.HttpResponse.responseJson;

public class InventoryBatchUpdateService {

  private final Logger logger = LoggerFactory.getLogger("inventory-update");

  public void handleInventoryBatchUpsertByHrid(RoutingContext routingCtx) {
    OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingCtx);
    long start = System.currentTimeMillis();
    JsonObject requestJson = routingCtx.getBodyAsJson();
    if (requestJson.containsKey("inventoryRecordSets")) {
      Repository repository = new Repository();
      repository
              .setIncomingRecordSets(requestJson.getJsonArray("inventoryRecordSets"))
              .buildRepositoryFromStorage(routingCtx)
              .onComplete(Void -> {
                JsonObject response = new JsonObject();
                response.put("existingRecordSets", new JsonArray());
                for (InventoryRecordSet recordSet : repository.getExistingRecordSets()) {
                  response.getJsonArray("existingRecordSets").add(recordSet.asJson());
                }
                response.put("updatingRecordSets", new JsonArray());
                for (InventoryRecordSet recordSet : repository.getIncomingRecordSets()) {
                  response.getJsonArray("updatingRecordSets").add(recordSet.asJson());
                }
                responseJson(routingCtx, 200).end(response.encodePrettily());

              });
    } else {
      responseError(routingCtx, 400,
              "InventoryBatchUpdateService expected but did not seem to receive " +
              "a batch of Inventory record sets");
    }
  }

}
