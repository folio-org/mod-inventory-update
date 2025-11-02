package org.folio.inventoryupdate.importing.foliodata;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.updating.UpdatePlan;
import org.folio.inventoryupdate.updating.UpdatePlanAllHRIDs;

public class InternalInventoryUpdateClient extends InventoryUpdateClient {

  public static final Logger logger = LogManager.getLogger("InventoryUpdateClient");

  private final Vertx vertx;
  private final RoutingContext routingContext;

  public InternalInventoryUpdateClient (Vertx vertx, RoutingContext routingContext) {
    this.vertx = vertx;
    this.routingContext = routingContext;
  }

  @Override
  public Future<UpdateResponse> inventoryDeletion(JsonObject theRecord) {
    return null;
  }

  @Override
  public Future<UpdateResponse> inventoryUpsert(JsonObject recordSets) {
    JsonArray inventoryRecordSets = recordSets.getJsonArray("inventoryRecordSets");
    InternalInventoryUpdateRequest req = new InternalInventoryUpdateRequest(vertx, routingContext, recordSets);
    UpdatePlan plan = new UpdatePlanAllHRIDs();
    return plan.upsertBatch(req, inventoryRecordSets).compose(
        outcome -> {
          if (outcome.getStatusCode() == 207) {
            logger.error(outcome.getError().getShortMessage());
          }
          return Future.succeededFuture(new UpdateResponse(outcome.getStatusCode(), outcome.getJson()));
        })
        .onFailure(e -> logger.error("Could not upsert batch: {}", e.getMessage()));
  }

}
