package org.folio.inventoryupdate.importing.foliodata;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.updating.DeletePlan;
import org.folio.inventoryupdate.updating.DeletePlanAllHRIDs;
import org.folio.inventoryupdate.updating.InventoryQuery;
import org.folio.inventoryupdate.updating.QueryByHrid;
import org.folio.inventoryupdate.updating.UpdatePlanAllHRIDs;
import org.folio.inventoryupdate.updating.service.HandlersUpdating;

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
    InventoryQuery queryByInstanceHrid = new QueryByHrid(theRecord.getString("hrid"));
    DeletePlan deletePlan = DeletePlanAllHRIDs.getDeletionPlan(queryByInstanceHrid);
    InternalInventoryDeleteRequest deleteRequest = new InternalInventoryDeleteRequest(vertx, routingContext, theRecord);
    return deletePlan.runDeletionPlan(deleteRequest).compose(outcome -> {
      JsonObject outcomeJson = outcome.getJson();
      if (outcome.getStatusCode() == 404) {
        if (outcome.getError() != null) {
          outcomeJson.put("errors", new JsonArray().add(new JsonObject().put("message", outcome.getError().getMessageAsString())));
        } else {
          outcomeJson.put("errors", new JsonArray().add(new JsonObject().put("message", "No message to provide")));
        }
      }
      return Future.succeededFuture(
              new UpdateResponse(outcome.getStatusCode(), outcomeJson));

        })
        .onFailure(e -> logger.error("Could not perform delete request {}", e.getMessage()));
  }

  @Override
  public Future<UpdateResponse> inventoryUpsert(JsonObject recordSets) {
    InternalInventoryUpdateRequest req = new InternalInventoryUpdateRequest(vertx, routingContext, recordSets);
    HandlersUpdating upsertMethods = new HandlersUpdating();
    return upsertMethods.doBatchUpsert(req, new UpdatePlanAllHRIDs()).compose(
        outcome -> {
          if (outcome.getStatusCode() == 207) {
            logger.warn("Upsert issue: {}", (outcome.getErrorResponse() != null ? outcome.getErrorResponse().getShortMessage() : ""));
          }
          return Future.succeededFuture(new UpdateResponse(outcome.getStatusCode(), outcome.getJson()));
        })
        .onFailure(e -> logger.error("Could not upsert batch: {}", e.getMessage()));
  }
}
