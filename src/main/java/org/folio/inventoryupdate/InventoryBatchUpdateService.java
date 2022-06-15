package org.folio.inventoryupdate;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.InventoryRecordSet;
import org.folio.inventoryupdate.entities.Repository;
import org.folio.inventoryupdate.entities.RepositoryByHrids;
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
      JsonArray inventoryRecordSets = requestJson.getJsonArray("inventoryRecordSets");
      UpdatePlanAllHRIDs plan = UpdatePlanAllHRIDs.getUpsertPlan();
      RequestValidation validations = InventoryUpdateService.validateIncomingRecordSets (plan, inventoryRecordSets);

      if (validations.passed()) {
        plan
                .setIncomingRecordSets(inventoryRecordSets)
                .buildRepositoryFromStorage(routingCtx).onComplete(
                        result -> {
                          if (result.succeeded()) {
                            plan.planInventoryUpdatesUsingRepository()
                                    .doInventoryUpdatesUsingRepository(
                                            InventoryStorage.getOkapiClient(routingCtx)).onComplete(inventoryUpdated -> {
                                      if (inventoryUpdated.succeeded()) {
                                        JsonObject pushedRecordSetWithStats = plan.getUpdatingRecordSetJsonFromRepository();
                                        pushedRecordSetWithStats.put("metrics", plan.getUpdateStatsFromRepository());
                                        responseJson(routingCtx, 200).end(pushedRecordSetWithStats.encodePrettily());
                                      } else {
                                        JsonObject pushedRecordSetWithStats = plan.getUpdatingRecordSetJsonFromRepository();
                                        pushedRecordSetWithStats.put("metrics", plan.getUpdateStatsFromRepository());
                                        pushedRecordSetWithStats.put("errors", plan.getErrorsUsingRepository());
                                        responseJson(routingCtx, 422).end(pushedRecordSetWithStats.encodePrettily());
                                      }
                                    });
                          } else {
                            responseJson(routingCtx, 422).end("{}");
                          }
                        });
      } else {
        responseJson(routingCtx, 422).end("The incoming record set(s) had errors and were not processed " + validations);
      }
    } else {
      responseError(routingCtx, 400,
              "InventoryBatchUpdateService expected but did not seem to receive " +
              "a batch of Inventory record sets");
    }
  }

}
