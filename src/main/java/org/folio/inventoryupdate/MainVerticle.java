/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventoryupdate;

import java.lang.management.ManagementFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

public class MainVerticle extends AbstractVerticle {

  private final Logger logger = LoggerFactory.getLogger("inventory-matcher");
  private final MatchService matchService = new MatchService();

  @Override
  public void start(Future<Void> fut)  {
    final int port = Integer.parseInt(System.getProperty("port", "8080"));
    logger.info("Starting Inventory match service "
      + ManagementFactory.getRuntimeMXBean().getName()
      + " on port " + port);

    Router router = Router.router(vertx);
    router.put("/*").handler(BodyHandler.create()); // Tell vertx we want the whole PUT body in the handler
    router.put(MatchService.INSTANCE_MATCH_PATH).handler(matchService::handleInstanceMatching); // old API

    router.put(MatchService.INVENTORY_UPSERT_HRID_PATH).handler(matchService::handleInventoryUpsertByHrid);
    router.put(MatchService.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH).handler(matchService::handleSharedInventoryUpsertByMatchkey);

    vertx.createHttpServer()
      .requestHandler(router::accept)
      .listen(port, result -> {
        if (result.succeeded()) {
          logger.debug("Succeeded in starting the listener for Inventory match/upsert service");
          fut.complete();
        } else {
          logger.error("Inventory match/upsert service failed: " + result.cause().getMessage());
          fut.fail(result.cause());
        }
      });
  }
}
