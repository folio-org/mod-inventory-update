/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventoryupdate;

import java.lang.management.ManagementFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

public class MainVerticle extends AbstractVerticle {

  public final static String INVENTORY_UPSERT_HRID_PATH = "/inventory-upsert-hrid";
  public final static String SHARED_INVENTORY_UPSERT_MATCHKEY_PATH = "/shared-inventory-upsert-matchkey";

  // Old API
  public final static String INSTANCE_MATCH_PATH = "/instance-storage-match/instances";

  private final Logger logger = LoggerFactory.getLogger("inventory-update");
  private final InventoryUpdateService matchService = new InventoryUpdateService();

  @Override
  public void start(Promise<Void> promise)  {
    final int port = Integer.parseInt(System.getProperty("port", "8080"));
    logger.info("Starting Inventory Update service "
      + ManagementFactory.getRuntimeMXBean().getName()
      + " on port " + port);

    Router router = Router.router(vertx);
    router.put("/*").handler(BodyHandler.create()); // Tell vertx we want the whole PUT body in the handler
    router.put(INSTANCE_MATCH_PATH).handler(matchService::handleInstanceMatching); // old API

    router.put(INVENTORY_UPSERT_HRID_PATH).handler(matchService::handleInventoryUpsertByHrid);
    router.put(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH).handler(matchService::handleSharedInventoryUpsertByMatchkey);

    vertx.createHttpServer()
      .requestHandler(router)
      .listen(port, result -> {
        if (result.succeeded()) {
          logger.debug("Succeeded in starting the listener for Inventory match/upsert service");
          promise.complete();
        } else {
          logger.error("Inventory match/upsert service failed: " + result.cause().getMessage());
          promise.fail(result.cause());
        }
      });
  }
}
