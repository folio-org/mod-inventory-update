package org.folio.inventoryupdate;

import java.lang.management.ManagementFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

public class MainVerticle extends AbstractVerticle {

  public final static String HEALTH_CHECK = "/admin/health";
  public final static String INVENTORY_UPSERT_HRID_PATH = "/inventory-upsert-hrid";
  public final static String SHARED_INVENTORY_UPSERT_MATCHKEY_PATH = "/shared-inventory-upsert-matchkey";

  public final static String FETCH_INVENTORY_RECORD_SETS_ID_PATH = INVENTORY_UPSERT_HRID_PATH+"/fetch/:id";
  public final static String FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH = SHARED_INVENTORY_UPSERT_MATCHKEY_PATH+"/fetch/:id";

  private final Logger logger = LoggerFactory.getLogger("inventory-update");
  private final InventoryUpdateService upsertService = new InventoryUpdateService();
  private final InventoryFetchService fetchService = new InventoryFetchService();

  @Override
  public void start(Promise<Void> promise)  {
    final int port = Integer.parseInt(System.getProperty("port", "8080"));
    logger.info("Starting Inventory Update service "
      + ManagementFactory.getRuntimeMXBean().getName()
      + " on port " + port);

    Router router = Router.router(vertx);
    router.put("/*").handler(BodyHandler.create()); // Tell vertx we want the whole PUT body in the handler
    router.delete("/*").handler(BodyHandler.create()); // Tell vertx we want the whole DELETE body in the handler

    router.put(INVENTORY_UPSERT_HRID_PATH).handler(upsertService::handleInventoryUpsertByHRID);
    router.put(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH).handler(upsertService::handleSharedInventoryUpsertByMatchKey);
    router.delete(INVENTORY_UPSERT_HRID_PATH).handler(upsertService::handleInventoryRecordSetDeleteByHRID);
    router.delete(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH).handler(upsertService::handleSharedInventoryRecordSetDeleteByIdentifiers);

    router.get( FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH ).handler(fetchService::handleSharedInventoryRecordSetFetch );
    router.get( FETCH_INVENTORY_RECORD_SETS_ID_PATH ).handler(fetchService::handleInventoryRecordSetFetchHrid );

    router.route("/apidocs/*").handler(StaticHandler.create("apidocs"));
    router.route(HEALTH_CHECK).handler(upsertService::handleHealthCheck);
    router.route("/*").handler(upsertService::handleUnrecognizedPath);

    vertx.createHttpServer()
      .requestHandler(router)
      .listen(port, result -> {
        if (result.succeeded()) {
          logger.debug("Succeeded in starting the listener for Inventory match/upsert service");
          promise.complete();
        } else {
          logger.error("Inventory upsert service failed: " + result.cause().getMessage());
          promise.fail(result.cause());
        }
      });
  }
}
