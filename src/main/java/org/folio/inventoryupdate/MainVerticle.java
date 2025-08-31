package org.folio.inventoryupdate;

import java.lang.management.ManagementFactory;

import io.vertx.core.Future;
import io.vertx.core.VerticleBase;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MainVerticle extends VerticleBase {

  public static final String HEALTH_CHECK = "/admin/health";
  public static final String INVENTORY_UPSERT_HRID_PATH = "/inventory-upsert-hrid";
  public static final String INVENTORY_BATCH_UPSERT_HRID_PATH = "/inventory-batch-upsert-hrid";

  public static final String SHARED_INVENTORY_BATCH_UPSERT_MATCHKEY_PATH = "/shared-inventory-batch-upsert-matchkey";
  public static final String SHARED_INVENTORY_UPSERT_MATCHKEY_PATH = "/shared-inventory-upsert-matchkey";

  public static final String FETCH_INVENTORY_RECORD_SETS_ID_PATH = INVENTORY_UPSERT_HRID_PATH+"/fetch/:id";
  public static final String FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH = SHARED_INVENTORY_UPSERT_MATCHKEY_PATH+"/fetch/:id";

  private final Logger logger = LogManager.getLogger("inventory-update");
  private final InventoryUpdateService upsertService = new InventoryUpdateService();
  private final InventoryFetchService fetchService = new InventoryFetchService();

    @Override
  public Future<?> start()  {
    final int port = Integer.parseInt(System.getProperty("port", "8080"));
    logger.info("Starting Inventory Update service {} on port {}",
        ManagementFactory.getRuntimeMXBean().getName(), port);

    Router router = Router.router(vertx);
    router.put("/*").handler(BodyHandler.create()); // Tell vertx we want the whole PUT body in the handler
    router.delete("/*").handler(BodyHandler.create()); // Tell vertx we want the whole DELETE body in the handler

    router.put(INVENTORY_UPSERT_HRID_PATH).handler(upsertService::handleInventoryUpsertByHRID);
    router.put(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH).handler(upsertService::handleSharedInventoryUpsertByMatchKey);
    router.delete(INVENTORY_UPSERT_HRID_PATH).handler(upsertService::handleInventoryRecordSetDeleteByHRID);
    router.delete(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH).handler(upsertService::handleSharedInventoryRecordSetDeleteByIdentifiers);

    router.put(INVENTORY_BATCH_UPSERT_HRID_PATH).handler(upsertService::handleInventoryUpsertByHRIDBatch);
    router.put(SHARED_INVENTORY_BATCH_UPSERT_MATCHKEY_PATH).handler(upsertService::handleSharedInventoryUpsertByMatchKeyBatch);

    router.get( FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH ).handler(fetchService::handleSharedInventoryRecordSetFetch );
    router.get( FETCH_INVENTORY_RECORD_SETS_ID_PATH ).handler(fetchService::handleInventoryRecordSetFetchHrid );

    router.route("/apidocs/*").handler(StaticHandler.create("apidocs"));
    router.route(HEALTH_CHECK).handler(upsertService::handleHealthCheck);
    router.route("/*").handler(upsertService::handleUnrecognizedPath);

    HttpServer server = vertx.createHttpServer()
        .requestHandler(router);
    return server.listen(port).onComplete(httpserver -> {
      if (httpserver.succeeded()) {
        logger.debug("Succeeded in starting the listener for Inventory match/upsert service");
      } else {
        logger.error("Inventory upsert service failed: {}", httpserver.cause().getMessage());
      }
    });
  }
}
