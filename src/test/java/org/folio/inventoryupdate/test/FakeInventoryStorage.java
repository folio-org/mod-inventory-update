package org.folio.inventoryupdate.test;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import org.folio.inventoryupdate.MatchKey;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

public class FakeInventoryStorage {
  public final static int PORT_INVENTORY_STORAGE = 9030;
  public final static String INSTANCE_STORAGE_PATH = "/instance-storage/instances";
  public static final String INSTANCE_RELATIONSHIP_STORAGE_PATH = "/instance-storage/instance-relationships";
  public static final String PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH = "/preceding-succeeding-titles";
  public static final String HOLDINGS_STORAGE_PATH = "/holdings-storage/holdings";
  public static final String ITEM_STORAGE_PATH = "/item-storage/items";
  public static final String LOCATION_STORAGE_PATH = "/locations";

  public InstanceStorage instanceStorage = new InstanceStorage();
  public PrecedingSucceedingStorage precedingSucceedingStorage = new PrecedingSucceedingStorage();

  private final Logger logger = LoggerFactory.getLogger("fake-inventory-storage");

  public FakeInventoryStorage (Vertx vertx, TestContext testContext, Async async) {
    Router router = Router.router(vertx);
    router.get(INSTANCE_STORAGE_PATH).handler(instanceStorage::getRecordsByQuery);
    router.get(INSTANCE_STORAGE_PATH +"/:id").handler(instanceStorage::getRecordById);
    router.get(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH).handler(precedingSucceedingStorage::getRecordsByQuery);
    router.get(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH + "/:id").handler(precedingSucceedingStorage::getRecordById);
    router.post("/*").handler(BodyHandler.create());
    router.post(INSTANCE_STORAGE_PATH).handler(instanceStorage::createInstance);
    router.post(PRECEDING_SUCCEEDING_TITLE_STORAGE_PATH).handler(precedingSucceedingStorage::createPrecedingSucceedingTitle);
    router.put("/*").handler(BodyHandler.create());
    router.put(INSTANCE_STORAGE_PATH +"/:id").handler(instanceStorage::updateInstance);

    HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
    vertx.createHttpServer(so)
      .requestHandler(router::accept)
      .listen(
        PORT_INVENTORY_STORAGE,
        result -> {
          if (result.failed()) {
            testContext.fail(result.cause());
          }
          async.complete();
        }
      );
  }

}
