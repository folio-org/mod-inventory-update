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

  // Property keys, JSON responses
  public static final String HOLDINGS_RECORDS = "holdingsRecords";
  public static final String ITEMS = "items";
  public static final String INSTANCE_RELATIONSHIPS = "instanceRelationships";
  public static final String PRECEDING_SUCCEEDING_TITLES = "precedingSucceedingTitles";
  public static final String LOCATIONS = "locations";

  public InstanceStorage instanceStorage = new InstanceStorage();

  private final Logger logger = LoggerFactory.getLogger("fake-inventory-storage");

  public FakeInventoryStorage (Vertx vertx, TestContext testContext, Async async) {
    Router router = Router.router(vertx);
    router.get(INSTANCE_STORAGE_PATH).handler(instanceStorage::getInstancesByQuery);
    router.get(INSTANCE_STORAGE_PATH +"/:id").handler(instanceStorage::getInstanceById);
    router.post("/*").handler(BodyHandler.create());
    router.post(INSTANCE_STORAGE_PATH).handler(instanceStorage::createInstance);
    router.put("/*").handler(BodyHandler.create());
    router.put(INSTANCE_STORAGE_PATH +"/:id").handler(instanceStorage::updateInstance);

    initializeInstanceStorage();

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

  private void initializeInstanceStorage() {
    Instance instance = (Instance) new Instance().setInstanceTypeId("123").setTitle("Initial Instance").setHrid("1");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    logger.info("Initializing fake storage with matchKey " + matchKey.getKey());
    instanceStorage.insert(instance);
  }


  public static String decode (String string) {
    try {
      return URLDecoder.decode(string, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      return "";
    }

  }

  public static String encode (String string) {
    try {
      return URLEncoder.encode(string, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      return "";
    }
  }

}
