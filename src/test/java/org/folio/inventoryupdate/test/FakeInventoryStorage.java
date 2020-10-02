package org.folio.inventoryupdate.test;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import org.folio.inventoryupdate.MatchKey;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class FakeInventoryStorage {
  public final static int PORT_INVENTORY_STORAGE = 9030;
  public final static String URL_INSTANCES = "/instance-storage/instances";
  private final Map<String,Instance> storedInstances = new HashMap<>();

  private final Logger logger = LoggerFactory.getLogger("fake-inventory-storage");

  public FakeInventoryStorage (Vertx vertx, TestContext testContext, Async async) {
    Router router = Router.router(vertx);
    router.get(URL_INSTANCES).handler(this::handlerGetInstancesByQuery);
    router.get(URL_INSTANCES+"/:id").handler(this::handlerGetInstanceById);
    router.post("/*").handler(BodyHandler.create());
    router.post(URL_INSTANCES).handler(this::handlerCreateInstance);
    router.put("/*").handler(BodyHandler.create());
    router.put(URL_INSTANCES+"/:id").handler(this::handlerUpdateInstance);

    initializeStoredInstances();

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

  private void initializeStoredInstances() {
    Instance instance = new Instance().setInstanceTypeId("123").setTitle("Initial Instance").setHrid("1");
    MatchKey matchKey = new MatchKey(instance.getJson());
    instance.setMatchKey(matchKey.getKey());
    logger.info("Initializing fake storage with matchkey of " + matchKey.getKey());
    addStoredInstance(instance);
  }

  private void addStoredInstance(Instance instance) {
    instance.setId(UUID.randomUUID().toString());
    storedInstances.put(instance.getId(), instance);
  }

  private JsonObject getInstancesAsJsonResultSet (String query) {
    JsonArray instancesJson = new JsonArray();
    for  (Instance instance: storedInstances.values()) {
      if (instance.match(query)) {
        instancesJson.add(instance.getJson());
      }
    }
    JsonObject response = new JsonObject();
    response.put("instances", instancesJson);
    response.put("totalRecords", instancesJson.size());
    return response;
  }

  /**
   * Inventory Storage handler
   * @param routingContext
   */
  private void handlerGetInstancesByQuery(RoutingContext routingContext) {
    final String query = decode(routingContext.request().getParam("query"));
    routingContext.request().endHandler(res -> {
      routingContext.response().headers().add("Content-Type", "application/json");
      routingContext.response().setStatusCode(200);
      routingContext.response().end(getInstancesAsJsonResultSet(query).encodePrettily());
    });
    routingContext.request().exceptionHandler(res -> {
      routingContext.response().setStatusCode(500);
      routingContext.response().end(res.getMessage());
    });
  }

  private void handlerGetInstanceById(RoutingContext routingContext) {
    final String id = routingContext.pathParam("id");
    Instance instance = storedInstances.get(id);

    routingContext.request().endHandler(res -> {
      routingContext.response().headers().add("Content-Type", "application/json");
      routingContext.response().setStatusCode(200);
      routingContext.response().end(instance.getJson().encodePrettily());
    });
    routingContext.request().exceptionHandler(res -> {
      routingContext.response().setStatusCode(500);
      routingContext.response().end(res.getMessage());
    });

  }

  private void handlerCreateInstance(RoutingContext routingContext) {

    JsonObject instanceJson = new JsonObject(routingContext.getBodyAsString());
    if (!instanceJson.containsKey("id")) {
      instanceJson.put("id", UUID.randomUUID().toString());
    }
    String id = instanceJson.getString("id");
    storedInstances.put(id, new Instance(instanceJson));

    routingContext.response().headers().add("Content-Type", "application/json");
    routingContext.response().setStatusCode(201);
    routingContext.response().end(instanceJson.encodePrettily());
  }

  private void handlerUpdateInstance(RoutingContext routingContext) {

    JsonObject instanceJson = new JsonObject(routingContext.getBodyAsString());
    String id = routingContext.pathParam("id");
    storedInstances.put(id, new Instance(instanceJson));

    routingContext.response().headers().add("Content-Type", "application/json");
    routingContext.response().setStatusCode(204);
    routingContext.response().end();
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
