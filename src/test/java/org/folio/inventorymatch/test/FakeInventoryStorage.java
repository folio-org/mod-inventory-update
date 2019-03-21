package org.folio.inventorymatch.test;


import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;

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
  private final int PORT_INVENTORY_STORAGE = 9030;
  private final String URL_INSTANCES = "/instance-storage/instances";
  private final List<Instance> storedInstances = new ArrayList<>();

  public FakeInventoryStorage (Vertx vertx, TestContext testContext, Async async) {
    Router router = Router.router(vertx);
    router.get(URL_INSTANCES).handler(this::handlerGetInstancesByQuery);
    router.post("/*").handler(BodyHandler.create());
    router.post(URL_INSTANCES).handler(this::handlerCreateInstance);
    router.put(URL_INSTANCES).handler(this::handlerUpdateInstance);

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
    addStoredInstance(new Instance().setInstanceTypeId("123").setTitle("Initial Instance"));
  }

  private void addStoredInstance(Instance instance) {
    storedInstances.add(instance);
  }

  private JsonObject getInstancesAsJsonResultSet (String query) {
    JsonArray instancesJson = new JsonArray();
    for  (Instance instance: storedInstances ) {
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

  private void handlerCreateInstance(RoutingContext routingContext) {

    JsonObject instanceJson = new JsonObject(routingContext.getBodyAsString());
    if (!instanceJson.containsKey("id")) {
      instanceJson.put("id", UUID.randomUUID().toString());
    }
    storedInstances.add(new Instance(instanceJson));

    routingContext.response().headers().add("Content-Type", "application/json");
    routingContext.response().setStatusCode(201);
    routingContext.response().end(instanceJson.encodePrettily());
  }

  private void handlerUpdateInstance(RoutingContext routingContext) {}

  /**
   * Tests fake storage method handlers
   * @param testContext
   */
  public void test (TestContext testContext) {

    // GET by query
    String bodyAsString;

    Response response1;

    RestAssured.port = PORT_INVENTORY_STORAGE;
    response1 = RestAssured.given()
      .get(URL_INSTANCES+"?query="+ encode("title=\"Initial Instance\""))
      .then()
      .log().ifValidationFails()
      .statusCode(200).extract().response();

    bodyAsString = response1.getBody().asString();
    JsonObject responseJson1 = new JsonObject(bodyAsString);

    testContext.assertEquals(responseJson1.getInteger("totalRecords"), 1,
                             "Number of instance records expected: 1" );

    // POST instance
    Response response2;
    JsonObject newInstance = new Instance().setTitle("New Instance").setInstanceTypeId("123").getJson();

    response2 = RestAssured.given()
      .body(newInstance.toString())
      .post(URL_INSTANCES)
      .then()
      .log().ifValidationFails()
      .statusCode(201).extract().response();

    bodyAsString = response2.getBody().asString();
    JsonObject responseJson2 = new JsonObject(bodyAsString);

    testContext.assertEquals(responseJson2.getString("title"), "New Instance");

  }

  private String encode (String string) {
    try {
      return URLEncoder.encode(string, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      return "";
    }
  }

  private String decode (String string) {
    try {
      return URLDecoder.decode(string, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      return "";
    }
  }

}
