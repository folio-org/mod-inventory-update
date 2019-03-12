package org.folio.inventorymatch.test;

import org.folio.inventorymatch.test.samples.Instance;
import org.folio.rest.RestVerticle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

@RunWith(VertxUnitRunner.class)
public class InstanceMatchingTestSuite {

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4jLogDelegateFactory");
  }
  Vertx vertx;
  private final int PORT_INVENTORY_STORAGE = 9030;
  private final int PORT_INVENTORY_MATCH = 9031;
  private final String URL_INSTANCES = "/instance-storage/instances";

  public InstanceMatchingTestSuite() {

  }

  @Before
  public void setUp(TestContext testContext) {
    vertx = Vertx.vertx();

    // Register the testContext exception handler to catch assertThat
    vertx.exceptionHandler(testContext.exceptionHandler());

    setUpMatch(testContext, testContext.async());
  }

  private void setUpMatch(TestContext testContext, Async async) {
    JsonObject conf = new JsonObject();
    conf.put("http.port", PORT_INVENTORY_MATCH);
    DeploymentOptions opt = new DeploymentOptions()
      .setConfig(conf);
    vertx.deployVerticle(RestVerticle.class.getName(), opt,
      r -> {
        testContext.assertTrue(r.succeeded());
        setUpFakeInventoryStorage(testContext, async);
      });
  }

  private void setUpFakeInventoryStorage(TestContext testContext, Async async) {

    Router router = Router.router(vertx);
    router.get(URL_INSTANCES).handler(this::handlerGetByQuery);

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

  /**
   * Inventory Storage handler
   * @param routingContext
   */
  private void handlerGetByQuery(RoutingContext routingContext) {
    final String query = routingContext.request().getParam("query");
    routingContext.request().endHandler(res -> {
      JsonArray instances = new JsonArray();
      JsonObject rec = new Instance()
              .setInstanceTypeId("123")
              .setTitle("Book 1")
              .getJson();
      instances.add(rec);
      routingContext.response().headers().add("Content-Type", "application/json");
      routingContext.response().setStatusCode(200);
      routingContext.response().end(rec.encodePrettily());
    });
    routingContext.request().exceptionHandler(res -> {
      routingContext.response().setStatusCode(500);
      routingContext.response().end(res.getMessage());
    });
  }

  @Test
  public void testFakeInventoryStorage(TestContext testContext) {
    Response response;
    String bodyAsString;

    RestAssured.port = PORT_INVENTORY_STORAGE;
    response = RestAssured.given()
      .get(URL_INSTANCES+"?query=water")
      .then()
      .log().ifValidationFails()
      .statusCode(200).extract().response();

    bodyAsString = response.getBody().asString();

    testContext.assertTrue(1==1);

  }

  @After
  public void tearDown(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      async.complete();
    }));
  }


}
