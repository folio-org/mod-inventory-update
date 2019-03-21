package org.folio.inventorymatch.test;

import org.folio.rest.RestVerticle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class InstanceMatchingTestSuite {

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4jLogDelegateFactory");
  }
  Vertx vertx;
  private final int PORT_INVENTORY_MATCH = 9031;
  private FakeInventoryStorage inventoryStorage;

  public InstanceMatchingTestSuite() {}

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
        inventoryStorage = new FakeInventoryStorage(vertx, testContext, async);
      });
  }

  @Test
  public void testFakeInventoryStorage(TestContext testContext) {
    FakeInventoryStorageValidator.validateStorage(inventoryStorage, testContext);
  }

  @After
  public void tearDown(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      async.complete();
    }));
  }

}
