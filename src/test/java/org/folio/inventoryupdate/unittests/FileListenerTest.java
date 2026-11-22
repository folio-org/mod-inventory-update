package org.folio.inventoryupdate.unittests;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileListener;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileProcessor;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class FileListenerTest {
  private final FailingFileListener listener = new FailingFileListener();

  @Test
  public void deployFailsWhenVerticleStartupFails(TestContext context) {
    listener.deploy().onComplete(context.asyncAssertFailure(cause -> {
      context.assertTrue(listener.startCalled);
      context.assertEquals("Couldn't launch file processor for channel [test-channel].", cause.getMessage());
    }));
  }

  @After
  public void closeDeploymentVertx(TestContext context) {
    listener.closeDeploymentVertx().onComplete(context.asyncAssertSuccess());
  }

  private static class FailingFileListener extends FileListener {
    private volatile boolean startCalled;

    FailingFileListener() {
      tenant = "test";
      updateChannel(new Channel().fromJson(new JsonObject().put("name", "test-channel")));
    }

    @Override
    public Future<?> start() {
      startCalled = true;
      return Future.failedFuture("Forced startup failure");
    }

    @Override
    public void listen() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Future<FileProcessor> getFileProcessor(boolean activating) {
      throw new UnsupportedOperationException();
    }

    Future<Void> closeDeploymentVertx() {
      return deploymentVertx == null ? Future.succeededFuture() : deploymentVertx.close();
    }
  }
}
