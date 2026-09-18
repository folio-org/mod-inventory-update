package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class FileListenerTest {

  static class FailingFileListener extends FileListener {
    @Override
    public Future<?> start() throws Exception {
      channel = new Channel(null, null, null, null, null, null, null, true, true);
      return Future.failedFuture("FailingFileListener.start() fails");
    }

    @Override
    public Future<FileProcessor> getFileProcessor(boolean activating) {
      return null;
    }

    @Override
    public void listen() {
    }
  };

  @Test
  void deployFailure(VertxTestContext vtc) {
    new FailingFileListener().deploy()
    .onComplete(vtc.failing(e -> {
      e.printStackTrace();
      assertThat(e.getMessage(), startsWith("Couldn't start file processor verticle"));
      vtc.completeNow();
    }));
  }

}
