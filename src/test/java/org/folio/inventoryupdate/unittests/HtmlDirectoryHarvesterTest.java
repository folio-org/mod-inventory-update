package org.folio.inventoryupdate.unittests;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileQueue;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.HtmlDirectoryHarvester;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.SourceFile;
import org.folio.inventoryupdate.unittests.fixtures.FileService;
import org.junit.Test;

/**
 * Supplements harvesting tests in {@link ImportTests} by also testing mocked index page with file dates,
 * ({@link ImportTests} uses {@link FileService} which does not contain dates in the index page)
 * and testing the sorting of files by update date as well.
 */
public class HtmlDirectoryHarvesterTest {
  @Test
  public void parsesDirectoryIndexLinksAndTimestamps() {
    String html = """
        <html><body><pre>
        <a href="../">Parent Directory</a>
        <a href="old.xml">old.xml</a>                 2026-07-12 09:15  42
        <a href="new%20file.xml">new file.xml</a>    13-Jul-2026 10:30  43
        <a href="early.xml">early.xml</a>             2026-07-13 08:15  41
        <a href="subdir/">subdir/</a>               13-Jul-2026 10:35  -
        <a href="?C=N;O=D">Name</a>
        <a href="absolute.xml">absolute.xml</a>       13-Jul-26 10:45  44
        </pre></body></html>
        """;

    List<HtmlDirectoryHarvester.DirectoryEntry> entries =
        HtmlDirectoryHarvester.parseDirectoryIndex(URI.create("https://example.org/harvest/"), html);

    assertThat(entries.size(), is(4));
    assertThat(entries.get(0).fileName(), is("absolute.xml"));
    assertThat(entries.get(0).url(), is("https://example.org/harvest/absolute.xml"));
    assertThat(entries.get(0).timestamp().get(), is(Instant.parse("2026-07-13T10:45:00Z")));
    assertThat(entries.get(1).fileName(), is("early.xml"));
    assertThat(entries.get(1).timestamp().get(), is(Instant.parse("2026-07-13T08:15:00Z")));
    assertThat(entries.get(2).fileName(), is("new file.xml"));
    assertThat(entries.get(2).timestamp().get(), is(Instant.parse("2026-07-13T10:30:00Z")));
    assertThat(entries.get(3).fileName(), is("old.xml"));
    assertThat(entries.get(3).timestamp().get(), is(Instant.parse("2026-07-12T09:15:00Z")));
  }

  @Test
  public void parsesStoredAndHttpTimestamps() {
    assertThat(HtmlDirectoryHarvester.parseTimestamp("2026-07-13T10:15:30,000+00:00").get(),
        is(Instant.parse("2026-07-13T10:15:30Z")));
    assertThat(HtmlDirectoryHarvester.parseTimestamp("2026-07-13T10:15:30,000").get(),
        is(Instant.parse("2026-07-13T10:15:30Z")));
    assertThat(HtmlDirectoryHarvester.parseHttpDate("Mon, 13 Jul 2026 10:15:30 GMT").get(),
        is(Instant.parse("2026-07-13T10:15:30Z")));
    assertTrue(HtmlDirectoryHarvester.parseTimestamp("not a timestamp").isEmpty());
  }

  @Test
  public void pushesFilesInModifiedDateOrder() throws InterruptedException {
    Vertx vertx = Vertx.vertx();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    FakeFileQueue fileQueue = new FakeFileQueue();

    vertx.createHttpServer()
        .requestHandler(request -> {
          if ("/index/".equals(request.path())) {
            request.response().end("""
                <a href="new.xml">new.xml</a> 2026-07-13 12:00
                <a href="old.xml">old.xml</a> 2026-07-13 10:00
                """);
          } else if ("/index/new.xml".equals(request.path())) {
            request.response().end("new");
          } else if ("/index/old.xml".equals(request.path())) {
            request.response().end("old");
          } else {
            request.response().setStatusCode(404).end();
          }
        })
        .listen(0)
        .compose(server -> {
          Channel channel = channel("http://localhost:" + server.actualPort() + "/index/");
          return new HtmlDirectoryHarvester(vertx).harvest(channel, fileQueue, null)
              .eventually(server::close);
        })
        .onComplete(result -> {
          if (result.failed()) {
            failure.set(result.cause());
          }
          latch.countDown();
        });

    assertTrue(latch.await(10, TimeUnit.SECONDS));
    assertNull(failure.get());
    assertThat(fileQueue.fileNames, is(List.of("old.xml", "new.xml")));
    vertx.close();
  }

  private Channel channel(String harvestUrl) {
    return new Channel(UUID.randomUUID(), "Channel", "channel", "XML", UUID.randomUUID(), harvestUrl, null,
        true, true) {
      @Override
      public Future<Integer> setLastHarvested(String lastHarvested, EntityStorage configStorage) {
        return Future.succeededFuture(1);
      }
    };
  }

  private static class FakeFileQueue implements FileQueue {
    private final List<String> fileNames = new ArrayList<>();

    @Override
    public Future<String> initialize(boolean retainFilesIfAny) {
      return Future.succeededFuture("ready");
    }

    @Override
    public Future<Void> push(String fileName, String timeStamp, String payload) {
      fileNames.add(fileName);
      return Future.succeededFuture();
    }

    @Override
    public Future<Boolean> hasFileInProcess() {
      return Future.succeededFuture(false);
    }

    @Override
    public Future<Boolean> isEmpty() {
      return Future.succeededFuture(fileNames.isEmpty());
    }

    @Override
    public Future<Integer> size() {
      return Future.succeededFuture(fileNames.size());
    }

    @Override
    public Future<String> nameOfFileInProcess() {
      return Future.succeededFuture("no file in process");
    }

    @Override
    public Future<SourceFile> promoteAndGetNextFileIfPossible() {
      return Future.succeededFuture(null);
    }

    @Override
    public Future<SourceFile> currentlyPromotedFile() {
      return Future.succeededFuture(null);
    }
  }
}
