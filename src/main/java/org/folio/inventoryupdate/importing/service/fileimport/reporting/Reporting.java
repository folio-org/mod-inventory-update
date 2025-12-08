package org.folio.inventoryupdate.importing.service.fileimport.reporting;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.Entity;
import org.folio.inventoryupdate.importing.moduledata.LogLine;
import org.folio.inventoryupdate.importing.moduledata.RecordFailure;
import org.folio.inventoryupdate.importing.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryupdate.importing.service.fileimport.BatchOfRecords;
import org.folio.inventoryupdate.importing.service.fileimport.FileProcessor;
import org.folio.inventoryupdate.importing.utils.SettableClock;

public class Reporting {

  public static final Logger logger = LogManager.getLogger("reporting");
  private final long startTime;
  private final AtomicInteger filesProcessed = new AtomicInteger(0);
  private final AtomicInteger recordsProcessed = new AtomicInteger(0);
  private final InventoryMetrics inventoryMetrics = new InventoryMetrics();
  private final BlockingQueue<FileStats> fileStats = new ArrayBlockingQueue<>(2);
  private final ModuleStorageAccess storage;
  private final FileProcessor fileProcessor;

  public Reporting(FileProcessor handler, String tenant, Vertx vertx) {
    this.fileProcessor = handler;
    this.startTime = System.currentTimeMillis();
    this.storage = new ModuleStorageAccess(vertx, tenant);
  }

  public void nowProcessing(String fileName) {
    try {
      logger.info("Processing file {}", fileName);
      fileStats.put(new FileStats(fileName));
    } catch (InterruptedException ie) {
      logger.error("Initiation of file stats interrupted.");
      Thread.currentThread().interrupt();
    }
  }

  public void incrementFilesProcessed() {
    filesProcessed.incrementAndGet();
  }

  public void incrementInventoryMetrics(InventoryMetrics metrics) {
    inventoryMetrics.add(metrics);
    if (fileStats.peek() != null) {
      fileStats.peek().addInventoryMetrics(metrics);
    }
  }

  public boolean pendingFileStats() {
    return !fileStats.isEmpty();
  }

  public void incrementRecordsProcessed(int delta) {
    recordsProcessed.addAndGet(delta);
    if (fileStats.peek() != null) {
      fileStats.peek().incrementRecordsProcessed(delta);
    }
  }

  public int getRecordsProcessed() {
    return recordsProcessed.get();
  }

  /**
   * Reports at end-of-current file.
   */
  public void endOfFile() {
    incrementFilesProcessed();
    reportFileStats();
  }

  public void endOfQueue() {
    reportFileQueueStats(true);
  }

  public void reportFileStats() {
    try {
      if (!fileStats.isEmpty()) {
        FileStats stats = fileStats.peek();
        assert stats != null;
        log("File #" + filesProcessed.get() + " (" + stats.getFileName() + ") "
            + stats.getRecordsProcessed() + " records in " + processingTimeAsString(stats.processingTime())
            + " (" + (stats.getRecordsProcessed() * 1000L / stats.processingTime()) + " recs/s.)")
            .compose(na -> log("File: " + stats.getInventoryMetrics().report()));
        fileStats.take();
      } else {
        logger.info("reportFileStats(): FileStatus queue was empty");
      }
    } catch (InterruptedException ie) {
      logger.error("Error reporting file statistics: {}", ie.getMessage());
      Thread.currentThread().interrupt();
    }
  }

  public void reportFileQueueStats(boolean queueDone) {
    long processingTime = System.currentTimeMillis() - startTime;
    log((queueDone ? "Done processing queue. " : "") + filesProcessed + " file(s) with " + recordsProcessed.get()
        + " records processed in " + processingTimeAsString(processingTime) + " ("
        + (recordsProcessed.get() * 1000L / processingTime) + " recs/s.)")
        .compose(na -> queueDone ? log("File queue: " + inventoryMetrics.report()) : null);
    if (queueDone) {
      fileProcessor.logFinish(recordsProcessed.get());

      logger.info("Done processing queue. {} file(s) with {} records processed in {} ({} recs/s.) ",
          filesProcessed, recordsProcessed.get(), processingTimeAsString(processingTime),
          recordsProcessed.get() * 1000L / processingTime);
      logger.info(fileProcessor.getStats());
    }
  }

  public Future<Void> reportErrors(BatchOfRecords batch) {
    logger.error("Reporting errors for file [{}]",
        fileStats.peek() == null ? "file name missing" : fileStats.peek().getFileName());
    try {
      return storage.storeEntities(
          batch.getErrors().stream()
              .map(JsonObject.class::cast)
              .map(error -> new RecordFailure(UUID.randomUUID(),
                  fileProcessor.getImportJob().getRecord().id(),
                  fileProcessor.getImportConfigId(),
                  fileProcessor.getImportJob().getRecord().channelName(),
                  getInstanceHridFromErrorResponse(error),
                  SettableClock.getLocalDateTime().toString(),
                  getBatchIndexFromErrorResponse(error) == -1 ? null :
                      batch.get(getBatchIndexFromErrorResponse(error)).getOriginalRecordAsString(),
                  error.getJsonObject("message", new JsonObject())
                      .getJsonArray("errors",
                          new JsonArray().add(new JsonObject()
                              .put("message", "Error message from storage missing or format unrecognized."))),
                  error.getJsonObject("requestJson"),
                  fileStats.peek() == null ? null : fileStats.peek().getFileName()
              ).withCreatingUser(null))
              .toList());
    } catch (Exception e) {
      logger.error("Exception storing failed records: {} {}.", e.getMessage(), Arrays.toString(e.getStackTrace()));
      return Future.failedFuture("Exception storing failed records: " + e.getMessage());
    }
  }

  private static String getInstanceHridFromErrorResponse(JsonObject errorJson) {
    if (errorJson != null && errorJson.getJsonObject("requestJson") != null
        && errorJson.getJsonObject("requestJson").containsKey("instance")) {
      return errorJson.getJsonObject("requestJson").getJsonObject("instance").getString("hrid");
    } else {
      return null;
    }
  }

  private static Integer getBatchIndexFromErrorResponse(JsonObject errorJson) {
    if (errorJson != null && errorJson.getJsonObject("requestJson") != null
        && errorJson.getJsonObject("requestJson").containsKey("processing")) {
      return errorJson.getJsonObject("requestJson").getJsonObject("processing").getInteger("batchIndex");
    } else {
      return -1;
    }
  }

  private static String processingTimeAsString(long processingTime) {
    int hours = (int) processingTime / (1000 * 60 * 60);
    long remainingMs = processingTime % (1000 * 60 * 60);
    int minutes = (int) remainingMs / (1000 * 60);
    remainingMs = remainingMs % (1000 * 60);
    int seconds = (int) remainingMs / 1000;
    return (hours > 0 ? hours + " hours " : "") + (hours > 0 || minutes > 0 ? minutes + " minutes " : "")
        + seconds + " seconds";
  }

  public Future<Void> log(String statement) {
    List<Entity> lines = new ArrayList<>();
    lines.add(new LogLine(
        UUID.randomUUID(),
        fileProcessor.getImportJob().getRecord().id(),
        fileProcessor.getImportJob().getRecord().channelId(),
        fileProcessor.getImportJob().getRecord().channelName(),
        SettableClock.getLocalDateTime().toString(),
        fileProcessor.getImportJob().getRecord().channelName(),
        statement).withCreatingUser(null));
    return storage.storeEntities(lines);
  }
}
