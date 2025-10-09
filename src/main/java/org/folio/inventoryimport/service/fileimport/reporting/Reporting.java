package org.folio.inventoryimport.service.fileimport.reporting;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryimport.moduledata.Entity;
import org.folio.inventoryimport.moduledata.LogLine;
import org.folio.inventoryimport.moduledata.RecordFailure;
import org.folio.inventoryimport.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryimport.service.fileimport.BatchOfRecords;
import org.folio.inventoryimport.service.fileimport.FileProcessor;
import org.folio.inventoryimport.utils.SettableClock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Reporting {

    private final long startTime;
    private final AtomicInteger filesProcessed = new AtomicInteger(0);
    private final AtomicInteger recordsProcessed = new AtomicInteger(0);
    private final InventoryMetrics inventoryMetrics = new InventoryMetrics();
    private final BlockingQueue<FileStats> fileStats = new ArrayBlockingQueue<>(2);
    private final ModuleStorageAccess storage;
    private final FileProcessor fileProcessor;

    public static final Logger logger = LogManager.getLogger("reporting");


    public Reporting(FileProcessor handler, String tenant, Vertx vertx) {
        this.fileProcessor = handler;
        this.startTime = System.currentTimeMillis();
        this.storage = new ModuleStorageAccess(vertx, tenant);
    }

    public void nowProcessing(String fileName) {
        try {
            fileStats.put(new FileStats(fileName));
        } catch (InterruptedException ignore) {}
    }

    public void incrementFilesProcessed() {
        filesProcessed.incrementAndGet();
    }

    public void incrementInventoryMetrics(InventoryMetrics metrics) {
        inventoryMetrics.add(metrics);
        if (fileStats.peek()!=null) fileStats.peek().addInventoryMetrics(metrics);
    }

    public boolean pendingFileStats() {
        return !fileStats.isEmpty();
    }

    public void incrementRecordsProcessed(int delta) {
        recordsProcessed.addAndGet(delta);
        if (fileStats.peek()!=null) fileStats.peek().incrementRecordsProcessed(delta);
    }

    public int getRecordsProcessed() {
        return recordsProcessed.get();
    }

    /**
     * Reports at end-of-current file
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
                        + stats.getRecordsProcessed() + " records in " + processingTimeAsString(stats.processingTime()) + " (" + (stats.getRecordsProcessed() * 1000L / stats.processingTime()) +
                        " recs/s.)")
                        .compose(na -> log("File: " + stats.getInventoryMetrics().report()));
                fileStats.take();
            } else {
                logger.info("reportFileStats(): FileStatus queue was empty");
            }
        } catch (InterruptedException ie) {
            logger.error("Error reporting file statistics: " + ie.getMessage());
        }
    }

    public void reportFileQueueStats(boolean queueDone) {
        long processingTime = (System.currentTimeMillis() - startTime);
        log((queueDone ? "Done processing queue. " : "") + filesProcessed + " file(s) with " + recordsProcessed.get() +
                " records processed in " + processingTimeAsString(processingTime) + " (" +
                (recordsProcessed.get() * 1000L / processingTime) + " recs/s.)")
                .compose(na -> queueDone ? log("File queue: " + inventoryMetrics.report()) : null);
        if (queueDone) {
            fileProcessor.logFinish(recordsProcessed.get());

            logger.info("Done processing queue. " + filesProcessed + " file(s) with " + recordsProcessed.get() +
                    " records processed in " + processingTimeAsString(processingTime) + " (" +
                    (recordsProcessed.get() * 1000L / processingTime) + " recs/s.)");
        }
    }

    public Future<Void> reportErrors(BatchOfRecords batch) {
        // Perform assert fileStats.peek() != null;
        // Perform String fileName = fileStats.peek().getFileName();
        try {
            return storage.storeEntities(
                    new RecordFailure(),
                    batch.getErrors().stream()
                            .map(error -> ((JsonObject) error))
                            .map(error -> new RecordFailure(UUID.randomUUID(),
                                    fileProcessor.getImportJob().record.id(),
                                    fileProcessor.getImportConfigId(),
                                    fileProcessor.getImportJob().record.importConfigName(),
                                    getInstanceHridFromErrorResponse(error),
                                    SettableClock.getLocalDateTime().toString(),
                                    getBatchIndexFromErrorResponse(error) == null ? null : batch.get(getBatchIndexFromErrorResponse(error)).getOriginalRecordAsString(),
                                    error.getJsonObject("message").getJsonArray("errors"),
                                    error.getJsonObject("requestJson"))
                            ).collect(Collectors.toList()));
        } catch (Exception e) {
            logger.error("Exception storing failed records: " + e.getMessage() + " " + Arrays.toString(e.getStackTrace()));
            return Future.failedFuture("Exception storing failed records: " + e.getMessage());
        }
    }

    private static String getInstanceHridFromErrorResponse(JsonObject errorJson) {
        if (errorJson != null && errorJson.getJsonObject("requestJson") !=null && errorJson.getJsonObject("requestJson").containsKey("instance")) {
            return errorJson.getJsonObject("requestJson").getJsonObject("instance").getString("hrid");
        } else {
            return null;
        }
    }

    private static Integer getBatchIndexFromErrorResponse(JsonObject errorJson) {
        if (errorJson != null && errorJson.containsKey("requestJson") && errorJson.getJsonObject("requestJson").containsKey("processing")) {
            return errorJson.getJsonObject("requestJson").getJsonObject("processing").getInteger("batchIndex");
        } else {
            return null;
        }
    }

    private static String processingTimeAsString (long processingTime) {
        int hours = (int) processingTime/(1000*60*60);
        long remainingMs = processingTime % (1000*60*60);
        int minutes = (int) remainingMs/(1000*60);
        remainingMs = remainingMs % (1000*60);
        int seconds = (int) remainingMs/1000;
        return (hours>0 ? hours + " hours " : "") +  (hours>0 || minutes>0 ? minutes  + " minutes " : "") + seconds + " seconds";
    }

    public Future<Void> log (String statement) {
        List<Entity> lines = new ArrayList<>();
        lines.add(new LogLine(
                UUID.randomUUID(),
                fileProcessor.getImportJob().record.id(),
                SettableClock.getLocalDateTime().toString(),
                fileProcessor.getImportJob().record.importConfigName(),
                statement));
        return storage.storeEntities(new LogLine(),lines);
    }
}
