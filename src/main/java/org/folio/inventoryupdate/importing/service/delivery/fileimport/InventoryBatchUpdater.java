package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.reporting.InventoryMetrics;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.upsertclient.InternalInventoryUpdateClient;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.upsertclient.InventoryUpdateClient;

public class InventoryBatchUpdater implements RecordReceiver {

  public static final Logger logger = LogManager.getLogger("InventoryBatchUpdater");
  private FileProcessor fileProcessor;
  private final ArrayList<ProcessingRecord> records = new ArrayList<>();
  private final InventoryUpdateClient updateClient;
  private final Turnstile turnstile = new Turnstile();
  private Promise<Void> fileFinished = Promise.promise();
  private long batchNumber;

  private long processingTime;
  private int recordsProcessed;

  public InventoryBatchUpdater(RoutingContext routingContext) {
    updateClient = new InternalInventoryUpdateClient(routingContext.vertx(), routingContext);
    batchNumber = 0L;
  }

  /**
   * Sets a reference back to the controller.
   */
  public InventoryBatchUpdater forFileProcessor(FileProcessor processor) {
    fileProcessor = processor;
    return this;
  }

  public Future<Void> startFile() {
    records.clear();
    fileFinished = Promise.promise();
    return fileFinished.future();
  }

  public void failCurrentFile(Throwable cause) {
    fileFinished.tryFail(cause);
  }

  @Override
  public void put(ProcessingRecord processingRecord) {
    if (processingRecord != null) {
      recordsProcessed++;
      processingRecord.setBatchIndex(records.size());
      records.add(processingRecord);
      if (records.size() > 99 || processingRecord.isDeletion()) {
        ArrayList<ProcessingRecord> copyOfRecords = new ArrayList<>(records);
        records.clear();
        if (!copyOfRecords.isEmpty()) {
          batchNumber++;
        }
        if (fileProcessor.paused()) {
          logger.info("Not releasing pending batch #{} because processing has been halted", batchNumber);
        } else {
          releaseBatch(new BatchOfRecords(copyOfRecords, false, batchNumber));
        }
      }
    } else { // a null record is the end-of-file signal, forward remaining records if any
      ArrayList<ProcessingRecord> copyOfRecords = new ArrayList<>(records);
      records.clear();
      if (!copyOfRecords.isEmpty()) {
        batchNumber++;
      }
      if (fileProcessor.paused()) {
        logger.info("Skipping remaining pending batch ({} records) because processing has been halted",
            copyOfRecords.size());
        fileFinished.tryComplete();
      } else {
        releaseBatch(new BatchOfRecords(copyOfRecords, true, batchNumber));
      }
    }
  }

  private void releaseBatch(BatchOfRecords batch) {
    if (!fileProcessor.paused()) {
      try {
        if (turnstile.enterBatch(batch)) {
          persistBatch()
              .onSuccess(na -> completeFileIfLastBatch(batch))
              .onFailure(this::handlePersistenceFailure)
              .onComplete(na -> {
                try {
                  turnstile.exitBatch();
                } catch (TimeoutException toe) {
                  handlePersistenceFailure(toe);
                }
              });
        }
      } catch (TimeoutException toe) {
        handlePersistenceFailure(toe);
      }
    } else {
      logger.info("Skipping through batch #{} because processing is halted.", batch.getBatchNumber());
      completeFileIfLastBatch(batch);
    }
  }

  private void completeFileIfLastBatch(BatchOfRecords batch) {
    if (batch.isLastBatchOfFile()) {
      fileFinished.tryComplete();
    }
  }

  @Override
  public void endOfDocument() {
    put(null);
  }

  @Override
  public long getProcessingTime() {
    return processingTime;
  }

  @Override
  public int getRecordsProcessed() {
    return recordsProcessed;
  }

  /**
   * This is the last function of the import pipeline, and since it's asynchronous
   * it must be in charge of when to invoke results reporting. The file listening verticle will not
   * know when the last upsert of a source file of records is done, for example.
   */
  private Future<Void> persistBatch() {
    BatchOfRecords batch = turnstile.viewCurrentBatch();
    if (fileProcessor.paused()) {
      logger.info("The file processor is paused, skipping batch {}{}.",
          batch == null ? "null" : batch.getBatchNumber(),
          batch != null && batch.size() == 0 ? "+" : "");
      return Future.succeededFuture();
    }
    if (batch == null) {
      return Future.succeededFuture();
    }
    if (batch.size() <= 0) {
      if (batch.hasDeletingRecord()) {
        return persistDeletion(batch);
      }
      // we get here when the last set of records had exactly 100. We just need to report
      if (batch.isLastBatchOfFile()) {
        reportEndOfFile();
      }
      return Future.succeededFuture();
    }

    long upsertStarted = System.nanoTime();
    return updateClient.inventoryUpsert(batch.getUpsertRequestBody())
        .compose(upsert -> {
          processingTime += System.nanoTime() - upsertStarted;
          if (upsert.statusCode() >= 400) {
            logger.error("Fatal error when updating inventory, status code: {}", upsert.statusCode());
            return Future.failedFuture("Inventory update failed with status code " + upsert.statusCode());
          }
          fileProcessor.reporting.incrementRecordsProcessed(batch.size());
          // In scenario with recurring HRIDs in batch, status will be 207 but no failed record to create.
          if (upsert.statusCode() == 207 && upsert.hasErrorObjects()) {
            batch.setResponse(upsert);
            fileProcessor.reporting.reportErrors(batch)
                .onFailure(err -> logger.error("Error logging upsert results for batch #{}, {}",
                batch.getBatchNumber(), err.getMessage()));
          }
          fileProcessor.reporting.incrementInventoryMetrics(new InventoryMetrics(upsert.getMetrics()));
          if (batch.hasDeletingRecord()) {
            return persistDeletion(batch);
          }
          if (batch.isLastBatchOfFile()) {
            reportEndOfFile();
          }
          return Future.succeededFuture();
        });
  }

  /**
   * Persists the deletion, complete the promise when done.
   *
   * @param batch   The batch of records containing a deletion record
   */
  private Future<Void> persistDeletion(BatchOfRecords batch) {
    long deletionStarted = System.nanoTime();
    JsonObject deletionRecord = batch.getDeletingRecord().getRecordAsJson().getJsonObject("delete");
    return updateClient.inventoryDeletion(deletionRecord)
        .onSuccess(deletion -> {
          fileProcessor.reporting.incrementRecordsProcessed(1);
          processingTime += System.nanoTime() - deletionStarted;
          if (deletion.statusCode() != 200) {
            logger.warn("No deletion performed with request {}, status code: {} due to {}",
                deletionRecord.encode(), deletion.statusCode(), deletion.getErrors());
            fileProcessor.reporting.log("No deletion performed with request " + deletionRecord.encode()
                + ", status code: " + deletion.statusCode() + " due to " + deletion.getErrors());
          } else {
            fileProcessor.reporting.incrementInventoryMetrics(new InventoryMetrics(deletion.getMetrics()));
          }
        })
        .onFailure(e -> fileProcessor.reporting.log("Error deleting inventory instance: " + e.getMessage()))
        .mapEmpty();
  }

  private void reportEndOfFile() {
    fileProcessor.reporting.endOfFile();
    fileProcessor.fileQueueDone(true)
        .onFailure(f -> logger.error("Error checking if file queue done {}", f.getMessage()))
        .compose(done -> {
          if (done) {
            fileProcessor.reporting.endOfQueue();
          }
          return Future.succeededFuture(null);
        });
  }

  private void handlePersistenceFailure(Throwable cause) {
    Throwable failure = cause != null
        ? cause
        : new IllegalStateException("Batch persistence failed without a cause");

    String detail = failure.getMessage();
    if (detail == null || detail.isBlank()) {
      detail = failure.getClass().getSimpleName();
    }

    String message =
        "Fatal error during upsert. Halting processing, skipping pending batches. "
            + detail;
    logger.error(message, failure);

    if (!fileProcessor.paused()) {
      fileProcessor.halt(message);
    }
    failCurrentFile(failure);
  }

  /**
   * Class wrapping a blocking queue of one, acting as a turnstile for batches in order to persist them one
   * at a time with no overlap.
   */
  private static class Turnstile {

    private final BlockingQueue<BatchOfRecords> gate = new ArrayBlockingQueue<>(1);

    /**
     * Puts batch in blocking queue-of-one; process waits if previous batch still in queue.
     */
    private boolean enterBatch(BatchOfRecords batch) throws TimeoutException {
      try {
        return gate.offer(batch, 90, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        logger.error("Upsert attempt timed out after 90 seconds.");
        Thread.currentThread().interrupt();
        throw new TimeoutException("Upsert attempt timed out after 90 seconds.");
      }
    }

    private void exitBatch() throws TimeoutException {
      try {
        gate.poll(10, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        logger.error("Taking batch from queue-of-one timed out after 10 seconds: {}", ie.getMessage());
        Thread.currentThread().interrupt();
        throw new TimeoutException("Taking batch from queue-of-one timed out after 10 seconds: " + ie.getMessage());
      }
    }

    private BatchOfRecords viewCurrentBatch() {
      return gate.peek();
    }
  }
}
