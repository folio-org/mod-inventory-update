package org.folio.inventoryupdate.importing.service.provisioning.fileimport;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.service.provisioning.fileimport.reporting.InventoryMetrics;
import org.folio.inventoryupdate.importing.service.provisioning.fileimport.upsertclient.InternalInventoryUpdateClient;
import org.folio.inventoryupdate.importing.service.provisioning.fileimport.upsertclient.InventoryUpdateClient;

public class InventoryBatchUpdater implements RecordReceiver {

  public static final Logger logger = LogManager.getLogger("InventoryBatchUpdater");
  private FileProcessor fileProcessor;
  private final ArrayList<ProcessingRecord> records = new ArrayList<>();
  private final InventoryUpdateClient updateClient;
  private final Turnstile turnstile = new Turnstile();
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
      } else {
        releaseBatch(new BatchOfRecords(copyOfRecords, true, batchNumber));
      }
    }
  }

  private void releaseBatch(BatchOfRecords batch) {
    if (!fileProcessor.paused()) {
      if (turnstile.enterBatch(batch)) {
        persistBatch().onFailure(na -> {
          logger.error("Fatal error during upsert. Pausing file processor, skipping pending batches. {}",
              na.getMessage());
          // Set job status to paused until resumed.
          fileProcessor.halt("Fatal error during upsert. Halting processing, skipping pending batches. "
              + na.getMessage());
          turnstile.exitBatch();
        }).onComplete(na -> turnstile.exitBatch());
      } else {
        turnstile.exitBatch();
        logger.error("Something is blocking the process? Could not forward batch for upsert in 60 seconds.");
      }
    } else {
      logger.info("Skipping through batch #{} because processing is halted.", batch.getBatchNumber());
      turnstile.exitBatch();
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
    Promise<Void> promise = Promise.promise();
    BatchOfRecords batch = turnstile.viewCurrentBatch();
    if (fileProcessor.paused()) {
      logger.info("The file processor is paused, skipping batch {}{}.",
          batch == null ? "null" : batch.getBatchNumber(),
          batch != null && batch.size() == 0 ? "+" : "");
      turnstile.exitBatch();
    } else if (batch != null) {
      if (batch.size() > 0) {
        long upsertStarted = System.currentTimeMillis();
        updateClient.inventoryUpsert(batch.getUpsertRequestBody()).onSuccess(upsert -> {
          processingTime += System.currentTimeMillis() - upsertStarted;
          if (upsert.statusCode() >= 400) {
            logger.error("Fatal error when updating inventory, status code: {}", upsert.statusCode());
            promise.fail("Inventory update failed with status code " + upsert.statusCode());
          } else {
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
              // Delete and complete the promise
              persistDeletion(batch, promise);
            } else {
              if (batch.isLastBatchOfFile()) {
                reportEndOfFile();
              }
              promise.complete();
            }
          }
        }).onFailure(handler -> promise.fail("Fatal error: " + handler.getMessage()));
      } else if (batch.hasDeletingRecord()) {
        // Delete and complete the promise
        persistDeletion(batch, promise);
      } else { // we get here when the last set of records had exactly 100. We just need to report
        if (batch.isLastBatchOfFile()) {
          reportEndOfFile();
        }
        promise.complete();
      }
    }
    return promise.future();
  }

  /**
   * Persists the deletion, complete the promise when done.
   *
   * @param batch   The batch of records containing a deletion record
   * @param promise The promise of persistBatch
   */
  private void persistDeletion(BatchOfRecords batch, Promise<Void> promise) {
    JsonObject deletionRecord = batch.getDeletingRecord().getRecordAsJson().getJsonObject("delete");
    updateClient.inventoryDeletion(deletionRecord)
        .onSuccess(deletion -> {
          fileProcessor.reporting.incrementRecordsProcessed(1);
          if (deletion.statusCode() != 200) {
            logger.warn("No deletion performed with request {}, status code: {} due to {}",
                deletionRecord.encode(), deletion.statusCode(), deletion.getErrors());
            fileProcessor.reporting.log("No deletion performed with request " + deletionRecord.encode()
                + ", status code: " + deletion.statusCode() + " due to " + deletion.getErrors());
          } else {
            fileProcessor.reporting.incrementInventoryMetrics(new InventoryMetrics(deletion.getMetrics()));
          }
          promise.succeed();
        })
        .onFailure(
            f -> {
              fileProcessor.reporting.log("Error deleting inventory instance: " + f.getMessage());
              promise.succeed();
            });
  }

  private void reportEndOfFile() {
    fileProcessor.reporting.endOfFile();
    boolean queueDone = fileProcessor.fileQueueDone(true);
    if (queueDone) {
      fileProcessor.reporting.endOfQueue();
    }
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
    private boolean enterBatch(BatchOfRecords batch) {
      try {
        return gate.offer(batch, 60, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        logger.error("Putting next batch in queue-of-one interrupted: {}", ie.getMessage());
        Thread.currentThread().interrupt();
      }
      return false;
    }

    private void exitBatch() {
      try {
        gate.poll(10, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        logger.error("Taking batch from queue-of-one interrupted: {}", ie.getMessage());
        Thread.currentThread().interrupt();
      }
    }

    private BatchOfRecords viewCurrentBatch() {
      return gate.peek();
    }
  }
}
