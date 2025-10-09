package org.folio.inventoryimport.service.fileimport;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryimport.foliodata.InventoryUpdateClient;
import org.folio.inventoryimport.service.fileimport.reporting.InventoryMetrics;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class InventoryBatchUpdater implements RecordReceiver {

    private FileProcessor fileProcessor;
    private final ArrayList<ProcessingRecord> records = new ArrayList<>();
    private final InventoryUpdateClient updateClient;
    private final Turnstile turnstile = new Turnstile();
    public static final Logger logger = LogManager.getLogger("InventoryBatchUpdater");

    public InventoryBatchUpdater(RoutingContext routingContext) {
        updateClient = InventoryUpdateClient.getClient(routingContext);
    }

    /**
     * Sets a reference back to the controller.
      */
    public InventoryBatchUpdater forFileProcessor(FileProcessor processor) {
        fileProcessor = processor;
        return this;
    }

    @Override
    public void put(ProcessingRecord record) {
        if (record != null) {
            record.setBatchIndex(records.size());
            records.add(record);
            if (records.size() > 99 || record.isDeletion()) {
                ArrayList<ProcessingRecord> copyOfRecords = new ArrayList<>(records);
                records.clear();
                releaseBatch(new BatchOfRecords(copyOfRecords, false));
            }
        } else { // a null record is the end-of-file signal, forward remaining records if any
            ArrayList<ProcessingRecord> copyOfRecords = new ArrayList<>(records);
            records.clear();
            releaseBatch(new BatchOfRecords(copyOfRecords, true));
        }
    }

    private void releaseBatch(BatchOfRecords batch) {
        if (!fileProcessor.paused()) {
            turnstile.enterBatch(batch);
            persistBatch().onFailure(na -> {
                logger.error("Fatal error during upsert. Halting job. " + na.getMessage());
                fileProcessor.reporting.log("Fatal error during upsert. Halting job. " + na.getMessage());
                fileProcessor.pause();
            }).onComplete(na -> turnstile.exitBatch());
        }
    }


    @Override
    public void endOfDocument() {
        put(null);
    }

    /**
     * This is the last function of the import pipeline, and since it's asynchronous
     * it must be in charge of when to invoke results reporting. The file listening verticle will not
     * know when the last upsert of a source file of records is done, for example.
     */
    private Future<Void> persistBatch() {
        Promise<Void> promise = Promise.promise();
        BatchOfRecords batch = turnstile.viewCurrentBatch();
        if (batch != null) {
            if (batch.size() > 0) {
                updateClient.inventoryUpsert(batch.getUpsertRequestBody()).onSuccess(upsert -> {
                    fileProcessor.reporting.incrementRecordsProcessed(batch.size());
                    if (upsert.statusCode() == 207) {
                        batch.setResponse(upsert);
                        fileProcessor.reporting.reportErrors(batch)
                                .onFailure(err -> logger.error("Error logging upsert results " + err.getMessage()));
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
                }).onFailure(handler -> promise.fail("Fatal error: " + handler.getMessage()));
            } else if (batch.hasDeletingRecord()) {
                // Delete and complete the promise
                persistDeletion(batch, promise);
            } else { // we get here when the last set of records has exactly 100. We just need to report
                if (batch.isLastBatchOfFile()) {
                    reportEndOfFile();
                }
                promise.complete();
            }
        }
        return promise.future();
    }

    /**
     * Persists the deletion, complete the promise when done
     * @param batch The batch of records containing a deletion record
     * @param promise The promise of persistBatch
     */
    private void persistDeletion(BatchOfRecords batch, Promise<Void> promise) {
        updateClient.inventoryDeletion(batch.getDeletingRecord().getRecordAsJson().getJsonObject("delete"))
                .onComplete(deletion -> {
                    if (deletion.succeeded()) {
                        fileProcessor.reporting.incrementRecordsProcessed(1);
                        fileProcessor.reporting.incrementInventoryMetrics(new InventoryMetrics(deletion.result().getMetrics()));
                    } else if (deletion.cause().getMessage().startsWith("404")) {
                        fileProcessor.reporting.log("404 error deleting inventory records, record set not found.");
                    } else{
                        fileProcessor.reporting.log("Error making delete request to inventory: "+ deletion.cause().getMessage());
                    }
                    promise.complete();
                });
    }

    private void reportEndOfFile() {
        fileProcessor.reporting.endOfFile();
        boolean queueDone = fileProcessor.fileQueueDone(true);
        if (queueDone) {
            fileProcessor.reporting.endOfQueue();
        }
    }

    public boolean noPendingBatches(int idlingChecksThreshold) {
        return turnstile.isIdle(idlingChecksThreshold);
    }


    /** Class wrapping a blocking queue of one, acting as a turnstile for batches in order to persist them one
     * at a time with no overlap. */
    private static class Turnstile {

        private final BlockingQueue<BatchOfRecords> turnstile = new ArrayBlockingQueue<>(1);
        // Records the number of consecutive checks of whether the queue is idling.
        private final AtomicInteger turnstileEmptyChecks = new AtomicInteger(0);

        /**
         * Puts batch in blocking queue-of-one; process waits if previous batch still in queue.
         */
        private void enterBatch(BatchOfRecords batch) {
            try {
                turnstile.put(batch);
            } catch (InterruptedException ie) {
                logger.error("Putting next batch in queue-of-one interrupted: ");
                throw new RuntimeException("Putting next batch in queue-of-one interrupted: " + ie.getMessage());
            }
        }

        private void exitBatch() {
            try {
                turnstile.take();
            } catch (InterruptedException ie) {
                logger.error("Taking batch from queue-of-one interrupted: ");
                throw new RuntimeException("Taking batch from queue-of-one interrupted: " + ie.getMessage());
            }
        }

        private BatchOfRecords viewCurrentBatch() {
            return turnstile.peek();
        }

        private boolean isIdle(int idlingChecksThreshold) {
            if (turnstile.isEmpty()) {
                if (turnstileEmptyChecks.incrementAndGet() > idlingChecksThreshold) {
                    logger.info("Batch turnstile has been idle for " + idlingChecksThreshold + " consecutive checks.");
                    turnstileEmptyChecks.set(0);
                    return true;
                }
            } else {
                turnstileEmptyChecks.set(0);
            }
            return false;
        }

    }
}
