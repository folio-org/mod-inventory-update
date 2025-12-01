package org.folio.inventoryupdate.importing.service.fileimport;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.ImportConfig;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Listens for incoming source files in a file queue and forwards them to a file processor for splitting, transformation, and ingestion.
 * <p/>Will instantiate a new job (with job ID, start timestamp etc.) if there was no active job already when the file was
 * picked up. Conversely, if an import job was already in progress, the file is considered yet another part of that job.
 */
public class XmlFileListener extends FileListener {


  public static final Logger logger = LogManager.getLogger("queued-files-processing");
  private final AtomicLong timerId = new AtomicLong(-1L);

  public XmlFileListener(ServiceRequest request, ImportConfig importConfig) {
    this.tenant = request.tenant();
    this.importConfig = importConfig;
    this.routingContext = request.routingContext();
    this.fileQueue = new FileQueue(request, getConfigIdStr());
  }

  @Override
  public Future<?> start() throws Exception {
    logger.info("Listening for files to forward for processing by job configuration ID [{}}], tenant [{}}].", getConfigIdStr(), tenant);
    listen();
    return super.start();
  }

  public boolean listenerEnabled() {
    return importConfig.getRecord().enabled();
  }

  public boolean hasTimerId() {
    return timerId.get()>0;
  }

  public long resetTimerId() {
    return timerId.getAndSet(-1L);
  }

  @Override
  public void listen() {
    if (hasTimerId()) {
      long oldTimerId = timerId.get();
      logger.warn("There is already a timer ({}) for the listener for {} ", oldTimerId, getConfigName());
      boolean cancelled = vertx.cancelTimer(resetTimerId());
      logger.warn(cancelled ? "Timer {} cancelled." : "No active timer found with ID {} to cancel though.", oldTimerId);
    }
    timerId.set(
        vertx.setPeriodic(200, r -> {
          if (listenerEnabled() && !importJobPaused()) {
            boolean processorResuming = (fileProcessor != null && fileProcessor.isResuming(false));
            File currentFile = getNextFileIfPossible(fileQueuePassive.get(), processorResuming);
            if (currentFile != null) {  // null if queue is empty or a previous file is still processing
              boolean queueWentFromPassiveToActive = fileQueuePassive.getAndSet(false); // check if job was passive before this file
              // Continue existing job if any (not activating), or instantiate a new (activating).
              getFileProcessor(queueWentFromPassiveToActive)
                  .compose(fileProcessor -> fileProcessor.processFile(currentFile))
                  .onComplete(na -> {
                    if (!importJobPaused()) { // if paused mid-file, keep file to resume
                      fileQueue.deleteFile(currentFile);
                    }
                  })
                  .onFailure(f -> logger.error("Error processing file: {}", f.getMessage()));
            }
          }
        })
    );
  }

  public void cancelListen() {
    long oldTimerId = timerId.get();
    if (hasTimerId()) {
      boolean cancelled = vertx.cancelTimer(resetTimerId());
      if (cancelled) {
        logger.info("Cancelled listener {} for {} ", oldTimerId, importConfig.getRecord().name());
        fileProcessor.halt("File listener deactivated.");
        markFileQueuePassive();
      } else {
        logger.warn("Listener to cancel ({}) not found for {}", oldTimerId, getConfigName());
      }
    } else {
      logger.warn("No listening timer found for {} ", getConfigName());
    }
  }

  public Future<FileProcessor> getFileProcessor(boolean instantiate) {
    if (instantiate) {
      return new XmlFileProcessor(vertx, tenant, getConfigId())
          .forFileListener(this)
          .withProcessingPipeline(tenant, getConfigId(), vertx, new InventoryBatchUpdater(routingContext))
          .compose(newFileProcessor -> {
            this.fileProcessor = newFileProcessor.running();
            return Future.succeededFuture(fileProcessor);
          });
    } else {
      return Future.succeededFuture(fileProcessor);
    }
  }

}
