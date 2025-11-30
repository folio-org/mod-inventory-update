package org.folio.inventoryupdate.importing.service.fileimport;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

import java.io.File;
import java.util.UUID;

/**
 * Listens for incoming source files in a file queue and forwards them to a file processor for splitting, transformation, and ingestion.
 * <p/>Will instantiate a new job (with job ID, start timestamp etc.) if there was no active job already when the file was
 * picked up. Conversely, if an import job was already in progress, the file is considered yet another part of that job.
 */
public class XmlFileListener extends FileListener {


  public static final Logger logger = LogManager.getLogger("queued-files-processing");

  public XmlFileListener(ServiceRequest request, String importConfigurationId) {
    this.tenant = request.tenant();
    this.importConfigurationId = UUID.fromString(importConfigurationId);
    this.routingContext = request.routingContext();
    this.fileQueue = new FileQueue(request, importConfigurationId);
  }

  @Override
  public Future<?> start() throws Exception {
    logger.info("Listening for files to forward for processing by job configuration ID [{}}], tenant [{}}].", importConfigurationId, tenant);
    vertx.setPeriodic(200, r -> listen());
    return super.start();
  }

  @Override
  protected void listen() {
    if (!importJobPaused()) {
      boolean processorResuming = (fileProcessor != null && fileProcessor.isResuming(false));
      File currentFile = getNextFileIfPossible(fileQueuePassive.get(), processorResuming);
      if (currentFile != null) {  // null if queue is empty or a previous file is still processing
        boolean queueWentFromPassiveToActive = fileQueuePassive.getAndSet(false); // check if job was passive before this file
        // Continue existing job if any (not activating), or instantiate a new (activating).
        getFileProcessor(queueWentFromPassiveToActive)
            .compose(fileProcessor -> fileProcessor.processFile(currentFile))
            .onComplete(na -> {
              if (!importJobPaused()) { // keep file to resume
                fileQueue.deleteFile(currentFile);
              }
            })
            .onFailure(f -> logger.error("Error processing file: {}", f.getMessage()));
      }
    }
  }


  public Future<FileProcessor> getFileProcessor(boolean instantiate) {
    if (instantiate) {
      return new XmlFileProcessor(vertx, tenant, importConfigurationId)
          .forFileListener(this)
          .withProcessingPipeline(tenant, importConfigurationId, vertx, new InventoryBatchUpdater(routingContext))
          .compose(newFileProcessor -> {
            this.fileProcessor = newFileProcessor;
            return Future.succeededFuture(fileProcessor);
          });
    } else {
      return Future.succeededFuture(fileProcessor);
    }
  }

}
