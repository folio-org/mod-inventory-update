package org.folio.inventoryupdate.importing.service.fileimport;

import io.vertx.core.Future;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

import java.io.File;

/**
 * Listens for incoming source files in a file queue and forwards them to a file processor for splitting, transformation, and ingestion.
 * <p/>Will instantiate a new job (with job ID, start timestamp etc.) if there was no active job already when the file was
 * picked up. Conversely, if an import job was already in progress, the file is considered yet another part of that job.
 */
public class XmlFileListener extends FileListener {


  public XmlFileListener(ServiceRequest request, Channel channel) {
    this.tenant = request.tenant();
    this.channel = channel;
    this.routingContext = request.routingContext();
    this.fileQueue = new FileQueue(request, getConfigIdStr());
  }

  @Override
  public Future<?> start() throws Exception {
    logger.info("Listening for files to forward for processing by job configuration ID [{}}], tenant [{}}].", getConfigIdStr(), tenant);
    listen();
    return super.start();
  }

  public boolean isListening() {
    return channel.getRecord().listening();
  }

  @Override
  public void listen() {
    vertx.setPeriodic(200, r -> {
          if (isListening() && !importJobPaused()) {
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
    });
  }

  public Future<FileProcessor> getFileProcessor(boolean instantiate) {
    if (instantiate) {
      logger.info("Get file processor on vertx [" + vertx + "]");
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
