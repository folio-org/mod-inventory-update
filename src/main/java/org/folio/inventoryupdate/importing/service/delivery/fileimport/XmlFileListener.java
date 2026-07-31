package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.service.ImportService;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

/**
 * Listens for incoming source files in a file queue and forwards them to a file processor for splitting,
 * transformation, and ingestion.
 * <p/>Will instantiate a new job (with job ID, start timestamp etc.) if there was no active job already when the
 * file was picked up. Conversely, if an import job was already in progress, the file is considered yet another
 * part of that job.
 */
public class XmlFileListener extends FileListener {

  public XmlFileListener(ServiceRequest request, Channel channel) {
    this.tenant = request.tenant();
    this.channel = channel;
    this.routingContext = request.routingContext();
    this.fileQueue = ImportService.getFileQueue(request, getConfigId());
  }

  @Override
  public Future<?> start() throws Exception {
    logger.info("Listening for files to forward for processing by job configuration ID [{}}], tenant [{}}].",
        getConfigId(), tenant);
    listen();
    return super.start();
  }

  public boolean isListening() {
    return channel.getRecord().listening();
  }

  @Override
  public void listen() {
    AtomicBoolean clear = new AtomicBoolean(true);
    vertx.setPeriodic(200, r -> {
      if (isListening() && !importJobPaused() && clear.get()) {
        clear.set(false);
        boolean processorResuming = fileProcessor != null && fileProcessor.isResuming(false);
        getNextFileIfPossible(fileQueuePassive.get(), processorResuming)
            .onFailure(f -> logger.error("Error when maybe fetching next file {}", f.getMessage()))
            .compose(currentFile -> {
              if (currentFile != null) {  // null if queue is either empty or already has a file in progress
                boolean queueWentFromPassiveToActive = fileQueuePassive.getAndSet(false);
                // Continue existing job if any (= not activating), or instantiate a new (= activating).
                return getFileProcessor(queueWentFromPassiveToActive)
                    .compose(fileProcessor -> fileProcessor.processFile(currentFile))
                    .compose(na -> {
                      if (!importJobPaused()) { // if paused mid-file, keep file to resume
                        return currentFile.discard().mapEmpty();
                      } else {
                        return Future.succeededFuture(null);
                      }
                    })
                    .onFailure(f -> logger.error("Error processing file: {}", f.getMessage()));
              } else {
                return Future.succeededFuture(null);
              }
            }).andThen(na -> clear.set(true));
      }
    });
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
