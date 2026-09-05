package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.atomic.AtomicBoolean;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.service.ImportService;
import org.folio.inventoryupdate.importing.service.Messaging;
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
    this.webClient = request.webClient();
    this.deploymentVertx = request.vertx();
  }

  @Override
  public Future<?> start() throws Exception {
    logCtx();
    logger.info("Starting verticle with deployment ID {}, listening for files "
            + "for channel {} [{}], tenant [{}}].",
        deploymentID(), channel.getName(), channel.getId(), tenant);

    listen();

    Messaging.consumeChannelUpdates(vertx, channel.getId().toString(),
        channelAsJson -> handleChannelUpdate(channelAsJson.body()));

    Messaging.consumeImportJobCommands(vertx, channel.getId().toString(),
        command -> handleImportJobCommand(command.body()));

    return super.start()
        .compose(na -> channel.setDeploymentId(deploymentID(), new EntityStorage(vertx, tenant)));
  }

  public void handleImportJobCommand(JsonObject command) {
    if (command != null && command.containsKey("command")) {
      if (command.getString("command").equalsIgnoreCase("pause")) {
        if (getProcessor().paused()) {
          logger.info("Received pause command but job is already paused");
        } else {
          getProcessor().pause();
        }
      } else if (command.getString("command").equalsIgnoreCase("resume")) {
        getProcessor().resume(command.getBoolean("discardFileInProcess"));
      }
    }
  }

  public void handleChannelUpdate(JsonObject channelAsJson) {
    this.channel = new Channel().fromJson(channelAsJson);
    if (!channel.getDeploymentId().equals(deploymentID())) {
      logger.warn("Mismatch between the deployment id registered on the channel ({}) "
          + "and the deployment ID of this verticle ({}) servicing the channel",
          channel.getDeploymentId(), deploymentID());
    }
  }

  public Future<?> stop() throws Exception {
    logger.info("Verticle with deployment ID {} for channel {} ({}) stopping.",
        deploymentID(), channel.getName(), channel.getId());
    return super.stop()
        .compose(na -> channel.setDeploymentId("", new EntityStorage(vertx, tenant)));
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
          .withProcessingPipeline(tenant, getConfigId(), vertx, new InventoryBatchUpdater(webClient, routingContext))
          .compose(newFileProcessor -> {
            this.fileProcessor = newFileProcessor.running();
            return Future.succeededFuture(fileProcessor);
          });
    } else {
      return Future.succeededFuture(fileProcessor);
    }
  }
}
