package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.ThreadingModel;
import io.vertx.core.VerticleBase;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.service.ImportService;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

public abstract class FileListener extends VerticleBase {

  public static final Logger logger = LogManager.getLogger("queued-files-processing");

  protected String tenant;
  protected Channel channel;
  protected RoutingContext routingContext;
  protected FileProcessor fileProcessor;
  protected FileQueue fileQueue;
  protected Vertx deploymentVertx;
  protected WebClient webClient;

  // For demarcating jobs by start/end
  protected AtomicBoolean fileQueuePassive = new AtomicBoolean(true);

  public Vertx getVertx() {
    return vertx;
  }

  public FileProcessor getProcessor() {
    return fileProcessor;
  }

  public UUID getConfigId() {
    return channel.getId();
  }

  public void markFileQueuePassive() {
    fileQueuePassive.set(true);
  }

  public boolean fileQueueIsPassive() {
    return fileQueuePassive.get();
  }

  public Future<Boolean> queueIsEmpty() {
    return fileQueue.isEmpty();
  }

  public abstract void listen();

  /**
   * Gets existing file processor or instantiates a new one.
   *
   * @param activating true if new job must be initialized, false to continue with existing processor.
   * @return new or previously initialized file processor
   */
  public abstract Future<FileProcessor> getFileProcessor(boolean activating);

  /**
   * Get next file from queue unless there is already a file in the processing slot.<br/>
   * Exception: Normally, a file in the processing slot will be a currently processing file that should be
   * waited for to finish. However, if this is an activation of a new job or is a resumption of a paused job,
   * then it is assumed that this file is from a past, interrupted run and that it should be re-processed.
   *
   * @return next file from filesystem queue
   *   <li>except, if there is already a file currently processing: returns null</li>
   *   <li>except, if the process is being newly activated or resumed, then returns the currently promoted file after
   *   all, to restart processing with that</li>
   *   <li>except, if there is no promoted file and no files in queue: returns null.</li>
   */
  public Future<SourceFile> getNextFileIfPossible(boolean fileQueuePassive, boolean processorResuming) {
    return fileQueue.hasFileInProcess()
        .compose(inProcess -> {
          if (inProcess && (fileQueuePassive || processorResuming)) {
            return fileQueue.currentlyPromotedFile();
          } else {
            return fileQueue.promoteAndGetNextFileIfPossible();
          }
        });
  }

  public boolean importJobPaused() {
    return fileProcessor != null && fileProcessor.paused();
  }

  public Future<String> deploy() {
    Promise<String> promise = Promise.promise();
    deploymentVertx.deployVerticle(this,
        new DeploymentOptions()
            .setWorkerPoolSize(4)
            .setInstances(1)
            .setMaxWorkerExecuteTime(10)
            .setThreadingModel(ThreadingModel.WORKER)
            .setMaxWorkerExecuteTimeUnit(TimeUnit.MINUTES)).onComplete(started -> {
              if (started.succeeded()) {
                logger.info("Started verticle [{}] on Vertx {} for [{}] and channel [{}].",
                    started.result(), vertx, tenant, channel.getRecord().name());
                promise.complete(started.result());
              } else {
                logger.error("Couldn't start file processor verticle for tenant [{}] and channel ID [{}].",
                    tenant, channel.getRecord().name());
                promise.fail("Couldn't launch file processor for channel [" + channel.getRecord().name() + "].");
              }
            });
    return promise.future();
  }

  public static Future<String> deployIfNotDeployed(ServiceRequest request, Channel channel) {
    if (channel == null || channel.getId() == null) {
      return Future.succeededFuture("No channel provided to deploy.");
    } else {
      boolean retainQueueIfAny = "true".equalsIgnoreCase(request.requestParam("retainQueue"));
      FileQueue fq = ImportService.getFileQueue(request, channel.getId());
      // Request parameter can override what is set on the channel record
      boolean listening = request.requestParam("listening") == null
          ? channel.isListeningIfEnabled()
          : !"false".equalsIgnoreCase(request.requestParam("listening"));
      logger.info("Channels deployment ID: {}. Deployed verticle IDs {} ", channel.getDeploymentId(), request.vertx().deploymentIDs());
      if (!channel.hasDeploymentId() || !request.vertx().deploymentIDs().contains(channel.getDeploymentId())) {
        logger.info("Deploying verticle for channel {}", channel.getName());
        return channel.setEnabledListening(true, listening, request.entityStorage())
            .compose(na -> fq.initialize(retainQueueIfAny).mapEmpty())
            .compose(na -> new ImportJob().changeRunningToInterruptedByChannelId(request.entityStorage(),
                channel.getId()))
            .compose(jobsInterrupted -> {
              String jobsMarkedInterrupted = jobsInterrupted > 0
                  ? jobsInterrupted + " previous job was marked 'RUNNING', now marked 'INTERRUPTED'. " : "";
              return new XmlFileListener(request, channel).deploy()
                  .compose(id -> channel.setDeploymentId(id, request.entityStorage()))
                  .map(resp -> jobsMarkedInterrupted + resp);
            });
      } else {
        return Future.succeededFuture(
            "File listener already commissioned for channel [" + channel.getName() + "].");
      }
    }
  }

  /**
   * If a verticle is deployed for the channel, un-deploys the verticle, deletes the file queue,
   * and de-registers the channel from static list of deployed verticles.
   *
   * @return statement about the outcome of the operation
   */
  public static Future<String> undeployIfDeployed(ServiceRequest request, Channel channel) {
    if (channel == null || channel.getId() == null) {
      return Future.succeededFuture("No channel provided to undeploy.");
    }
    boolean retainQueue = "true".equalsIgnoreCase(request.requestParam("retainQueue"));
    if (channel.hasDeploymentId() && request.vertx().deploymentIDs().contains(channel.getDeploymentId())) {
      return channel.setEnabledListening(false, channel.isListeningIfEnabled(), request.entityStorage())
          .compose(na -> request.vertx().undeploy(channel.getDeploymentId()))
          .compose(na -> channel.setDeploymentId("", request.entityStorage()))
          .map(na -> {
            ImportService.getFileQueue(request, channel.getId()).initialize(retainQueue);
            return channel.getId();
          }).map("Channel decommissioned." + channel.getRecord().name());
    } else {
      return Future.succeededFuture(
          "Did not find channel [" + channel.getName() + "] in list of commissioned channels.");
    }
  }
}
