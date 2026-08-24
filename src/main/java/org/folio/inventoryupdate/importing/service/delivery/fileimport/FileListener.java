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

  public Future<Void> undeploy() {
    return deploymentVertx.undeploy(deploymentID());
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
                promise.complete("Started verticle [" + started.result() + "] for channel ID ["
                    + channel.getRecord().name() + "].");
              } else {
                logger.error("Couldn't start file processor verticle for tenant [{}] and channel ID [{}].",
                    tenant, channel.getRecord().name());
                promise.fail("Couldn't launch file processor for channel [" + channel.getRecord().name() + "].");
              }
            });
    return promise.future();
  }
}
