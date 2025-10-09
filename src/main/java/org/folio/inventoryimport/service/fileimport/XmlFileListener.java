package org.folio.inventoryimport.service.fileimport;

import io.vertx.core.*;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryimport.service.ServiceRequest;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Listens for incoming source files in a file queue and forwards them to a file processor for splitting, transformation, and ingestion.
 * <p/>Will instantiate a new job (with job ID, start timestamp etc.) if there was no active job already when the file was
 * picked up. Conversely, if an import job was already in progress, the file is considered yet another part of that job.
 */
public class XmlFileListener extends FileListener {

    private final String tenant;
    private final UUID importConfigurationId;
    private final RoutingContext routingContext;

    public static final Logger logger = LogManager.getLogger("queued-files-processing");

    public XmlFileListener(ServiceRequest request, String importConfigurationId) {
        this.tenant = request.tenant();
        this.importConfigurationId = UUID.fromString(importConfigurationId);
        this.routingContext = request.routingContext();
        this.fileQueue = new FileQueue(request, importConfigurationId);
    }

    public static Future<String> deployIfNotDeployed(ServiceRequest request, String importConfigurationId) {
        Promise<String> promise = Promise.promise();
        FileListener fileListener = FileListeners.getFileListener(request.tenant(), importConfigurationId);
        if (fileListener == null) {
            Verticle verticle = FileListeners.addFileListener(request.tenant(), importConfigurationId, new XmlFileListener(request, importConfigurationId));
            request.vertx().deployVerticle(verticle,
                    new DeploymentOptions().setWorkerPoolSize(1).setMaxWorkerExecuteTime(10).setMaxWorkerExecuteTimeUnit(TimeUnit.MINUTES)).onComplete(
                    started -> {
                        if (started.succeeded()) {
                            logger.info("Started verticle [" + started.result() + "] for [" + request.tenant() + "] and configuration ID [" + importConfigurationId + "].");
                            promise.complete("Started verticle [" + started.result() + "] for configuration ID [" + importConfigurationId + "].");
                        } else {
                            logger.error("Couldn't start file processor verticle for tenant [" + request.tenant() + "] and import configuration ID [" + importConfigurationId + "].");
                            promise.fail("Couldn't start file processor verticle for import configuration ID [" + importConfigurationId + "].");
                        }
                    });
        } else {
            promise.complete("File listener already created for import configuration ID [" + importConfigurationId + "].");
        }
        return promise.future();
    }

    @Override
    public void start() {
        logger.info("Listening for files to forward for processing by job configuration ID [" + importConfigurationId + "], tenant [ " + tenant + "] .");
        vertx.setPeriodic(200, (r) -> {
            if (!importJobPaused()) {
                File currentFile = getNextFileIfPossible();
                if (currentFile != null) {  // null if queue is empty or a previous file is still processing
                    boolean activating = fileQueuePassive.getAndSet(false); // check if job was passive before this file
                    // Continue existing job if any (not activating), or instantiate a new (activating).
                    getFileProcessor(activating)
                            .compose(job -> job.processFile(currentFile))
                            .onComplete(na -> {
                                if (!importJobPaused()) { // keep file to resume
                                    fileQueue.deleteFile(currentFile);
                                }
                            })
                            .onFailure(f -> logger.error("Error processing file: " + f.getMessage()));
                }
            }
        });
    }

    private boolean importJobPaused() {
        return fileProcessor != null && fileProcessor.paused();
    }

    public File getNextFileIfPossible () {
        if (fileProcessor != null && fileProcessor.resumeHaltedProcessing()) {
            return fileQueue.currentlyPromotedFile();
        } else {
            return fileQueue.nextFileIfPossible();
        }
    }

    /**
     * Instantiates a new import job unless there is an existing import process in progress as indicated by the parameter.
     * @param activating true if new job must be initialized, false to continue with existing job.
     * @return new or previously initialized file processor
     */
    public Future<FileProcessor> getFileProcessor(boolean activating) {
        if (activating) {
            return FileProcessor.initiateJob(tenant, importConfigurationId, this, vertx, routingContext)
                    .compose(job -> {
                        fileProcessor = job;
                        return Future.succeededFuture(fileProcessor);
                    });
        } else {
            return Future.succeededFuture(fileProcessor);
        }
    }
}