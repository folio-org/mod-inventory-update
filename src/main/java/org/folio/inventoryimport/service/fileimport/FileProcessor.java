package org.folio.inventoryimport.service.fileimport;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryimport.moduledata.ImportConfig;
import org.folio.inventoryimport.moduledata.ImportJob;
import org.folio.inventoryimport.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryimport.service.fileimport.reporting.Reporting;
import org.folio.inventoryimport.service.fileimport.transformation.TransformationPipeline;
import org.folio.inventoryimport.service.fileimport.transformation.XmlRecordsReader;
import org.folio.inventoryimport.utils.SettableClock;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

/**
 * File processing is made up of following components, listed in the order of processing
 *   <li>a queue of source files (in VertX file system, synchronous access)</li>
 *   <li>a file listener (a verticle) that feeds files from the queue to the processor</li>
 *   <li>a SAX parser splitting a file of records into individual xml records (synchronous)</li>
 *   <li>an XSLT transformation pipeline and an XML to JSON converter, handling individual xml records (synchronous)</li>
 *   <li>a client that collects records into sets of 100 json objects and pushes the result to Inventory Update, one batch at a time (asynchronous)</li>
 * <p/>The import process additionally uses a logging component for reporting status and errors.
 */
public class FileProcessor {
    final UUID importConfigId;
    ImportJob importJob;
    Reporting reporting;
    FileListener fileListener;
    TransformationPipeline transformationPipeline;
    final Vertx vertx;
    final ModuleStorageAccess configStorage;

    private boolean paused = false;

    public static final Logger logger = LogManager.getLogger("ImportJob");

    private FileProcessor(Vertx vertx, String tenant, UUID importConfigId) {
        this.vertx = vertx;
        this.importConfigId = importConfigId;
        this.configStorage = new ModuleStorageAccess(vertx, tenant);
        this.reporting = new Reporting(this, tenant, vertx);
    }

    /**
     * Sets the file processor that the file listener forwards files to.
     */
    public FileProcessor forFileListener(FileListener fileListener) {
        this.fileListener = fileListener;
        return this;
    }

    private Future<Void> withJobLog(UUID importConfigId) {
        return configStorage.getEntity(importConfigId, new ImportConfig())
                .compose(importConfig -> {
                    importJob = new ImportJob().initiate((ImportConfig) importConfig);
                    return configStorage.storeEntity(importJob);
                }).mapEmpty();
    }

    private Future<TransformationPipeline> withTransformationPipeline(String tenant, UUID importConfigId, Vertx vertx) {
        return new ModuleStorageAccess(vertx, tenant).getEntity(importConfigId,new ImportConfig())
                .map(cfg -> ((ImportConfig) cfg).record.transformationId())
                .compose(transformationId -> TransformationPipeline.create(vertx, tenant, transformationId))
                .onComplete(pipelineCreated -> transformationPipeline = pipelineCreated.result());
    }

    /**
     * Attaches a file processor to the file listener, a job log and a transformation pipeline to the file processor,
     * and an inventory batch updater to the transformation pipeline.
     * @return a file processor for the listener, with a transformation pipeline and an inventory updater.
     */
    public static Future<FileProcessor> initiateJob(String tenant, UUID jobConfigId, FileListener fileListener, Vertx vertx,
                                                    RoutingContext routingContext) {
        return Future.succeededFuture(new FileProcessor(vertx, tenant, jobConfigId).forFileListener(fileListener))
                .compose(processor -> processor.withJobLog(jobConfigId)
                        .compose(v -> processor.withTransformationPipeline(tenant, jobConfigId, vertx))
                        .compose(transformer -> {
                            transformer.withTarget(new InventoryBatchUpdater(routingContext).forFileProcessor(processor));
                            return Future.succeededFuture(processor);
                        })
                );
    }

    public boolean fileQueueDone(boolean atEndOfCurrentFile) {
        if (atEndOfCurrentFile && fileListener.fileQueueIsEmpty() && !reporting.pendingFileStats()) {
            fileListener.markFileQueuePassive();
        }
        return fileListener.fileQueueIsPassive();
    }

    public void logFinish(int recordCount) {
        importJob.logFinish(SettableClock.getLocalDateTime(), recordCount, configStorage);
    }

    /**
     * Reads XML file and splits it into individual records that are forwarded to the transformation pipeline.
     * @param xmlFile an XML file containing a `collection` of 0 or more `record`s
     * @return future completion of the file import
     */
    Future<Void> processFile(File xmlFile) {
        Promise<Void> promise = Promise.promise();
        try {
            reporting.nowProcessing(xmlFile.getName());
            String xmlFileContents = Files.readString(xmlFile.toPath(), StandardCharsets.UTF_8);
            vertx.executeBlocking(new XmlRecordsReader(xmlFileContents, transformationPipeline))
                    .onComplete(processing -> {
                                if (processing.succeeded()) {
                                    promise.complete();
                                } else {
                                    logger.error("Processing failed with " + processing.cause().getMessage());
                                    promise.complete();
                                }
                            });

        } catch (IOException e) {
            promise.fail("Could not open XML source file for importing " + e.getMessage());
        }
        return promise.future();
    }

    /**
     * If there's a file in the processing slot but no activity in the inventory updater, the current job
     * is assumed to be in a paused state, which could for example be due to a module restart.
     * @return true if there's a file ostensibly processing but no activity detected in inventory updater
     * for `idlingChecksThreshold` consecutive checks
     */
    public boolean resumeHaltedProcessing() {
        return fileListener.processingSlotIsOccupied() && transformationPipeline.getUpdater().noPendingBatches(10);
    }

    public boolean paused() {
        return paused;
    }

    public void pause() {
        paused = true;
        importJob.logStatus(ImportJob.JobStatus.PAUSED,reporting.getRecordsProcessed(),configStorage);
        reporting.log("Job paused");
        reporting.reportFileStats();
    }

    public void resume() {
        importJob.logStatus(ImportJob.JobStatus.RUNNING,reporting.getRecordsProcessed(),configStorage);
        paused = false;
    }

    public ImportJob getImportJob() {
        return importJob;
    }

    public UUID getImportConfigId() {
        return importConfigId;
    }
}
