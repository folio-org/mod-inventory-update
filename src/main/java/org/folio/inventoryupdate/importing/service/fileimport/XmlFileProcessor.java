package org.folio.inventoryupdate.importing.service.fileimport;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.ImportConfig;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryupdate.importing.service.fileimport.reporting.Reporting;
import org.folio.inventoryupdate.importing.service.fileimport.transformation.TransformationPipeline;
import org.folio.inventoryupdate.importing.service.fileimport.transformation.XmlRecordsReader;

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
public class XmlFileProcessor extends FileProcessor {
    TransformationPipeline transformationPipeline;
    InventoryBatchUpdater inventoryBatchUpdater;
    final Vertx vertx;

    public static final Logger logger = LogManager.getLogger("ImportJob");

    public XmlFileProcessor(Vertx vertx, String tenant, UUID importConfigId) {
        this.vertx = vertx;
        this.importConfigId = importConfigId;
        this.configStorage = new ModuleStorageAccess(vertx, tenant);
        this.reporting = new Reporting(this, tenant, vertx);
        this.tenant = tenant;
    }


    private Future<Void> withJobLog(UUID importConfigId) {
        return configStorage.getEntity(importConfigId, new ImportConfig())
                .compose(importConfig -> {
                    importJob = new ImportJob().initiate((ImportConfig) importConfig);
                    return configStorage.storeEntity(importJob.withCreatingUser(null));
                }).mapEmpty();
    }

    private Future<TransformationPipeline> withTransformationPipeline(String tenant, UUID importConfigId, Vertx vertx) {
        return new ModuleStorageAccess(vertx, tenant).getEntity(importConfigId,new ImportConfig())
                .map(cfg -> ((ImportConfig) cfg).getRecord().transformationId())
                .compose(transformationId -> TransformationPipeline.create(vertx, tenant, transformationId))
                .onComplete(pipelineCreated -> transformationPipeline = pipelineCreated.result());
    }

    public XmlFileProcessor withInventoryBatchUpdater(RoutingContext routingContext) {
      inventoryBatchUpdater = new InventoryBatchUpdater(routingContext).forFileProcessor(this);
      return this;
    }

    /**
     * Attaches a file processor to the file listener, a job log and a transformation pipeline to the file processor,
     * and an inventory batch updater to the transformation pipeline.
     * @return a file processor for the listener, with a transformation pipeline and an inventory updater.
     */
    public Future<XmlFileProcessor> initiateJob() {
        return this.withJobLog(this.importConfigId)
                        .compose(v -> withTransformationPipeline(tenant, importConfigId, vertx))
                        .compose(transformer -> {
                            transformer.withTarget(inventoryBatchUpdater);
                            return Future.succeededFuture(this);
                        });
    }

    @Override
    public XmlFileProcessor forFileListener(FileListener fileListener) {
      this.fileListener = fileListener;
      return this;
    }

    /**
     * Reads XML file and splits it into individual records that are forwarded to the transformation pipeline.
     * @param xmlFile an XML file containing a `collection` of 0 or more `record`s
     * @return future completion of the file import
     */
    public Future<Void> processFile(File xmlFile) {
        Promise<Void> promise = Promise.promise();
        try {
            reporting.nowProcessing(xmlFile.getName());
            String xmlFileContents = Files.readString(xmlFile.toPath(), StandardCharsets.UTF_8);
            vertx.executeBlocking(new XmlRecordsReader(xmlFileContents, transformationPipeline),true)
                    .onComplete(processing -> {
                                if (processing.succeeded()) {
                                    promise.complete();
                                } else {
                                    logger.error("Processing failed with {}", processing.cause().getMessage());
                                    promise.complete();
                                }
                            });

        } catch (IOException e) {
            promise.fail("Could not open XML source file for importing " + e.getMessage());
        }
        return promise.future();
    }

    public String getStats() {
      String stats = "Transformation, records processed: " + transformationPipeline.getRecordsProcessed()
          + ", Upserting, records processed: " + inventoryBatchUpdater.getRecordsProcessed()
          + ".";
      if (transformationPipeline.getRecordsProcessed()>0 && inventoryBatchUpdater.getRecordsProcessed()>0) {
        stats += " Transformation: "
            + (transformationPipeline.getRecordsProcessed() * 1000L / transformationPipeline.getProcessingTime()) + " recs/s."
            + " Upserting: "
            + (inventoryBatchUpdater.getRecordsProcessed() * 1000L / inventoryBatchUpdater.getProcessingTime()) + " recs/s.";
      }
      return stats;
    }
}
