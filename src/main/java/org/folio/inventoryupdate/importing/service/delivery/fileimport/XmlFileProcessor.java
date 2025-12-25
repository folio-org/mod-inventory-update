package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.reporting.Reporting;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.transformation.XmlRecordsReader;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.transformation.XmlTransformationPipeline;

/**
 * File processing is made up of following components, listed in the order of processing.
 * <li>a queue of source files (in VertX file system, synchronous access)</li>
 * <li>a file listener (a verticle) that feeds files from the queue to the processor</li>
 * <li>a SAX parser splitting a file of records into individual XML records (synchronous)</li>
 * <li>an XSLT transformation pipeline and an XML to JSON converter, handling individual xml records (synchronous)</li>
 * <li>a client that collects records into sets of 100 JSON objects and pushes the result to Inventory Update, one
 * batch at a time (asynchronous)</li>
 * <p/>The import process additionally uses a logging component for reporting status and errors.
 */
public class XmlFileProcessor extends FileProcessor {

  public static final Logger logger = LogManager.getLogger("ImportJob");
  XmlTransformationPipeline transformationPipeline;
  RecordReceiver inventoryBatchUpdater;
  final Vertx vertx;

  public XmlFileProcessor(Vertx vertx, String tenant, UUID channelId) {
    this.vertx = vertx;
    this.channelId = channelId;
    this.configStorage = new EntityStorage(vertx, tenant);
    this.tenant = tenant;
  }

  /**
   * Creates processing pipeline. With
   * <li>XML transformation</li><li>XML-to-JSON conversion</li><li>batch upserting to inventory storage</li>
   * <li>reporting</li><li>a new job instance (ImportJob) for logging/reporting to the database</li>
   *
   * @param inventoryBatchUpdater component responsible for batch and putting transformed records to inventory
   * @return file processor with pipeline
   */
  public Future<XmlFileProcessor> withProcessingPipeline(String tenant, UUID channelId, Vertx vertx,
                                                         InventoryBatchUpdater inventoryBatchUpdater) {
    return configStorage.getEntity(channelId, new Channel())
        .map(cfg -> ((Channel) cfg).getTransformationId())
        .compose(transformationId -> XmlTransformationPipeline.create(vertx, tenant, transformationId))
        .compose(pipelineCreated -> {
          inventoryBatchUpdater.forFileProcessor(this);
          this.transformationPipeline = pipelineCreated.withTarget(inventoryBatchUpdater);
          this.inventoryBatchUpdater = inventoryBatchUpdater;
          this.reporting = new Reporting(this, tenant, vertx);
          return Future.succeededFuture(this);
        })
        .compose(p -> p.withJobLog(channelId));
  }

  private Future<XmlFileProcessor> withJobLog(UUID channelId) {
    return configStorage.getEntity(channelId, new Channel())
        .compose(channel -> {
          importJob = new ImportJob().initiate((Channel) channel.withCreatingUser(null));
          return configStorage.storeEntity(importJob);
        }).compose(v -> Future.succeededFuture(this));
  }

  @Override
  public XmlFileProcessor forFileListener(FileListener fileListener) {
    this.fileListener = fileListener;
    return this;
  }

  /**
   * Reads XML file and splits it into individual records that are forwarded to the transformation pipeline.
   *
   * @param xmlFile an XML file containing a `collection` of 0 or more `record`s
   * @return future completion of the file import
   */
  public Future<Void> processFile(File xmlFile) {
    Promise<Void> promise = Promise.promise();
    try {
      reporting.nowProcessing(xmlFile.getName());
      String xmlFileContents = Files.readString(xmlFile.toPath(), StandardCharsets.UTF_8);
      vertx.executeBlocking(new XmlRecordsReader(xmlFileContents, transformationPipeline), true)
          .onComplete(processing -> {
            if (processing.succeeded()) {
              promise.complete();
            } else {
              logger.error("Processing failed with {}", processing.cause().getMessage());
              halt("Processing failed with " + processing.cause().getMessage());
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
    if (transformationPipeline.getRecordsProcessed() > 0 && inventoryBatchUpdater.getRecordsProcessed() > 0) {
      stats += " Transformation: "
          + (transformationPipeline.getRecordsProcessed() * 1000000000L / transformationPipeline.getProcessingTime())
          + " recs/s."
          + " Upserting: "
          + (inventoryBatchUpdater.getRecordsProcessed() * 1000000000L / inventoryBatchUpdater.getProcessingTime())
          + " recs/s.";
    }
    return stats;
  }
}
