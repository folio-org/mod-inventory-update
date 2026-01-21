package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import java.io.File;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.reporting.Reporting;
import org.folio.inventoryupdate.importing.utils.SettableClock;

public abstract class FileProcessor {
  UUID channelId;
  ImportJob importJob;
  Reporting reporting;
  FileListener fileListener;
  boolean paused = false;
  AtomicBoolean running = new AtomicBoolean(false);
  AtomicBoolean resuming = new AtomicBoolean(false);
  EntityStorage configStorage;
  String tenant;

  public FileProcessor running() {
    running.set(true);
    return this;
  }

  /**
   * Sets the file processor that the file listener forwards files to.
   */
  public FileProcessor forFileListener(FileListener fileListener) {
    this.fileListener = fileListener;
    return this;
  }

  public abstract Future<Void> processFile(File file);

  public boolean paused() {
    return paused;
  }

  public boolean isResuming(boolean newValue) {
    return resuming.getAndSet(newValue);
  }

  public void pause() {
    paused = true;
    importJob.logStatus(ImportJob.JobStatus.PAUSED,
        "On operator's request", reporting.getRecordsProcessed(), configStorage);
    reporting.log("Job paused");
    reporting.reportFileStats();
  }

  public void resume() {
    importJob.logStatus(ImportJob.JobStatus.RUNNING, "", reporting.getRecordsProcessed(), configStorage);
    paused = false;
    isResuming(true);
  }

  public ImportJob getImportJob() {
    return importJob;
  }

  public UUID getImportConfigId() {
    return channelId;
  }

  public void halt(String errorMessage) {
    paused = true;
    reporting.log(errorMessage);
    importJob.logStatus(ImportJob.JobStatus.PAUSED, errorMessage, reporting.getRecordsProcessed(), configStorage);
    reporting.reportFileStats();
    reporting.reportFileQueueStats(false);
  }

  public boolean fileQueueDone(boolean atEndOfCurrentFile) {
    if (atEndOfCurrentFile && fileListener.queueIsEmpty() && !reporting.pendingFileStats()) {
      fileListener.markFileQueuePassive();
    }
    return fileListener.fileQueueIsPassive();
  }

  public void logFinish(int recordCount) {
    importJob.logFinish(SettableClock.getLocalDateTime(), recordCount, configStorage);
  }

  public abstract String getStats();
}
