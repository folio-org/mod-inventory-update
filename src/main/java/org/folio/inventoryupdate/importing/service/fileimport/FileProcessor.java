package org.folio.inventoryupdate.importing.service.fileimport;

import io.vertx.core.Future;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryupdate.importing.service.fileimport.reporting.Reporting;
import org.folio.inventoryupdate.importing.utils.SettableClock;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class FileProcessor {
  UUID importConfigId;
  ImportJob importJob;
  Reporting reporting;
  FileListener fileListener;
  boolean paused = false;
  AtomicBoolean resuming = new AtomicBoolean(false);
  ModuleStorageAccess configStorage;
  String tenant;



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
    importJob.logStatus(ImportJob.JobStatus.PAUSED,reporting.getRecordsProcessed(),configStorage);
    reporting.log("Job paused");
    reporting.reportFileStats();
  }

  public void resume() {
    importJob.logStatus(ImportJob.JobStatus.RUNNING,reporting.getRecordsProcessed(),configStorage);
    paused = false;
    isResuming(true);
  }

  public ImportJob getImportJob() {
    return importJob;
  }

  public UUID getImportConfigId() {
    return importConfigId;
  }

  public void halt(String errorMessage) {
    paused = true;
    reporting.log(errorMessage);
    importJob.logHalted(SettableClock.getLocalDateTime(), reporting.getRecordsProcessed(), configStorage);
    reporting.reportFileStats();
    reporting.reportFileQueueStats(true);
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

  public abstract String getStats ();

}
