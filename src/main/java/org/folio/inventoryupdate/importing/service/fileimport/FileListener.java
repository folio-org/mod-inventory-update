package org.folio.inventoryupdate.importing.service.fileimport;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class FileListener extends AbstractVerticle {
    protected FileProcessor fileProcessor;
    protected FileQueue fileQueue;

    // For demarcating jobs by start/end
    protected AtomicBoolean fileQueuePassive = new AtomicBoolean(true);


    public FileProcessor getImportJob() {
        return fileProcessor;
    }

    public abstract Future<FileProcessor> getFileProcessor(boolean activating);

    public void markFileQueuePassive() {
        fileQueuePassive.set(true);
    }

    public boolean fileQueueIsPassive() {
        return fileQueuePassive.get();
    }

    public boolean fileQueueIsEmpty() {
        return !fileQueue.hasNextFile();
    }

  /**
   * If there is a file in the processing slot, then it would normally be a currently processing file that should be
   * waited for to finish.
   * However, if the process is newly activated or is a resumption of a paused job, then it is assumed that this file
   * is from a past, interrupted run and that it should be re-processed.
   * @return true if there is file from a past run that was promoted for processing but didn't finish, false if the file is
   * already being processed by the currently running job and should be waited for, as is normally the case.
   */
  public boolean resumePromotedFile() {
      return (fileQueue.processingSlotTaken() &&
        fileQueueIsPassive() || (fileProcessor != null && fileProcessor.isResuming(false)));
    }

}
