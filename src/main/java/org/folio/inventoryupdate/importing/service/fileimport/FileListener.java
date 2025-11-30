package org.folio.inventoryupdate.importing.service.fileimport;

import io.vertx.core.Future;
import io.vertx.core.VerticleBase;
import io.vertx.ext.web.RoutingContext;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class FileListener extends VerticleBase {

  protected String tenant;
  protected UUID importConfigurationId;
  protected RoutingContext routingContext;

  protected FileProcessor fileProcessor;
  protected FileQueue fileQueue;

  // For demarcating jobs by start/end
  protected AtomicBoolean fileQueuePassive = new AtomicBoolean(true);


  public FileProcessor getImportJob() {
    return fileProcessor;
  }

  public void markFileQueuePassive() {
    fileQueuePassive.set(true);
  }

  public boolean fileQueueIsPassive() {
    return fileQueuePassive.get();
  }

  public boolean fileQueueIsEmpty() {
    return !fileQueue.hasNextFile();
  }

  protected abstract void listen();

  /**
   * Gets existing file processor or instantiates a new one
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
   * <li>except, if there is already a file currently processing: returns null</li>
   * <li>except, if the process is being newly activated or resumed, then returns the currently promoted file after all, to
   * restart processing with that</li>
   * <li>except, if there is no promoted file and no files in queue: returns null.</li>
   */
  public File getNextFileIfPossible(boolean fileQueuePassive, boolean processorResuming) {
    if (fileQueue.processingSlotTaken() && (fileQueuePassive || processorResuming)) {
      return fileQueue.currentlyPromotedFile();
    } else {
      return fileQueue.nextFileIfPossible();
    }
  }


  public boolean importJobPaused() {
    return fileProcessor != null && fileProcessor.paused();
  }


}
