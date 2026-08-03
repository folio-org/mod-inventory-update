package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;

public interface FileQueue {

  Future<String> initialize(boolean retainFilesIfAny);

  Future<Void> push(String fileName, String timeStamp, String payload);

  Future<Boolean> hasFileInProcess();

  Future<Boolean> isEmpty();

  Future<Integer> size();

  Future<String> nameOfFileInProcess();

  /**
   * Promotes the next file in the queue for processing if possible, and returns the newly promoted file.
   *
   * @return null if there is already a file from the queue processing or if there are no more files in queue,
   *   otherwise returns the next file promoted for processing.
   */
  Future<SourceFile> promoteAndGetNextFileIfPossible();

  /**
   * Gets the File that is currently promoted for processing, if any.
   *
   * @return The File being processed, null if there is none.
   */
  Future<SourceFile> currentlyPromotedFile();
}
