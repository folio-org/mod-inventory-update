package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.FileSystemException;
import java.io.File;
import java.util.Comparator;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

public class FileQueue {

  public static final String SOURCE_FILES_ROOT_DIR = "MIU_QUEUE";
  public static final String TENANT_DIR_PREFIX = "TENANT_";
  public static final String CHANNEL_PREFIX = "CHANNEL_";
  public static final String DIRECTORY_OF_CURRENTLY_PROCESSING_FILE = ".processing";
  public static final String TMP_DIR = ".tmp";
  private final String jobPath;
  private final String jobTmpDir;
  private final String jobProcessingSlot;
  private final FileSystem fs;

  public FileQueue(ServiceRequest request, String configId) {
    fs = request.vertx().fileSystem();
    String tenantRootDir = new File(SOURCE_FILES_ROOT_DIR, TENANT_DIR_PREFIX + request.tenant()).getPath();
    jobPath = new File(tenantRootDir, CHANNEL_PREFIX + configId).getPath();
    jobProcessingSlot = new File(jobPath, DIRECTORY_OF_CURRENTLY_PROCESSING_FILE).getPath();
    jobTmpDir = new File(jobPath, TMP_DIR).getPath();
  }

  public static void clearTenantQueues(Vertx vertx, String tenant) {
    if (vertx.fileSystem().existsBlocking(SOURCE_FILES_ROOT_DIR + "/" + TENANT_DIR_PREFIX + tenant)) {
      vertx.fileSystem().deleteRecursiveBlocking(SOURCE_FILES_ROOT_DIR + "/" + TENANT_DIR_PREFIX + tenant);
    }
  }

  public void createDirectoriesIfNotExist() {
    if (!fs.existsBlocking(jobProcessingSlot)) {
      fs.mkdirsBlocking(jobProcessingSlot);
    }
    if (!fs.existsBlocking(jobTmpDir)) {
      fs.mkdirsBlocking(jobTmpDir);
    }
  }

  public void deleteDirectoriesIfExist() {
    if (fs.existsBlocking(jobPath)) {
      fs.deleteRecursiveBlocking(jobPath);
    }
  }

  /**
   * Create system directories for a source file queue for the current channel. If the directories
   * already exist with source files in them, initializing the queue will remove all the source files, thereby
   * resetting the queue to empty.
   *
   * @return Message describing the action taken.
   */
  public String initializeQueue() {
    int filesInQueueBefore = 0;
    if (fs.existsBlocking(jobPath)) {
      filesInQueueBefore = fs.readDirBlocking(jobPath).size() - 2;
      if (filesInQueueBefore > 0 || processingSlotTaken()) {
        deleteDirectoriesIfExist();
      }
    }
    createDirectoriesIfNotExist();
    if (filesInQueueBefore > 0) {
      return "Deleted " + filesInQueueBefore + " source files from the queue at " + jobPath;
    } else {
      return "Initialized file system queue at " + jobPath;
    }
  }

  /**
   * Creates a new file in the staging directory for the given job configuration. If a file with the same name
   * already exists in staging, the existing file is replaced with the new one.
   *
   * @param fileName The name of the file to stage.
   * @param file     The file contents.
   */
  public void addNewFile(String fileName, Buffer file) {
    fs.writeFileBlocking(jobPath + "/" + TMP_DIR + "/" + fileName, file)
        .move(jobPath + "/" + TMP_DIR + "/" + fileName, jobPath + "/" + fileName,
            new CopyOptions().setReplaceExisting(true));
  }

  /**
   * Checks if there is a file in the processing directory for the
   * given job ID (or if it's empty and thus available for the next file to import).
   *
   * @return true if the processing directory is occupied, false if it's ready for next file.
   */
  public boolean processingSlotTaken() {
    try {
      return fs.readDirBlocking(jobProcessingSlot).stream().map(File::new).anyMatch(File::isFile);
    } catch (FileSystemException fse) {
      if (fse.getMessage().contains("Does not exist")) {
        createDirectoriesIfNotExist();
        return false;
      } else {
        throw fse;
      }
    }
  }

  public boolean hasNextFile() {
    return fs.readDirBlocking(jobPath).stream().map(File::new).anyMatch(File::isFile);
  }

  public int size() {
    if (fs.existsBlocking(jobPath)) {
      return fs.readDirBlocking(jobPath).stream().map(File::new).filter(File::isFile).toList().size();
    } else {
      return -1;
    }
  }

  public String fileInProcess() {
    if (fs.existsBlocking(jobProcessingSlot)) {
      return fs.readDirBlocking(jobProcessingSlot).stream().map(File::new)
          .filter(File::isFile).findFirst().map(File::getName).orElse("no file in process");
    } else {
      return "no file in process";
    }
  }

  /**
   * Promotes the next file in the staging directory to the processing directory
   * and returns true if a staged file was found (and the processing directory was free), otherwise returns false.
   *
   * @return true if another file was found for processing, otherwise false.
   */
  public boolean promoteNextFileIfPossible() {
    if (!processingSlotTaken()) {
      return fs.readDirBlocking(jobPath).stream().map(File::new).filter(File::isFile)
          .min(Comparator.comparing(File::lastModified))
          .map(file -> {
            if (!processingSlotTaken()) {
              fs.moveBlocking(file.getPath(), jobProcessingSlot + "/" + file.getName());
              return true;
            } else {
              return false;
            }
          }).orElse(false);
    }
    return false;
  }

  /**
   * Gets the name of the file currently processing under the given job configuration.
   *
   * @return The name of file being processed, "none" if there is none.
   */
  public File currentlyPromotedFile() {
    return fs.readDirBlocking(jobProcessingSlot).stream().map(File::new).filter(File::isFile).findFirst().orElse(null);
  }

  public void deleteFile(File file) {
    fs.deleteBlocking(file.getPath());
  }

  /**
   * Used for waiting for the current file to process before getting the next file (if any).
   *
   * @return null if there is already a file from the queue processing or if there are no more files in queue,
   *   otherwise returns the next file for processing.
   */
  public File nextFileIfPossible() {
    if (promoteNextFileIfPossible()) {
      return currentlyPromotedFile();
    }
    return null;
  }
}
