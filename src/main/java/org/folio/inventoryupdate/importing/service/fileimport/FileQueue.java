package org.folio.inventoryupdate.importing.service.fileimport;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.FileSystem;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

import java.io.File;
import java.util.*;

public class FileQueue {

    public static final String SOURCE_FILES_ROOT_DIR = "MIU_QUEUE";
    public static final String TENANT_DIR_PREFIX = "TENANT_";
    public static final String CONFIG_DIR_PREFIX = "IMPORT_";
    public static final String DIRECTORY_OF_CURRENTLY_PROCESSING_FILE = ".processing";
    public static final String TMP_DIR = ".tmp";
    private final String jobPath;
    private final String pathToProcessingSlot;
    private final FileSystem fs;

    public static void clearTenantQueues(Vertx vertx, String tenant) {
      if (vertx.fileSystem().existsBlocking(SOURCE_FILES_ROOT_DIR + "/" + TENANT_DIR_PREFIX + tenant)) {
        vertx.fileSystem().deleteRecursiveBlocking(SOURCE_FILES_ROOT_DIR + "/" + TENANT_DIR_PREFIX + tenant);
      }
    }

    public FileQueue(ServiceRequest request, String configId) {
        this.fs = request.vertx().fileSystem();
        String tenantRootDir = SOURCE_FILES_ROOT_DIR + "/" + TENANT_DIR_PREFIX + request.tenant();
        if (!fs.existsBlocking(SOURCE_FILES_ROOT_DIR)) {
            fs.mkdirBlocking(SOURCE_FILES_ROOT_DIR);
        }
        if (!fs.existsBlocking(tenantRootDir)) {
            fs.mkdirBlocking(tenantRootDir);
        }
        jobPath = new File(tenantRootDir, CONFIG_DIR_PREFIX+configId).getPath();
        pathToProcessingSlot = new File(jobPath, DIRECTORY_OF_CURRENTLY_PROCESSING_FILE).getPath();
        createImportConfigDirectories();
    }

    private void createImportConfigDirectories () {
      if (! fs.existsBlocking(jobPath)) {
        fs.mkdirsBlocking(pathToProcessingSlot).mkdirBlocking(jobPath + "/" + TMP_DIR);
      }
    }

  /**
   * Create system directories for a source file queue for this import configuration. If the directories
   * already exist with source files in them, initializing the queue will remove all the source files, thereby
   * resetting the queue to empty.
   * @return Message describing the action taken.
   */
  public String initializeQueue() {
      int filesInQueueBefore = fs.readDirBlocking(jobPath).size();
      fs.deleteRecursiveBlocking(jobPath);
      createImportConfigDirectories();
      int filesInQueueAfter = fs.readDirBlocking(jobPath).size();
      if (filesInQueueBefore>filesInQueueAfter) {
        return "Deleted " + (filesInQueueBefore-filesInQueueAfter) + " source files from the queue at " + jobPath;
      } else {
        return "Initialized file system queue at " + jobPath;
      }
    }

    /**
     * Creates a new file in the staging directory for the given job configuration. If a file with the same name
     * already exists in staging, the existing file is replaced with the new one.
     * @param fileName The name of the file to stage.
     * @param file The file contents.
     */
    public void addNewFile(String fileName, Buffer file) {
        fs.writeFileBlocking(jobPath + "/"+ TMP_DIR + "/" + fileName, file)
            .move(jobPath+"/"+ TMP_DIR + "/"+fileName, jobPath+"/"+fileName,
                new CopyOptions().setReplaceExisting(true));
    }

    /**
     * Checks if there is a file in the processing directory for the
     * given job ID (or if it's empty and thus available for the next file to import).
     * @return true if the processing directory is occupied, false if it's ready for next file.
     */
    public boolean processingSlotTaken() {
        return fs.readDirBlocking(pathToProcessingSlot).stream().map(File::new).anyMatch(File::isFile);
    }

    public boolean hasNextFile() {
        return fs.readDirBlocking(jobPath).stream().map(File::new).anyMatch(File::isFile);
    }

    /**
     * Promotes the next file in the staging directory to the processing directory
     * and returns true if a staged file was found (and the processing directory was free), otherwise returns false.
     * @return true if another file was found for processing, otherwise false.
     */
    public boolean promoteNextFileIfPossible() {
        if (!processingSlotTaken()) {
            return fs.readDirBlocking(jobPath).stream().map(File::new).filter(File::isFile).min(Comparator.comparing(File::lastModified))
                    .map(file -> {
                        if (!processingSlotTaken()) {
                            fs.moveBlocking(file.getPath(), pathToProcessingSlot + "/" + file.getName());
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
     * @return The name of file being processed, "none" if there is none.
     */
    public File currentlyPromotedFile() {
        return fs.readDirBlocking(pathToProcessingSlot).stream().map(File::new).filter(File::isFile).findFirst().orElse(null);
    }

    public void deleteFile(File file) {
        fs.deleteBlocking(file.getPath());
    }

  /**
   * Used for waiting for the current file to process before getting the next file (if any).
   * @return null if there is already a file from the queue processing or if there are no more files in queue,
   * otherwise returns the next file for processing.
   */
  public File nextFileIfPossible() {
         if (promoteNextFileIfPossible()) {
            return currentlyPromotedFile();
        }
        return null;
    }

}
