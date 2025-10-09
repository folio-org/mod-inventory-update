package org.folio.inventoryimport.service.fileimport;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import org.folio.inventoryimport.service.ServiceRequest;

import java.io.File;
import java.util.*;

public class FileQueue {

    public static final String SOURCE_FILES_ROOT_DIR = "source-files";
    public static final String HARVEST_JOB_FILE_PROCESSING_DIR = "processing";
    private final String jobPath;
    private final String pathToProcessingSlot;
    private final FileSystem fs;

    public FileQueue(ServiceRequest request, String jobConfigId) {
        this.fs = request.vertx().fileSystem();
        String sourceFilesRootDir = SOURCE_FILES_ROOT_DIR;
        String tenantRootDir = sourceFilesRootDir + "/" + request.tenant();
        if (!fs.existsBlocking(sourceFilesRootDir)) {
            fs.mkdirBlocking(sourceFilesRootDir);
        }
        if (!fs.existsBlocking(tenantRootDir)) {
            fs.mkdirBlocking(tenantRootDir);
        }
        jobPath = tenantRootDir + "/" + jobConfigId;
        pathToProcessingSlot = jobPath + "/" + HARVEST_JOB_FILE_PROCESSING_DIR;
        if (! fs.existsBlocking(jobPath)) {
            fs.mkdirsBlocking(pathToProcessingSlot).mkdirBlocking(jobPath + "/tmp");
        }
    }

    /**
     * Creates a new file in the staging directory for the given job configuration.
     * @param fileName The name of the file to stage.
     * @param file The file contents.
     */
    public void addNewFile(String fileName, Buffer file) {
        fs.writeFileBlocking(jobPath + "/tmp/" + fileName, file)
                .moveBlocking(jobPath+"/tmp/"+fileName, jobPath+"/"+fileName);
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

    public File nextFileIfPossible() {
         if (promoteNextFileIfPossible()) {
            return currentlyPromotedFile();
        }
        return null;
    }

}
