package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.FileSystem;
import java.io.File;
import java.util.Comparator;
import java.util.UUID;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

public final class FileQueueFs implements FileQueue {

  public static final String SOURCE_FILES_ROOT_DIR = "MIU_QUEUE";
  public static final String TENANT_DIR_PREFIX = "TENANT_";
  public static final String CHANNEL_PREFIX = "CHANNEL_";
  public static final String DIRECTORY_OF_CURRENTLY_PROCESSING_FILE = ".processing";
  public static final String TMP_DIR = ".tmp";
  private final String jobPath;
  private final String jobTmpDir;
  private final String jobProcessingSlot;
  private final FileSystem fs;

  private FileQueueFs(ServiceRequest request, UUID channelId) {
    fs = request.vertx().fileSystem();
    String tenantRootDir = new File(SOURCE_FILES_ROOT_DIR, TENANT_DIR_PREFIX + request.tenant()).getPath();
    jobPath = new File(tenantRootDir, CHANNEL_PREFIX + channelId).getPath();
    jobProcessingSlot = new File(jobPath, DIRECTORY_OF_CURRENTLY_PROCESSING_FILE).getPath();
    jobTmpDir = new File(jobPath, TMP_DIR).getPath();
  }

  public static FileQueueFs get(ServiceRequest request, UUID channelId) {
    return new FileQueueFs(request, channelId);
  }

  private Future<Void> createDirectoriesIfNotExist() {
    return fs.exists(jobProcessingSlot).compose(processingExists -> {
      if (!processingExists) {
        return fs.mkdirs(jobProcessingSlot);
      } else {
        return Future.succeededFuture();
      }
    }).compose(na -> fs.exists(jobTmpDir).compose(tmpExists -> {
      if (!tmpExists) {
        return fs.mkdirs(jobTmpDir);
      } else {
        return Future.succeededFuture();
      }
    }));
  }

  private Future<Void> deleteDirectoriesIfExist() {
    return fs.exists(jobPath).compose(exists -> {
      if (exists) {
        return fs.deleteRecursive(jobPath);
      } else {
        return Future.succeededFuture();
      }
    });
  }

  /**
   * Create system directories for a source file queue for the current channel. If the directories
   * already exist with source files in them, initializing the queue will remove all the source files, thereby
   * resetting the queue to empty.
   *
   * @return Message describing the action taken.
   */
  public Future<String> initialize(boolean retainFilesIfAny) {
    return fs.exists(jobPath)
        .compose(exists -> {
          if (exists) {
            if (!retainFilesIfAny) {
              return fs.readDir(jobPath)
                  .compose(list -> hasFileInProcess()
                      .compose(inProcess -> {
                        if (list.size() - 2 > 0 || inProcess) {
                          return deleteDirectoriesIfExist()
                              .map("Deleted " + (list.size() - 2) + " sources files from queue at " + jobPath);
                        } else {
                          return Future.succeededFuture("Initialized file system queue at " + jobPath);
                        }
                      }));
            } else {
              return Future.succeededFuture();
            }
          } else {
            return Future.succeededFuture();
          }
        })
        .compose(msg -> createDirectoriesIfNotExist().map(msg));
  }

  /**
   * Creates a new file in the staging directory for the given job configuration. If a file with the same name
   * already exists in staging, the existing file is replaced with the new one.
   *
   * @param fileName The name of the file to stage.
   * @param payload     The file contents.
   */
  public Future<Void> push(String fileName, String payload) {
    return createDirectoriesIfNotExist()
        .compose(na -> fs.writeFile(jobPath + "/" + TMP_DIR + "/" + fileName, Buffer.buffer(payload)))
        .compose(na -> fs.move(jobPath + "/" + TMP_DIR + "/" + fileName, jobPath + "/" + fileName,
          new CopyOptions().setReplaceExisting(true))).mapEmpty();
  }

  /**
   * Checks if there is a file in the processing directory for the
   * given job ID (or if it's empty and thus available for the next file in line).
   *
   * @return future true if the processing directory is occupied, false if it's ready for next file.
   */
  public Future<Boolean> hasFileInProcess() {
    return createDirectoriesIfNotExist().compose(na -> fs.readDir(jobProcessingSlot)
        .map(list -> list.stream().map(File::new).anyMatch(File::isFile)));
  }

  public Future<Boolean> isEmpty() {
    return fs.readDir(jobPath).compose(list -> Future.succeededFuture(list.stream().map(File::new)
        .noneMatch(File::isFile)));
  }

  public Future<Integer> size() {
    return fs.exists(jobPath).compose(exists -> {
      if (exists) {
        return fs.readDir(jobPath)
            .map(list -> list.stream().map(File::new).filter(File::isFile).toList().size());
      } else {
        return Future.succeededFuture(-1);
      }
    });
  }

  public Future<String> nameOfFileInProcess() {
    return fs.exists(jobProcessingSlot).compose(exists -> {
      if (exists) {
        return fs.readDir(jobProcessingSlot)
            .map(list -> list.stream().map(File::new)
                .filter(File::isFile).findFirst().map(File::getName).orElse("no file in process"));
      } else {
        return Future.succeededFuture("no file in process");
      }
    });
  }

  public Future<SourceFile> currentlyPromotedFile() {
    return fs.readDir(jobProcessingSlot)
        .map(list -> list.stream().map(File::new).filter(File::isFile).findFirst().map(file ->
            new SourceFileFs(file, fs)).orElse(null));
  }

  public Future<SourceFile> promoteAndGetNextFileIfPossible() {
    return hasFileInProcess()
        .compose(alreadyHasFileInProcess -> {
          if (alreadyHasFileInProcess) {
            return Future.succeededFuture(null);
          } else {
            return fs.readDir(jobPath)
                .compose(list -> list.stream().map(File::new).filter(File::isFile)
                    .min(Comparator.comparing(File::lastModified))
                    .map(f -> fs.move(f.getPath(), jobProcessingSlot + "/" + f.getName())
                        .compose(na -> currentlyPromotedFile()))
                    .orElse(Future.succeededFuture(null))); // Queue was empty.
          }
        });
  }
}
