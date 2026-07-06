package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import io.vertx.core.file.FileSystem;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class SourceFileFs implements SourceFile {

  final File file;
  final FileSystem fs;

  public SourceFileFs(File file, FileSystem fs) {
    this.file = file;
    this.fs = fs;
  }

  @Override
  public String getName() {
    return file.getName();
  }

  @Override
  public String getPayload() throws Exception {
    try {
      return Files.readString(file.toPath(), StandardCharsets.UTF_8);
    } catch (IOException ioe) {
      throw new Exception(ioe.getMessage());
    }
  }

  @Override
  public Future<Void> discard() {
    return fs.delete(file.getPath());
  }
}
