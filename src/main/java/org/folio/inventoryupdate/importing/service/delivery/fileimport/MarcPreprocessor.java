package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.buffer.Buffer;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.converter.impl.AnselToUnicode;

public class MarcPreprocessor {
  AtomicBoolean processingInProgress = new AtomicBoolean(false);
  FileQueue queue;

  public MarcPreprocessor(FileQueue queue) {
    this.queue = queue;
  }

  public void addFile(String fileName, Buffer file) {
    queue.addFileToPreprocessing(fileName, file);
  }

  public void preprocess() {
    if (!processingInProgress.getAndSet(true)) {
      if (queue.hasFilesForPreprocessing()) {
        File next = queue.nextFileForPreprocessing();
        if (next != null) {
          fromMarcToXmlQueue(queue.readFile(next), next.getName());
          queue.deleteFile(next);
        }
        processingInProgress.set(false);
      } else {
        // wasn't in progress before and no new files so still not in progress
        processingInProgress.set(false);
      }
    }
  }

  /**
   * Converts binary MARC file from the preprocessing queue to a MARC XML file written to the channel's XML queue.
   *   Then deletes the MARC file from the preprocessing queue.
   *
   * @param marcFile binary MARC from upload request
   *
   * @param fileName name of the MARC file if provided at upload otherwise a UUID
   */
  private void fromMarcToXmlQueue(Buffer marcFile, String fileName) {
    MarcReader marcReader = new MarcPermissiveStreamReader(
        new ByteArrayInputStream(marcFile.getBytes()), true, true);
    if (marcReader.hasNext()) {
      try {
        OutputStream out = new FileOutputStream(queue.getJobTmpDir() + "/" + fileName);
        MarcXmlWriter writer = new MarcXmlWriter(out, true);
        AnselToUnicode converter = new AnselToUnicode();
        writer.setConverter(converter);
        while (marcReader.hasNext()) {
          org.marc4j.marc.Record record = marcReader.next();
          writer.write(record);
        }
        writer.close();
        queue.fromTmpToQueue(fileName);
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }
}
