package org.folio.inventoryupdate.importing.service.delivery.fileimport;

public class HarvestResult {
  int discoveredFiles;
  int queuedFiles;
  int skippedOldFiles;
  int skippedFilesWithoutTimestamp;
  String lastHarvested;
  String error;

  public HarvestResult(int discoveredFiles, int queuedFiles, int skippedOldFiles,
                       int skippedFilesWithoutTimestamp) {
    this.discoveredFiles = discoveredFiles;
    this.queuedFiles = queuedFiles;
    this.skippedOldFiles = skippedOldFiles;
    this.skippedFilesWithoutTimestamp = skippedFilesWithoutTimestamp;
  }

  public HarvestResult(String error) {
    this.error = error;
  }

  public HarvestResult withLastHarvested(String lastHarvested) {
    this.lastHarvested = lastHarvested;
    return this;
  }

  public int queuedFiles() {
    return queuedFiles;
  }

  public int skippedOldFiles() {
    return skippedOldFiles;
  }

  public String error() {
    return error;
  }

  public boolean isInError() {
    return error != null;
  }

  public String report() {
    if (isInError()) {
      return error();
    } else {
      return String.format("Queued %s file%s (skipped %s old file%s).",
          queuedFiles(), pluralS(queuedFiles()), skippedOldFiles(), pluralS(skippedOldFiles()));
    }
  }

  private String pluralS(int count) {
    return count == 1 ? "" : "s";
  }
}
