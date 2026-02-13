package org.folio.inventoryupdate.importing.service.delivery.fileimport.reporting;

import java.util.concurrent.atomic.AtomicInteger;

public class FileStats {
  private final String fileName;
  private final long startTimeNanos;
  private final InventoryMetrics metrics;
  private final AtomicInteger recordsProcessed = new AtomicInteger(0);

  public FileStats(String fileName) {
    this.fileName = fileName;
    startTimeNanos = System.nanoTime();
    metrics = new InventoryMetrics();
  }

  public void incrementRecordsProcessed(int delta) {
    recordsProcessed.addAndGet(delta);
  }

  public void addInventoryMetrics(InventoryMetrics metrics) {
    this.metrics.add(metrics);
  }

  public InventoryMetrics getInventoryMetrics() {
    return metrics;
  }

  public long processingTimeNanos() {
    return System.nanoTime() - startTimeNanos;
  }

  public String getFileName() {
    return fileName;
  }

  public int getRecordsProcessed() {
    return recordsProcessed.get();
  }
}
