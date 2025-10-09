package org.folio.inventoryimport.service.fileimport;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class FileListener extends AbstractVerticle {
    protected FileProcessor fileProcessor;
    protected FileQueue fileQueue;

    // For demarcating jobs by start/end
    protected AtomicBoolean fileQueuePassive = new AtomicBoolean(true);


    public FileProcessor getImportJob() {
        return fileProcessor;
    }

    public abstract Future<FileProcessor> getFileProcessor(boolean activating);

    public void markFileQueuePassive() {
        fileQueuePassive.set(true);
    }

    public boolean fileQueueIsPassive() {
        return fileQueuePassive.get();
    }

    public boolean fileQueueIsEmpty() {
        return !fileQueue.hasNextFile();
    }

    public boolean processingSlotIsOccupied() {
        return fileQueue.processingSlotTaken();
    }

}
