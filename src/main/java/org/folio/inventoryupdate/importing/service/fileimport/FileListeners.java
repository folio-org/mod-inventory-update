package org.folio.inventoryupdate.importing.service.fileimport;

import io.vertx.core.Verticle;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class FileListeners {
    private static final ConcurrentMap<String, ConcurrentMap<String, FileListener>> FILE_LISTENERS = new ConcurrentHashMap<>();

    private FileListeners () {
      throw new IllegalStateException("Utility class");
    }

    public static FileListener getFileListener(String tenant, String importConfigurationId) {
        FILE_LISTENERS.putIfAbsent(tenant, new ConcurrentHashMap<>());
        return FILE_LISTENERS.get(tenant).get(importConfigurationId);
    }

    public static Verticle addFileListener(String tenant, String importConfigurationId, FileListener fileListener) {
        FILE_LISTENERS.putIfAbsent(tenant, new ConcurrentHashMap<>());
        FILE_LISTENERS.get(tenant).put(importConfigurationId,fileListener);
        return fileListener;
    }

    public static boolean hasFileListener(String tenant, String importConfigurationId) {
        return getFileListener(tenant, importConfigurationId) != null;
    }
}
