package org.folio.inventoryimport.service.fileimport;

import io.vertx.core.Verticle;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class FileListeners {
    private final static ConcurrentMap<String, ConcurrentMap<String, FileListener>> fileListeners = new ConcurrentHashMap<>();

    public static FileListener getFileListener(String tenant, String importConfigurationId) {
        fileListeners.putIfAbsent(tenant, new ConcurrentHashMap<>());
        return fileListeners.get(tenant).get(importConfigurationId);
    }

    public static Verticle addFileListener(String tenant, String importConfigurationId, FileListener fileListener) {
        fileListeners.putIfAbsent(tenant, new ConcurrentHashMap<>());
        fileListeners.get(tenant).put(importConfigurationId,fileListener);
        return fileListener;
    }

    public static boolean hasFileListener(String tenant, String importConfigurationId) {
        return getFileListener(tenant, importConfigurationId) != null;
    }
}
