package org.folio.inventoryupdate.importing.service.provisioning.fileimport;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

public final class FileListeners {

  public static final Logger logger = LogManager.getLogger("file-listeners");

  private static final ConcurrentMap<String, ConcurrentMap<String, FileListener>> FILE_LISTENERS
      = new ConcurrentHashMap<>();

  private FileListeners() {
    throw new IllegalStateException("Utility class");
  }

  public static FileListener getFileListener(String tenant, String channelId) {
    FILE_LISTENERS.putIfAbsent(tenant, new ConcurrentHashMap<>());
    return FILE_LISTENERS.get(tenant).get(channelId);
  }

  public static FileListener addFileListener(String tenant, String channelId, FileListener fileListener) {
    FILE_LISTENERS.putIfAbsent(tenant, new ConcurrentHashMap<>());
    FILE_LISTENERS.get(tenant).put(channelId, fileListener);
    return fileListener;
  }

  public static boolean hasFileListener(String tenant, String channelId) {
    return getFileListener(tenant, channelId) != null;
  }

  public static Future<String> deployIfNotDeployed(ServiceRequest request, Channel channel) {
    Promise<String> promise = Promise.promise();
    String cfgId = channel.getRecord().id().toString();
    FileListener fileListener = FileListeners.getFileListener(request.tenant(), cfgId);
    if (fileListener == null) {
      FileListener listenerVerticle = addFileListener(request.tenant(), cfgId, new XmlFileListener(request, channel));
      return new ImportJob().changeRunningToInterruptedByChannelId(request.moduleStorageAccess(), cfgId)
          .compose(jobsInterrupted -> {
            String jobsMarkedInterrupted = jobsInterrupted > 0
                ? jobsInterrupted + " previous job was marked 'RUNNING', now marked 'INTERRUPTED'. " : "";
            return listenerVerticle.deploy().map(resp -> jobsMarkedInterrupted + resp);
          });
    } else {
      promise.complete("File listener already commissioned for channel [" + channel.getRecord().name() + "].");
    }
    return promise.future();
  }

  public static Future<String> undeployIfDeployed(ServiceRequest request, Channel channel) {
    String cfgId = channel.getRecord().id().toString();
    FileListener fileListener = FileListeners.getFileListener(request.tenant(), cfgId);
    if (fileListener != null) {
      return fileListener.undeploy()
          .map(na -> FILE_LISTENERS.get(request.tenant())
              .remove(cfgId)).map("Decommissioned channel " + channel.getRecord().name());
    } else {
      return Future.succeededFuture(
          "Did not find channel [" + channel.getRecord().name() + "] in list of commissioned channels.");
    }
  }
}
