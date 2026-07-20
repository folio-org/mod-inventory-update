package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.service.ImportService;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

public final class FileListeners {

  public static final Logger logger = LogManager.getLogger("file-listeners");

  private static final ConcurrentMap<String, ConcurrentMap<UUID, FileListener>> FILE_LISTENERS
      = new ConcurrentHashMap<>();

  private FileListeners() {
    throw new UnsupportedOperationException("Utility class");
  }

  public static FileListener getFileListener(String tenant, UUID channelId) {
    FILE_LISTENERS.putIfAbsent(tenant, new ConcurrentHashMap<>());
    return FILE_LISTENERS.get(tenant).get(channelId);
  }

  public static FileListener addFileListener(String tenant, UUID channelId, FileListener fileListener) {
    FILE_LISTENERS.putIfAbsent(tenant, new ConcurrentHashMap<>());
    FILE_LISTENERS.get(tenant).put(channelId, fileListener);
    return fileListener;
  }

  public static boolean hasFileListener(String tenant, UUID channelId) {
    return getFileListener(tenant, channelId) != null;
  }

  public static Future<String> deployIfNotDeployed(ServiceRequest request, Channel channel) {
    if (channel == null || channel.getId() == null) {
      return Future.succeededFuture("No channel provided to deploy.");
    } else {
      boolean retainQueueIfAny = "true".equalsIgnoreCase(request.requestParam("retainQueue"));
      FileQueue fq = ImportService.getFileQueue(request, channel.getId());
      // Request parameter can override what is set on the channel record
      boolean listening = request.requestParam("listening") == null
          ? channel.isListeningIfEnabled()
          : !"false".equalsIgnoreCase(request.requestParam("listening"));
      FileListener fileListener = FileListeners.getFileListener(request.tenant(), channel.getId());
      if (fileListener == null) {
        FileListener listenerVerticle = addFileListener(request.tenant(), channel.getId(),
            new XmlFileListener(request, channel));
        return channel.setEnabledListening(true, listening, request.entityStorage())
            .compose(na -> fq.initialize(retainQueueIfAny).mapEmpty())
            .compose(na -> new ImportJob().changeRunningToInterruptedByChannelId(request.entityStorage(),
                channel.getId()))
            .compose(jobsInterrupted -> {
              String jobsMarkedInterrupted = jobsInterrupted > 0
                  ? jobsInterrupted + " previous job was marked 'RUNNING', now marked 'INTERRUPTED'. " : "";
              return listenerVerticle.deploy().map(resp -> jobsMarkedInterrupted + resp);
            });
      } else {
        return Future.succeededFuture(
            "File listener already commissioned for channel [" + channel.getName() + "].");
      }
    }
  }

  /**
   * If a verticle is deployed for the channel, un-deploys the verticle, deletes the file queue,
   * and de-registers the channel from static list of deployed verticles.
   *
   * @return statement about the outcome of the operation
   */
  public static Future<String> undeployIfDeployed(ServiceRequest request, Channel channel) {
    boolean retainQueue = "true".equalsIgnoreCase(request.requestParam("retainQueue"));
    FileListener fileListener = FileListeners.getFileListener(request.tenant(), channel.getId());
    if (fileListener != null) {
      return channel.setEnabledListening(false, channel.isListeningIfEnabled(), request.entityStorage())
          .compose(na -> fileListener.undeploy())
          .map(na -> {
            ImportService.getFileQueue(request, channel.getId()).initialize(retainQueue);
            return FILE_LISTENERS.get(request.tenant()).remove(channel.getId());
          }).map("Channel decommissioned." + channel.getRecord().name());
    } else {
      return Future.succeededFuture(
          "Did not find channel [" + channel.getName() + "] in list of commissioned channels.");
    }
  }

  /**
   * In support of unit testing.
   * Un-deploys and de-registers listener verticles that otherwise would
   * survive across test method invocations.
   */
  public static Future<Void> clearRegistry(String tenant) {
    List<Future<Void>> undeployFutures = new ArrayList<>();
    if (FILE_LISTENERS.get(tenant) != null) {
      for (Map.Entry<UUID, FileListener> listener : FILE_LISTENERS.get(tenant).entrySet()) {
        FileListener fileListener = FILE_LISTENERS.get(tenant).get(listener.getKey());
        undeployFutures.add(fileListener.deploymentVertx.undeploy(fileListener.deploymentId));
      }
    }
    return Future.all(undeployFutures).onComplete(na -> FILE_LISTENERS.clear()).mapEmpty();
  }
}
