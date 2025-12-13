package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.ArrayList;
import java.util.List;
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
    boolean retainQueueIfAny = "true".equalsIgnoreCase(request.requestParam("retainQueue"));
    // Request parameter can override what is set on the channel record
    boolean listening = request.requestParam("listening") == null
        ? channel.isListeningIfEnabled()
        : !"false".equalsIgnoreCase(request.requestParam("listening"));
    String cfgId = channel.getRecord().id().toString();
    FileListener fileListener = FileListeners.getFileListener(request.tenant(), cfgId);
    if (fileListener == null) {
      if (retainQueueIfAny) {
        new FileQueue(request, cfgId).createDirectoriesIfNotExist();
      } else {
        FileQueue queue = new FileQueue(request, cfgId);
        queue.deleteDirectoriesIfExist();
        queue.createDirectoriesIfNotExist();
      }
      FileListener listenerVerticle = addFileListener(request.tenant(), cfgId, new XmlFileListener(request, channel));
      channel.setEnabledListening(true, listening, request.entityStorage())
          .compose(na -> new ImportJob().changeRunningToInterruptedByChannelId(request.entityStorage(), cfgId))
          .compose(jobsInterrupted -> {
            String jobsMarkedInterrupted = jobsInterrupted > 0
                ? jobsInterrupted + " previous job was marked 'RUNNING', now marked 'INTERRUPTED'. " : "";
            return listenerVerticle.deploy().map(resp -> jobsMarkedInterrupted + resp);
          })
          .onSuccess(promise::complete)
          .onFailure(f -> promise.fail(f.getMessage()));
    } else {
      promise.complete("File listener already commissioned for channel [" + channel.getRecord().name() + "].");
    }
    return promise.future();
  }

  /**
   * If a verticle is deployed for the channel, un-deploys the verticle, deletes the file queue,
   * and de-registers the channel from static list of deployed verticles.
   *
   * @return statement about the outcome of the operation
   */
  public static Future<String> undeployIfDeployed(ServiceRequest request, Channel channel) {
    String cfgId = channel.getRecord().id().toString();
    boolean retainQueue = "true".equalsIgnoreCase(request.requestParam("retainQueue"));
    FileListener fileListener = FileListeners.getFileListener(request.tenant(), cfgId);
    if (fileListener != null) {
      return channel.setEnabledListening(false, channel.isListeningIfEnabled(), request.entityStorage())
          .compose(na -> fileListener.undeploy())
          .map(na -> {
            if (!retainQueue) {
              new FileQueue(request, cfgId).deleteDirectoriesIfExist();
            }
            return FILE_LISTENERS.get(request.tenant()).remove(cfgId);
          }).map("Decommissioned channel " + channel.getRecord().name());
    } else {
      return Future.succeededFuture(
          "Did not find channel [" + channel.getRecord().name() + "] in list of commissioned channels.");
    }
  }

  /**
   * In support of unit testing.
   * Un-deploys and de-registers listener verticles that otherwise would
   * survive across test method invocations.
   */
  public static Future<Void> clearRegistry() {
    List<Future<Void>> undeployFutures = new ArrayList<>();
    for (String tenant : FILE_LISTENERS.keySet()) {
      for (String listenerId : FILE_LISTENERS.get(tenant).keySet()) {
        FileListener listener = FILE_LISTENERS.get(tenant).get(listenerId);
        undeployFutures.add(listener.deploymentVertx.undeploy(listener.deploymentId));
      }
    }
    return Future.all(undeployFutures).onComplete(na -> FILE_LISTENERS.clear()).mapEmpty();
  }
}
