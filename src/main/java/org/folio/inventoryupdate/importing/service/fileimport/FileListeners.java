package org.folio.inventoryupdate.importing.service.fileimport;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.ThreadingModel;
import io.vertx.core.VerticleBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class FileListeners {
  private static final ConcurrentMap<String, ConcurrentMap<String, FileListener>> FILE_LISTENERS = new ConcurrentHashMap<>();

  public static final Logger logger = LogManager.getLogger("file-listeners");

  private FileListeners() {
    throw new IllegalStateException("Utility class");
  }

  public static FileListener getFileListener(String tenant, String channelId) {
    FILE_LISTENERS.putIfAbsent(tenant, new ConcurrentHashMap<>());
    return FILE_LISTENERS.get(tenant).get(channelId);
  }

  public static VerticleBase addFileListener(String tenant, String channelId, FileListener fileListener) {
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
      VerticleBase verticle = FileListeners.addFileListener(request.tenant(), cfgId, new XmlFileListener(request, channel));
      request.vertx().deployVerticle(verticle,
          new DeploymentOptions()
              .setWorkerPoolSize(4)
              .setInstances(1)
              .setMaxWorkerExecuteTime(10)
              .setThreadingModel(ThreadingModel.WORKER)
              .setMaxWorkerExecuteTimeUnit(TimeUnit.MINUTES)).onComplete(
          started -> {
            if (started.succeeded()) {
              logger.info("Started verticle [{}] for [{}] and configuration ID [{}].", started.result(), request.tenant(), channel.getRecord().name());
              promise.complete("Started verticle [" + started.result() + "] for configuration ID [" + channel.getRecord().name() + "].");
            } else {
              logger.error("Couldn't start file processor verticle for tenant [{}] and import configuration ID [{}].", request.tenant(), channel.getRecord().name());
              promise.fail("Couldn't start file processor verticle for import configuration ID [" + channel.getRecord().name() + "].");
            }
          });
    } else {
      promise.complete("File listener already created for import configuration ID [" + channel.getRecord().name() + "].");
    }
    return promise.future();
  }

}
