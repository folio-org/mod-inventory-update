package org.folio.inventoryupdate.importing.service.provisioning;

import static org.folio.inventoryupdate.importing.service.provisioning.GenericRequests.deleteEntity;
import static org.folio.inventoryupdate.importing.service.provisioning.GenericRequests.getEntities;
import static org.folio.inventoryupdate.importing.service.provisioning.GenericRequests.getEntity;
import static org.folio.okapi.common.HttpResponse.responseJson;
import static org.folio.okapi.common.HttpResponse.responseText;

import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryupdate.importing.moduledata.database.SqlQuery;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.inventoryupdate.importing.service.provisioning.fileimport.FileListener;
import org.folio.inventoryupdate.importing.service.provisioning.fileimport.FileListeners;
import org.folio.inventoryupdate.importing.service.provisioning.fileimport.FileQueue;

public final class Channels {

  private static final AtomicBoolean channelsAlreadyBootstrapped = new AtomicBoolean(false);

  private Channels() {
    throw new IllegalStateException("Static storage utilities");
  }

  public static Future<Void> postChannel(ServiceRequest request) {
    Channel channel = new Channel().fromJson(request.bodyAsJson());
    ModuleStorageAccess db = request.moduleStorageAccess();
    return db.storeEntity(channel.withCreatingUser(request.currentUser())).compose(id -> {
      new FileQueue(request, id.toString()).createDirectoriesIfNotExist();
      return Future.succeededFuture(id);
    }).compose(id -> db.getEntity(id, channel)).compose(cfg -> {
      if (((Channel) cfg).isEnabled()) {
        return FileListeners.deployIfNotDeployed(request, (Channel) cfg).map(na -> cfg)
            .compose(na -> responseJson(request.routingContext(), 201).end(cfg.asJson().encodePrettily())).mapEmpty();
      } else {
        return responseJson(request.routingContext(), 201).end(cfg.asJson().encodePrettily()).mapEmpty();
      }
    });
  }

  public static Future<Void> getChannels(ServiceRequest request) {
    return getEntities(request, new Channel());
  }

  public static Future<Void> getChannelById(ServiceRequest request) {
    return getEntity(request, new Channel());
  }

  public static Future<Void> putChannel(ServiceRequest request) {
    Channel inputChannel = new Channel().fromJson(request.bodyAsJson());
    UUID id = UUID.fromString(request.requestParam("id"));
    return request.moduleStorageAccess().updateEntity(id, inputChannel.withUpdatingUser(request.currentUser()))
        .onSuccess(result -> {
          if (result.rowCount() == 1) {
            request.moduleStorageAccess().getEntity(id, new Channel()).map(entity -> (Channel) entity)
                .compose(channel -> {
                  if (channel.isEnabled() && channel.isCommissioned()) {
                    FileListener listener = FileListeners.getFileListener(request.tenant(), id.toString());
                    listener.updateChannel(channel);
                    return responseText(request.routingContext(), 200).end();
                  } else if (!channel.isEnabled() && channel.isCommissioned()) {
                    return FileListeners.undeployIfDeployed(request, channel).map(
                        na -> responseText(request.routingContext(), 200).end()).mapEmpty();
                  } else if (channel.isEnabled() && !channel.isCommissioned()) {
                    return FileListeners.deployIfNotDeployed(request, channel)
                        .map(message -> responseText(request.routingContext(), 200).end(message)).mapEmpty();
                  } else {
                    return responseText(request.routingContext(), 200).end();
                  }
                });
          } else {
            responseText(request.routingContext(), 404).end("Channel config to update not found");
          }
        }).mapEmpty();
  }

  public static Future<Void> deleteChannel(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return responseText(request.routingContext(), 404)
            .end("Found no channel with tag or id " + channelId + " to delete.").mapEmpty();
      } else {
        return deleteEntity(request, new Channel()).compose(na -> undeployFileListener(request)).mapEmpty();
      }
    });
  }

  public static Future<Void> deployFileListener(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return deployFileListener(request, channelId)
        .onSuccess(response -> responseText(request.routingContext(), 200).end(response)).mapEmpty();
  }

  public static Future<String> deployFileListener(ServiceRequest request, String channelId) {
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return responseText(request.routingContext(), 404)
            .end("Found no channel with tag or id " + channelId + " to deploy.").mapEmpty();
      } else {
        return FileListeners.deployIfNotDeployed(request, channel);
      }
    });
  }

  public static Future<Void> undeployFileListener(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel != null) {
        return FileListeners.undeployIfDeployed(request, channel)
            .onSuccess(response -> responseText(request.routingContext(), 200).end(response)).mapEmpty();
      } else {
        return responseText(request.routingContext(), 404)
            .end("Found no channel with tag or id " + channelId + " to undeploy.").mapEmpty();
      }
    });
  }

  public static Future<Channel> getChannelByTagOrUuid(ServiceRequest request, String channelIdentifier) {
    ModuleStorageAccess db = request.moduleStorageAccess();
    SqlQuery queryFromCql = new Channel().cqlToSql(
        channelIdentifier.length() > 24 ? "id==" + channelIdentifier : "tag==" + channelIdentifier, "0", "1",
        db.schemaDotTable(Tables.CHANNEL), new Channel().getQueryableFields());
    return db.getEntities(queryFromCql.getQueryWithLimits(), new Channel()).map(entities -> {
      if (entities.isEmpty()) {
        return null;
      } else {
        return (Channel) (entities.getFirst());
      }
    });
  }

  public static Future<List<Channel>> getDeployableChannels(ServiceRequest request) {
    ModuleStorageAccess db = request.moduleStorageAccess();
    SqlQuery queryFromCql = new Channel().cqlToSql("enabled==true", "0", "100",
        db.schemaDotTable(Tables.CHANNEL), new Channel().getQueryableFields());
    return db.getEntities(queryFromCql.getQueryWithLimits(), new Channel())
        .compose(entities -> {
          List<Channel> deployableChannels = entities.stream().map(entity -> (Channel) entity)
              .filter(channel -> !channel.isCommissioned()).toList();
          return Future.succeededFuture(deployableChannels);
        });
  }

  public static Future<Void> recoverChannels(ServiceRequest request) {
    // If channel recovery was not already requested once
    // OR if this is an explicit user request (as opposed to a system request),
    // then do attempt recovery.
    if (!channelsAlreadyBootstrapped.getAndSet(true) || request.currentUser() != null) {
      return getDeployableChannels(request).compose(channels -> {
        if (channels.isEmpty()) {
          return responseText(request.routingContext(), 200).end("Found no channels to re-deploy").mapEmpty();
        } else {
          List<Future<String>> deploymentFutures = new ArrayList<>();
          for (Channel channel : channels) {
            deploymentFutures.add(deployFileListener(request, channel.getId().toString()));
          }
          return Future.join(deploymentFutures)
              .compose(deployments -> responseText(request.routingContext(), 200)
                  .end("Deployed: " + deployments.list().toString()));
        }
      });
    } else {
      return responseText(request.routingContext(), 200).end();
    }
  }

  public static Future<Void> initFileSystemQueue(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel != null) {
        String initMessage = new FileQueue(request, channelId).initializeQueue();
        return responseText(request.routingContext(), 200).end(initMessage).mapEmpty();
      } else {
        return responseText(request.routingContext(), 404)
            .end("Could not find channel [" + channelId + "].").mapEmpty();
      }
    });
  }
}
