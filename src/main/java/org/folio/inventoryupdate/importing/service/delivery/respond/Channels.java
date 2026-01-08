package org.folio.inventoryupdate.importing.service.delivery.respond;

import static org.folio.okapi.common.HttpResponse.responseJson;
import static org.folio.okapi.common.HttpResponse.responseText;

import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.moduledata.database.SqlQuery;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileListener;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileListeners;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileQueue;

public final class Channels extends EntityResponses {

  private static final AtomicBoolean CHANNELS_ALREADY_BOOTSTRAPPED = new AtomicBoolean(false);

  private Channels() {
    throw new UnsupportedOperationException("Static storage utilities");
  }

  public static Future<Void> postChannel(ServiceRequest request) {
    Channel channel = new Channel().fromJson(request.bodyAsJson());
    EntityStorage db = request.entityStorage();
    return db.storeEntity(channel.withCreatingUser(request.currentUser()))
        .map(id -> {
          FileQueue.get(request, id.toString()).createDirectoriesIfNotExist();
          return id;
        }).compose(id -> db.getEntity(id, channel)).compose(cfg -> {
          if (((Channel) cfg).isEnabled()) {
            return FileListeners.deployIfNotDeployed(request, (Channel) cfg).map(na -> cfg)
                .compose(na -> responseJson(request.routingContext(), 201).end(cfg.asJson().encodePrettily()))
                .mapEmpty();
          } else {
            return responseJson(request.routingContext(), 201).end(cfg.asJson().encodePrettily()).mapEmpty();
          }
        });
  }

  public static Future<Void> getChannels(ServiceRequest request) {
    return getEntitiesAndRespond(request, new Channel());
  }

  public static Future<Void> getChannelById(ServiceRequest request) {
    return getEntityAndRespond(request, new Channel());
  }

  public static Future<Void> putChannel(ServiceRequest request) {
    Channel inputChannel = new Channel().fromJson(request.bodyAsJson());
    UUID id = UUID.fromString(request.requestParam("id"));
    return request.entityStorage().updateEntity(id, inputChannel.withUpdatingUser(request.currentUser()))
        .compose(result -> {
          if (result.rowCount() == 1) {
            return request.entityStorage()
                .getEntity(id, new Channel())
                .map(Channel.class::cast)
                .compose(channel -> {
                  if (channel.isEnabled() && channel.isCommissioned()) {
                    FileListeners.getFileListener(request.tenant(), id.toString()).updateChannel(channel);
                    return Future.succeededFuture();
                  } else if (!channel.isEnabled() && channel.isCommissioned()) {
                    return FileListeners.undeployIfDeployed(request, channel);
                  } else if (channel.isEnabled() && !channel.isCommissioned()) {
                    return FileListeners.deployIfNotDeployed(request, channel);
                  } else {
                    return Future.succeededFuture();
                  }
                })
                .compose(na -> responseText(request.routingContext(), 200).end())
                .mapEmpty();
          } else {
            return responseText(request.routingContext(), 404).end("Channel config to update not found");
          }
        });
  }

  public static Future<Void> deleteChannel(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return responseText(request.routingContext(), 404)
            .end("Found no channel with tag or id " + channelId + " to delete.").mapEmpty();
      } else {
        return deleteEntityAndRespond(request, new Channel()).compose(na -> decommission(request)).mapEmpty();
      }
    });
  }

  public static Future<Void> commission(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return commission(request, channelId)
        .onSuccess(response -> responseText(request.routingContext(), 200).end(response)).mapEmpty();
  }

  public static Future<String> commission(ServiceRequest request, String channelId) {
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return responseText(request.routingContext(), 404)
            .end("Found no channel with tag or id " + channelId + " to deploy.").mapEmpty();
      } else {
        return FileListeners.deployIfNotDeployed(request, channel);
      }
    });
  }

  public static Future<Void> decommission(ServiceRequest request) {
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

  public static Future<Void> listen(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel != null) {
        return channel.setListening(true, request.entityStorage())
            .compose(na -> {
              FileListener listener = FileListeners.getFileListener(request.tenant(), channel.getId().toString());
              if (listener != null) {
                listener.updateChannel(channel);
              }
              return Future.succeededFuture();
            })
            .onSuccess(response -> responseText(request.routingContext(), 200)
                .end("Channel " + channelId + " set to listen for source files.")).mapEmpty();
      } else {
        return responseText(request.routingContext(), 404)
            .end("Found no channel with tag or id " + channelId + " to turn listening on for.").mapEmpty();
      }
    });
  }

  public static Future<Void> noListen(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel != null) {
        return channel.setListening(false, request.entityStorage())
            .compose(na -> {
              FileListener listener = FileListeners.getFileListener(request.tenant(), channel.getId().toString());
              if (listener != null) {
                listener.updateChannel(channel);
              }
              return Future.succeededFuture();
            })
            .onSuccess(response -> responseText(request.routingContext(), 200)
                .end("Channel " + channelId + " set to not listen for source files.")).mapEmpty();
      } else {
        return responseText(request.routingContext(), 404)
            .end("Found no channel with tag or id " + channelId + " to turn listening off for.").mapEmpty();
      }
    });
  }

  public static Future<Channel> getChannelByTagOrUuid(ServiceRequest request, String channelIdentifier) {
    EntityStorage db = request.entityStorage();
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
    EntityStorage db = request.entityStorage();
    SqlQuery queryFromCql = new Channel().cqlToSql("enabled==true", "0", "100",
        db.schemaDotTable(Tables.CHANNEL), new Channel().getQueryableFields());
    return db.getEntities(queryFromCql.getQueryWithLimits(), new Channel())
        .compose(entities -> {
          List<Channel> deployableChannels = entities.stream().map(Channel.class::cast)
              .filter(channel -> !channel.isCommissioned()).toList();
          return Future.succeededFuture(deployableChannels);
        });
  }

  public static Future<Void> recoverChannels(ServiceRequest request) {
    // If channel recovery was not already requested once
    // OR if this is an explicit user request (as opposed to a system request),
    // then do attempt recovery.
    if (!CHANNELS_ALREADY_BOOTSTRAPPED.getAndSet(true) || request.currentUser() != null) {
      return getDeployableChannels(request).compose(channels -> {
        if (channels.isEmpty()) {
          return responseText(request.routingContext(), 200).end("Found no channels to re-deploy").mapEmpty();
        } else {
          List<Future<String>> deploymentFutures = new ArrayList<>();
          for (Channel channel : channels) {
            deploymentFutures.add(commission(request, channel.getId().toString()));
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
        String initMessage = FileQueue.get(request, channelId).initialize();
        return responseText(request.routingContext(), 200).end(initMessage).mapEmpty();
      } else {
        return responseText(request.routingContext(), 404)
            .end("Could not find channel [" + channelId + "].").mapEmpty();
      }
    });
  }
}
