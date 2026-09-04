package org.folio.inventoryupdate.importing.service;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.importing.moduledata.Channel;

public class Messaging {
  public static final String CHANNELS = "channels";

  public static void publishChannelUpdate(Vertx vertx, Channel channel) {
    vertx.eventBus().publish(Messaging.CHANNELS + "-" + channel.getId().toString(), channel.asJson());
  }

  public static void consumeChannelUpdates(Vertx vertx, String channelId, Handler<Message<JsonObject>> handler) {
    vertx.eventBus().consumer(CHANNELS + "-" + channelId, handler);
  }
}
