package org.folio.inventoryupdate.importing.service;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.importing.moduledata.Channel;

public class Messaging {
  public static final String CHANNELS = "channels";
  public static final String IMPORT_JOBS = "importJobs";

  public static final JsonObject COMMAND_IMPORT_JOB_PAUSE
      = new JsonObject().put("command", "pause");

  public static final JsonObject COMMAND_IMPORT_JOB_RESUME
      = new JsonObject("{\"command\": \"resume\", \"discardFileInProcess\": false}");

  public static final JsonObject COMMAND_IMPORT_JOB_RESUME_DISCARD_FILE
      = new JsonObject("{\"command\": \"resume\", \"discardFileInProcess\": true}");


  public static void publishChannelUpdate(Vertx vertx, Channel channel) {
    vertx.eventBus().publish(Messaging.CHANNELS + "-" + channel.getId().toString(), channel.asJson());
  }

  public static void consumeChannelUpdates(Vertx vertx, String channelId, Handler<Message<JsonObject>> handler) {
    vertx.eventBus().consumer(CHANNELS + "-" + channelId, handler);
  }

  public static void publishImportJobCommand(Vertx vertx, Channel channel, JsonObject command) {
    vertx.eventBus().publish(Messaging.IMPORT_JOBS + "-" + channel.getId().toString(), command);
  }

  public static void consumeImportJobCommands(Vertx vertx, String channelId, Handler<Message<JsonObject>> handler) {
    vertx.eventBus().consumer(IMPORT_JOBS + "-" + channelId, handler);
  }
}
