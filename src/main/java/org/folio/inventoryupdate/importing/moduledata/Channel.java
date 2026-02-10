package org.folio.inventoryupdate.importing.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.folio.inventoryupdate.importing.moduledata.database.Entity;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.moduledata.database.PgColumn;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.moduledata.database.Util;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileListeners;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileQueue;
import org.folio.tlib.postgres.TenantPgPool;

public class Channel extends Entity {

  public static final String ID = "ID";
  public static final String TAG = "TAG";
  public static final String NAME = "NAME";
  public static final String TYPE = "TYPE";
  public static final String TRANSFORMATION_ID = "TRANSFORMATION_ID";
  public static final String ENABLED = "ENABLED";
  public static final String LISTENING = "LISTENING";
  // virtual (non-db) property
  public static final String PROPERTY_COMMISSIONED = "commissioned";
  private static final Map<String, Field> CHANNEL_FIELDS = new HashMap<>();
  ChannelRecord theRecord;
  private FileQueue fileQueue;

  static {
    CHANNEL_FIELDS.put(ID,
        new Field("id", "id", PgColumn.Type.UUID, false, true).isPrimaryKey());
    CHANNEL_FIELDS.put(NAME,
        new Field("name", "name", PgColumn.Type.TEXT, false, true).isUnique());
    CHANNEL_FIELDS.put(TAG,
        new Field("tag", "tag", PgColumn.Type.TEXT, true, true).isUnique());
    CHANNEL_FIELDS.put(TYPE,
        new Field("type", "type", PgColumn.Type.TEXT, false, true));
    CHANNEL_FIELDS.put(TRANSFORMATION_ID,
        new Field("transformationId", "transformation_id", PgColumn.Type.UUID, false, true));
    CHANNEL_FIELDS.put(ENABLED,
        new Field("enabled", "enabled", PgColumn.Type.BOOLEAN, false, true));
    CHANNEL_FIELDS.put(LISTENING,
        new Field("listening", "listening", PgColumn.Type.BOOLEAN, false, true));
  }

  public Channel() {
  }

  public Channel(UUID id, String name, String tag, String type, UUID transformationId,
                 boolean enabled, boolean listening) {
    theRecord = new ChannelRecord(id, name, tag, type, transformationId, enabled, listening);
  }

  public ChannelRecord getRecord() {
    return theRecord;
  }

  @Override
  public Map<String, Field> fields() {
    return CHANNEL_FIELDS;
  }

  @Override
  public String jsonCollectionName() {
    return "channels";
  }

  @Override
  public String entityName() {
    return "Channel";
  }

  public Channel withFileQueue(FileQueue fileQueue) {
    this.fileQueue = fileQueue;
    return this;
  }

  public Channel fromJson(JsonObject channelJson) {
    return new Channel(
        getUuidOrGenerate(channelJson.getString(jsonPropertyName(ID))),
        channelJson.getString(jsonPropertyName(NAME)),
        channelJson.getString(jsonPropertyName(TAG)),
        channelJson.getString(jsonPropertyName(TYPE)),
        Util.getUuid(channelJson, jsonPropertyName(TRANSFORMATION_ID)),
        "TRUE".equalsIgnoreCase(channelJson.getString(jsonPropertyName(ENABLED))),
        "TRUE".equalsIgnoreCase(channelJson.getString(jsonPropertyName(LISTENING))));
  }

  @Override
  public RowMapper<Entity> fromRow() {
    return row -> new Channel(
        row.getUUID(dbColumnName(ID)),
        row.getString(dbColumnName(NAME)),
        row.getString(dbColumnName(TAG)),
        row.getString(dbColumnName(TYPE)),
        row.getUUID(dbColumnName(TRANSFORMATION_ID)),
        row.getBoolean(dbColumnName(ENABLED)),
        row.getBoolean(dbColumnName(LISTENING)))
        .withMetadata(row);
  }

  @Override
  public TupleMapper<Entity> toTemplateParameters() {
    return TupleMapper.mapper(
        entity -> {
          ChannelRecord rec = ((Channel) entity).theRecord;
          Map<String, Object> parameters = new HashMap<>();
          parameters.put(dbColumnName(ID), rec.id());
          parameters.put(dbColumnName(NAME), rec.name());
          parameters.put(dbColumnName(TAG), rec.tag());
          parameters.put(dbColumnName(TYPE), rec.type());
          parameters.put(dbColumnName(TRANSFORMATION_ID), rec.transformationId());
          parameters.put(dbColumnName(ENABLED), rec.enabled());
          parameters.put(dbColumnName(LISTENING), rec.listening());
          putMetadata(parameters);
          return parameters;
        });
  }

  /**
   * Channel pojo to JSON mapping.
   */
  public JsonObject asJson() {
    JsonObject json = new JsonObject();
    json.put(jsonPropertyName(ID), theRecord.id());
    json.put(jsonPropertyName(NAME), theRecord.name());
    json.put(jsonPropertyName(TAG), theRecord.tag());
    json.put(jsonPropertyName(TYPE), theRecord.type());
    json.put(jsonPropertyName(TRANSFORMATION_ID), theRecord.transformationId());
    json.put(jsonPropertyName(ENABLED), theRecord.enabled());
    json.put(PROPERTY_COMMISSIONED, isCommissioned());
    json.put(jsonPropertyName(LISTENING), theRecord.listening());
    if (fileQueue != null) {
      json.put("queuedFiles", fileQueue.size());
      json.put("fileInProcess", fileQueue.fileInProcess());
    }
    putMetadata(json);
    return json;
  }

  @Override
  public Tables table() {
    return Tables.CHANNEL;
  }

  @Override
  public UUID getId() {
    return theRecord == null ? null : theRecord.id();
  }

  @Override
  public Future<Void> createDatabase(TenantPgPool pool) {
    return executeSqlStatements(pool,
        "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + table()
            + "("
            + field(ID).pgColumnDdl() + ", "
            + field(TAG).pgColumnDdl() + ", "
            + field(NAME).pgColumnDdl() + ", "
            + field(TYPE).pgColumnDdl() + ", "
            + field(TRANSFORMATION_ID).pgColumnDdl()
            + " REFERENCES " + pool.getSchema() + "." + Tables.TRANSFORMATION
            + " (" + new Transformation().dbColumnName(Transformation.ID) + "), "
            + field(ENABLED).pgColumnDdl() + ", "
            + field(LISTENING).pgColumnDdl() + ", "
            + metadata.columnsDdl()
            + ")"
    ).mapEmpty();
  }

  public boolean isCommissioned() {
    if (tenant == null) {
      logger.warn(
          "Tenant not specified for this Channel object ({}), cannot say if the channel is commissioned",
          theRecord.name());
    }
    return tenant != null && FileListeners.hasFileListener(tenant, theRecord.id().toString());
  }

  public boolean isEnabled() {
    return theRecord.enabled();
  }

  public boolean isListeningIfEnabled() {
    return theRecord.listening();
  }

  public UUID getTransformationId() {
    return theRecord.transformationId;
  }

  public Future<Integer> setEnabledListening(boolean enabled, boolean listening, EntityStorage configStorage) {
    theRecord = new ChannelRecord(theRecord.id(), theRecord.name(), theRecord.tag(), theRecord.type(),
        theRecord.transformationId(), enabled, listening);
    return configStorage.updateEntity(this.withUpdatingUser(null),
        "UPDATE " + configStorage.schema() + "." + table()
            + " SET "
            + dbColumnName(ENABLED) + " = #{" + dbColumnName(ENABLED) + "} "
            + ", "
            + dbColumnName(LISTENING) + " = #{" + dbColumnName(LISTENING) + "} "
            + ", "
            + metadata.updateClauseColumnTemplates()
            + " WHERE id = #{id}").map(SqlResult::rowCount);
  }

  public Future<Integer> setListening(boolean listening, EntityStorage configStorage) {
    theRecord = new ChannelRecord(theRecord.id(), theRecord.name(), theRecord.tag(), theRecord.type(),
        theRecord.transformationId(), theRecord.enabled(), listening);
    return configStorage.updateEntity(this.withUpdatingUser(null),
        "UPDATE " + configStorage.schema() + "." + table()
            + " SET "
            + dbColumnName(LISTENING) + " = #{" + dbColumnName(LISTENING) + "} "
            + ", "
            + metadata.updateClauseColumnTemplates()
            + " WHERE id = #{id}").map(SqlResult::rowCount);
  }

  // Import config record, the entity data.
  public record ChannelRecord(UUID id, String name, String tag, String type, UUID transformationId,
                              boolean enabled, boolean listening) {}
}
