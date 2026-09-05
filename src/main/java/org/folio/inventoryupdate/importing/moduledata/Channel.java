package org.folio.inventoryupdate.importing.moduledata;

import static org.folio.inventoryupdate.importing.utils.DateTimeFormatter.formatDateTime;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
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
import org.folio.inventoryupdate.importing.moduledata.database.PgColumn.Type;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.moduledata.database.Util;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.tlib.postgres.TenantPgPool;

public class Channel extends Entity {

  public static final String ID = "ID";
  public static final String TAG = "TAG";
  public static final String NAME = "NAME";
  public static final String TYPE = "TYPE";
  public static final String TRANSFORMATION_ID = "TRANSFORMATION_ID";
  public static final String HARVEST_URL = "HARVEST_URL";
  public static final String LAST_HARVESTED = "LAST_HARVESTED";
  public static final String ENABLED = "ENABLED";
  public static final String LISTENING = "LISTENING";
  public static final String DEPLOYMENT_ID = "DEPLOYMENT_ID";
  // virtual (non-db) property
  public static final String PROPERTY_COMMISSIONED = "commissioned";
  private static final Map<String, Field> CHANNEL_FIELDS = new HashMap<>();
  ChannelRecord theRecord;
  private int queueLength = 0;
  private String nameOfProcessingFile = "no file processing";
  private boolean isCommissioned = false;

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
    CHANNEL_FIELDS.put(HARVEST_URL,
        new Field("harvestUrl", "harvest_url", PgColumn.Type.TEXT, true, true));
    CHANNEL_FIELDS.put(LAST_HARVESTED,
        new Field("lastHarvested", "last_harvested", PgColumn.Type.TIMESTAMP, true, true));
    CHANNEL_FIELDS.put(ENABLED,
        new Field("enabled", "enabled", PgColumn.Type.BOOLEAN, false, true));
    CHANNEL_FIELDS.put(LISTENING,
        new Field("listening", "listening", PgColumn.Type.BOOLEAN, false, true));
    CHANNEL_FIELDS.put(DEPLOYMENT_ID,
        new Field("deploymentId", "deployment_id", Type.TEXT, true, true).isRestricted());
  }

  public Channel() {
  }

  public Channel(UUID id, String name, String tag, String type, UUID transformationId, String harvestUrl,
                 String lastHarvested, boolean enabled, boolean listening, String deploymentId) {
    theRecord = new ChannelRecord(id, name, tag, type, transformationId, harvestUrl, lastHarvested, enabled,
        listening, deploymentId);
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

  public Channel withNameOfProcessingFile(String fileName) {
    this.nameOfProcessingFile = fileName;
    return this;
  }

  public Channel withLengthOfQueue(int size) {
    this.queueLength = size;
    return this;
  }

  public Channel withCommissioned(boolean isCommissioned) {
    this.isCommissioned = isCommissioned;
    return this;
  }

  public Channel fromJson(JsonObject channelJson) {
    return new Channel(
        getUuidOrGenerate(channelJson.getString(jsonPropertyName(ID))),
        channelJson.getString(jsonPropertyName(NAME)),
        channelJson.getString(jsonPropertyName(TAG)),
        channelJson.getString(jsonPropertyName(TYPE)),
        Util.getUuid(channelJson, jsonPropertyName(TRANSFORMATION_ID)),
        channelJson.getString(jsonPropertyName(HARVEST_URL)),
        channelJson.getString(jsonPropertyName(LAST_HARVESTED)),
        "TRUE".equalsIgnoreCase(channelJson.getString(jsonPropertyName(ENABLED))),
        "TRUE".equalsIgnoreCase(channelJson.getString(jsonPropertyName(LISTENING))),
        channelJson.getString(jsonPropertyName(DEPLOYMENT_ID)));
  }

  @Override
  public RowMapper<Entity> fromRow() {
    return row -> {
      return new Channel(
          row.getUUID(dbColumnName(ID)),
          row.getString(dbColumnName(NAME)),
          row.getString(dbColumnName(TAG)),
          row.getString(dbColumnName(TYPE)),
          row.getUUID(dbColumnName(TRANSFORMATION_ID)),
          row.getString(dbColumnName(HARVEST_URL)),
          row.getValue(dbColumnName(LAST_HARVESTED)) != null
              ? formatDateTime(row.getLocalDateTime(dbColumnName(LAST_HARVESTED))) : null,
          row.getBoolean(dbColumnName(ENABLED)),
          row.getBoolean(dbColumnName(LISTENING)),
          row.getString(dbColumnName(DEPLOYMENT_ID)))
          .withMetadata(row);
    };
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
          parameters.put(dbColumnName(HARVEST_URL), rec.harvestUrl());
          parameters.put(dbColumnName(LAST_HARVESTED), rec.lastHarvested());
          parameters.put(dbColumnName(ENABLED), rec.enabled());
          parameters.put(dbColumnName(LISTENING), rec.listening());
          putMetadata(parameters);
          return parameters;
        });
  }

  public TupleMapper<Entity> toParametersForSettingDeployment() {
    return TupleMapper.mapper(
        entity -> {
          ChannelRecord rec = ((Channel) entity).theRecord;
          Map<String, Object> parameters = new HashMap<>();
          parameters.put(dbColumnName(ID), rec.id());
          parameters.put(dbColumnName(DEPLOYMENT_ID), rec.deploymentId());
          putMetadata(parameters);
          return parameters;
        });
  }

  public Future<Entity> getById(ServiceRequest getOrPutRequest) {
    UUID id = UUID.fromString(getOrPutRequest.requestParam("id"));
    return getById(id, getOrPutRequest.entityStorage())
        .compose(entity -> {
          this.isCommissioned = ((Channel) entity).isCommissioned(getOrPutRequest.vertx());
          return Future.succeededFuture(entity);
        });
  }

  /**
   * Channel pojo to JSON mapping.
   */
  public JsonObject asJson() {
    JsonObject json = new JsonObject();
    json.put(jsonPropertyName(ID), theRecord.id().toString());
    json.put(jsonPropertyName(NAME), theRecord.name());
    putIfNotNull(json, jsonPropertyName(TAG), theRecord.tag());
    json.put(jsonPropertyName(TYPE), theRecord.type());
    json.put(jsonPropertyName(TRANSFORMATION_ID), theRecord.transformationId().toString());
    putIfNotNull(json, jsonPropertyName(HARVEST_URL), theRecord.harvestUrl());
    putIfNotNull(json, jsonPropertyName(LAST_HARVESTED), theRecord.lastHarvested());
    json.put(jsonPropertyName(ENABLED), theRecord.enabled());
    json.put(PROPERTY_COMMISSIONED, isCommissioned);
    json.put(jsonPropertyName(LISTENING), theRecord.listening());
    json.put(jsonPropertyName(DEPLOYMENT_ID), theRecord.deploymentId());
    json.put("queuedFiles", queueLength);
    json.put("fileInProcess", nameOfProcessingFile);

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

  public String getName() {
    return theRecord == null ? null : theRecord.name();
  }

  public String getHarvestUrl() {
    return theRecord == null ? null : theRecord.harvestUrl();
  }

  public String getLastHarvested() {
    return theRecord == null ? null : theRecord.lastHarvested();
  }

  public boolean hasDeploymentId() {
    return theRecord != null && theRecord.deploymentId() != null && ! theRecord.deploymentId().isEmpty();
  }

  public String getDeploymentId() {
    return theRecord == null ? null : theRecord.deploymentId();
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
            + field(HARVEST_URL).pgColumnDdl() + ", "
            + field(LAST_HARVESTED).pgColumnDdl() + ", "
            + field(ENABLED).pgColumnDdl() + ", "
            + field(LISTENING).pgColumnDdl() + ", "
            + metadata.columnsDdl()
            + ")",
        "ALTER TABLE " + pool.getSchema() + "." + table()
            + " ADD COLUMN IF NOT EXISTS " + field(HARVEST_URL).pgColumnDdl(),
        "ALTER TABLE " + pool.getSchema() + "." + table()
            + " ADD COLUMN IF NOT EXISTS " + field(LAST_HARVESTED).pgColumnDdl(),
        "ALTER TABLE " + pool.getSchema() + "." + table()
            + " ADD COLUMN IF NOT EXISTS " + field(DEPLOYMENT_ID).pgColumnDdl()
        ).mapEmpty();
  }

  public boolean isCommissioned(Vertx vertx) {
    if (tenant == null) {
      logger.warn(
          "Tenant not specified for this Channel object ({}), cannot say if the channel is commissioned",
          theRecord.name());
    }
    return tenant != null && this.hasDeploymentId() && vertx.deploymentIDs().contains(getDeploymentId());
  }

  public boolean isEnabled() {
    return theRecord.enabled();
  }

  public boolean isListeningIfEnabled() {
    return theRecord != null && theRecord.listening();
  }

  public boolean hasHarvestUrl() {
    return theRecord.harvestUrl != null && !theRecord.harvestUrl.isBlank();
  }

  public UUID getTransformationId() {
    return theRecord.transformationId;
  }

  public Future<Integer> setDeploymentId(String deploymentId, EntityStorage configStorage) {
    Promise<Integer> promise = Promise.promise();
    theRecord = new ChannelRecord(theRecord.id(), theRecord.name(), theRecord.tag(), theRecord.type(),
        theRecord.transformationId(), theRecord.harvestUrl(), theRecord.lastHarvested(), theRecord.enabled(),
        theRecord.listening, deploymentId);
    configStorage.updateEntity(this.withUpdatingUser(null),
        "UPDATE " + configStorage.schema() + "." + table()
            + " SET "
            + dbColumnName(DEPLOYMENT_ID) + " = #{" + dbColumnName(DEPLOYMENT_ID) + "} "
            + ", "
            + metadata.updateClauseColumnTemplates()
            + " WHERE id = #{id}",
            toParametersForSettingDeployment())
        .onFailure(x -> {
          logger.error("error: {}", x.getMessage());
          promise.complete(0);
        })
        .compose(res -> {
          promise.complete(res.rowCount());
          return Future.succeededFuture(res);
        });
    return promise.future();
  }

  public Future<Integer> setEnabledListening(boolean enabled, boolean listening, EntityStorage configStorage) {
    if (theRecord == null) {
      return Future.succeededFuture(0);
    }
    theRecord = new ChannelRecord(theRecord.id(), theRecord.name(), theRecord.tag(), theRecord.type(),
        theRecord.transformationId(), theRecord.harvestUrl(), theRecord.lastHarvested(), enabled, listening,
        theRecord.deploymentId());
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
        theRecord.transformationId(), theRecord.harvestUrl(), theRecord.lastHarvested(), theRecord.enabled(),
        listening, theRecord.deploymentId());
    return configStorage.updateEntity(this.withUpdatingUser(null),
        "UPDATE " + configStorage.schema() + "." + table()
            + " SET "
            + dbColumnName(LISTENING) + " = #{" + dbColumnName(LISTENING) + "} "
            + ", "
            + metadata.updateClauseColumnTemplates()
            + " WHERE id = #{id}").map(SqlResult::rowCount);
  }

  public Future<Integer> setLastHarvested(String lastHarvested, EntityStorage configStorage) {
    theRecord = new ChannelRecord(theRecord.id(), theRecord.name(), theRecord.tag(), theRecord.type(),
        theRecord.transformationId(), theRecord.harvestUrl(), lastHarvested, theRecord.enabled(),
        theRecord.listening(), theRecord.deploymentId);
    return configStorage.updateEntity(this.withUpdatingUser(null),
        "UPDATE " + configStorage.schema() + "." + table()
            + " SET "
            + dbColumnName(LAST_HARVESTED) + " = "
            + " TO_TIMESTAMP(#{" + dbColumnName(LAST_HARVESTED) + "}, '" + DATE_FORMAT_TO_DB + "'), "
            + metadata.updateClauseColumnTemplates()
            + " WHERE id = #{id}").map(SqlResult::rowCount);
  }

  // Import config record, the entity data.
  public record ChannelRecord(UUID id, String name, String tag, String type, UUID transformationId,
                              String harvestUrl, String lastHarvested, boolean enabled, boolean listening,
                              String deploymentId) {}
}
