package org.folio.inventoryupdate.importing.moduledata;

import static org.folio.inventoryupdate.importing.utils.DateTimeFormatter.formatDateTime;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.SqlTemplate;
import io.vertx.sqlclient.templates.TupleMapper;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.folio.inventoryupdate.importing.moduledata.database.Entity;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.moduledata.database.PgColumn;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.moduledata.database.Util;
import org.folio.inventoryupdate.importing.utils.SettableClock;
import org.folio.tlib.postgres.TenantPgPool;

public class ImportJob extends Entity {

  public static final String ID = "ID";
  public static final String CHANNEL_ID = "CHANNEL_ID";
  public static final String CHANNEL_NAME = "CHANNEL_NAME";
  public static final String IMPORT_TYPE = "IMPORT_TYPE";
  public static final String TRANSFORMATION = "TRANSFORMATION";
  public static final String STATUS = "STATUS";
  public static final String STARTED = "STARTED";
  public static final String FINISHED = "FINISHED";
  public static final String AMOUNT_IMPORTED = "AMOUNT_IMPORTED";
  public static final String MESSAGE = "MESSAGE";
  private static final Map<String, Field> FIELDS = new HashMap<>();

  static {
    FIELDS.put(ID,
        new Field("id", "id", PgColumn.Type.UUID, false, true).isPrimaryKey());
    FIELDS.put(CHANNEL_ID,
        new Field("channelId", "channel_id", PgColumn.Type.UUID, false, true));
    FIELDS.put(CHANNEL_NAME,
        new Field("channelName", "channel_name", PgColumn.Type.TEXT, false, true));
    FIELDS.put(IMPORT_TYPE,
        new Field("importType", "import_type", PgColumn.Type.TEXT, false, true));
    FIELDS.put(TRANSFORMATION,
        new Field("transformation", "transformation_id", PgColumn.Type.UUID, false, true));
    FIELDS.put(STATUS,
        new Field("status", "status", PgColumn.Type.TEXT, true, true));
    FIELDS.put(STARTED,
        new Field("started", "started", PgColumn.Type.TIMESTAMP, false, true));
    FIELDS.put(FINISHED,
        new Field("finished", "finished", PgColumn.Type.TIMESTAMP, true, true));
    FIELDS.put(AMOUNT_IMPORTED,
        new Field("amountImported", "amount_imported", PgColumn.Type.INTEGER, true, true));
    FIELDS.put(MESSAGE,
        new Field("message", "message", PgColumn.Type.TEXT, true, true));
  }

  public enum JobStatus {
    RUNNING,
    DONE,
    PAUSED,
    HALTED,
    INTERRUPTED
  }

  ImportJobRecord theRecord;

  public ImportJob() {
  }

  @SuppressWarnings("java:S107") // too many parameters, ignore for entity constructors
  public ImportJob(UUID id,
                   UUID channelId,
                   String channelName,
                   String importType,
                   UUID transformation,
                   JobStatus status,
                   String started,
                   String finished,
                   Integer amountImported,
                   String message) {
    theRecord = new ImportJob.ImportJobRecord(
        id, channelId, channelName, importType, transformation, status, started, finished, amountImported, message);
  }

  public ImportJobRecord getRecord() {
    return theRecord;
  }

  /**
   * Implement to return an enum identifier for the underlying database table for the implementing entity.
   *
   * @return a Tables enum value
   */
  @Override
  public Tables table() {
    return Tables.IMPORT_JOB;
  }

  @Override
  public UUID getId() {
    return theRecord == null ? null : theRecord.id();
  }

  /**
   * For building JSON collection response.
   *
   * @return the JSON property name for a collection of the entity
   */
  @Override
  public String jsonCollectionName() {
    return "importJobs";
  }

  /**
   * For logging and response messages.
   *
   * @return label to display in messages.
   */
  @Override
  public String entityName() {
    return "Import job";
  }

  /**
   * Implement to provide a map of the {@link Field} fields of the implementing entity.
   *
   * @return Map fields by field keys to be used for finding queryable fields or, if possible, for creating
   *   the database table and more.
   */
  @Override
  public Map<String, Field> fields() {
    return FIELDS;
  }

  public ImportJob initiate(Channel channel) {
    Channel.ChannelRecord cfg = channel.theRecord;
    return new ImportJob(UUID.randomUUID(), cfg.id(), cfg.name(), cfg.type(),
        cfg.transformationId(),
        JobStatus.RUNNING, SettableClock.getLocalDateTime().toString(), "", 0, "");
  }

  public boolean markedRunning() {
    return theRecord.status().equals(JobStatus.RUNNING);
  }

  /**
   * Implement to map from request body JSON to entity POJO.
   *
   * @param json incoming JSON body
   * @return Entity POJO
   */
  @Override
  public ImportJob fromJson(JsonObject json) {
    String started = json.getString(jsonPropertyName(STARTED));
    String finished = json.getString(jsonPropertyName(FINISHED));
    return new ImportJob(
        getUuidOrGenerate(json.getString(jsonPropertyName(ID))),
        UUID.fromString(json.getString(jsonPropertyName(CHANNEL_ID))),
        json.getString(jsonPropertyName(CHANNEL_NAME)),
        json.getString(jsonPropertyName(IMPORT_TYPE)),
        Util.getUuid(json, jsonPropertyName(TRANSFORMATION)),
        JobStatus.valueOf(json.getString(jsonPropertyName(STATUS))),
        json.getString(jsonPropertyName(STARTED)),
        finished != null && started != null && started.compareTo(finished) < 0
            ? json.getString(jsonPropertyName(FINISHED)) : null,
        json.getInteger(jsonPropertyName(AMOUNT_IMPORTED)),
        json.getString(jsonPropertyName(MESSAGE)));
  }

  /**
   * Implement to map for entity POJO to response JSON.
   *
   * @return json representation of the entity
   */
  @Override
  public JsonObject asJson() {
    JsonObject json = new JsonObject();
    json.put(jsonPropertyName(ID), theRecord.id);
    json.put(jsonPropertyName(CHANNEL_ID), theRecord.channelId);
    json.put(jsonPropertyName(CHANNEL_NAME), theRecord.channelName);
    json.put(jsonPropertyName(IMPORT_TYPE), theRecord.importType);
    json.put(jsonPropertyName(TRANSFORMATION), theRecord.transformation);
    json.put(jsonPropertyName(STATUS), theRecord.status);
    json.put(jsonPropertyName(STARTED), theRecord.started);
    json.put(jsonPropertyName(FINISHED), theRecord.finished);
    json.put(jsonPropertyName(AMOUNT_IMPORTED), theRecord.amountImported);
    json.put(jsonPropertyName(MESSAGE), theRecord.message);
    putMetadata(json);
    return json;
  }

  /**
   * Creates vert.x row mapper that maps a database select result row onto data object(s).
   */
  @Override
  public RowMapper<Entity> fromRow() {
    return row -> new ImportJob(
        row.getUUID(dbColumnName(ID)),
        row.getUUID(dbColumnName(CHANNEL_ID)),
        row.getString(dbColumnName(CHANNEL_NAME)),
        row.getString(dbColumnName(IMPORT_TYPE)),
        row.getUUID(dbColumnName(TRANSFORMATION)),
        JobStatus.valueOf(row.getString(dbColumnName(STATUS))),
        formatDateTime(row.getLocalDateTime(dbColumnName(STARTED))),
        row.getValue(dbColumnName(FINISHED)) != null
            ? formatDateTime(row.getLocalDateTime(dbColumnName(FINISHED))) : null,
        row.getValue(dbColumnName(AMOUNT_IMPORTED)) != null
            ? row.getInteger(dbColumnName(AMOUNT_IMPORTED)) : null,
        row.getString(dbColumnName(MESSAGE))).withMetadata(row);
  }

  /**
   * Creates vert.x tuple mapper to field values from Postgres column names. Used with insert and update templates.
   */
  @Override
  public TupleMapper<Entity> toTemplateParameters() {
    return TupleMapper.mapper(
        entity -> {
          ImportJob.ImportJobRecord rec = ((ImportJob) entity).theRecord;
          Map<String, Object> parameters = new HashMap<>();
          parameters.put(dbColumnName(ID), rec.id);
          parameters.put(dbColumnName(CHANNEL_ID), rec.channelId);
          parameters.put(dbColumnName(CHANNEL_NAME), rec.channelName);
          parameters.put(dbColumnName(IMPORT_TYPE), rec.importType);
          parameters.put(dbColumnName(TRANSFORMATION), rec.transformation);
          parameters.put(dbColumnName(STATUS), rec.status);
          if (rec.started != null) {
            parameters.put(dbColumnName(STARTED), rec.started);
          }
          if (rec.finished != null) {
            parameters.put(dbColumnName(FINISHED), rec.finished);
          }
          if (rec.amountImported != null) {
            parameters.put(
                dbColumnName(AMOUNT_IMPORTED), rec.amountImported);
          }
          parameters.put(dbColumnName(MESSAGE), rec.message);
          putMetadata(parameters);
          return parameters;
        });
  }

  @Override
  public String insertTemplate(String schema) {
    return "INSERT INTO " + schema + "." + table()
        + " ("
        + dbColumnName(ID) + ", "
        + dbColumnName(CHANNEL_ID) + ", "
        + dbColumnName(CHANNEL_NAME) + ", "
        + dbColumnName(IMPORT_TYPE) + ", "
        + dbColumnName(TRANSFORMATION) + ", "
        + dbColumnName(STATUS) + ", "
        + dbColumnName(STARTED) + ", "
        + dbColumnName(AMOUNT_IMPORTED) + ", "
        + dbColumnName(MESSAGE) + ", "
        + metadata.insertClauseColumns()
        + ")"
        + " VALUES ("
        + "#{" + dbColumnName(ID) + "}, "
        + "#{" + dbColumnName(CHANNEL_ID) + "}, "
        + "#{" + dbColumnName(CHANNEL_NAME) + "}, "
        + "#{" + dbColumnName(IMPORT_TYPE) + "}, "
        + "#{" + dbColumnName(TRANSFORMATION) + "}, "
        + "#{" + dbColumnName(STATUS) + "}, "
        + "TO_TIMESTAMP(#{" + dbColumnName(STARTED) + "},'" + DATE_FORMAT_TO_DB + "'), "
        + "#{" + dbColumnName(AMOUNT_IMPORTED) + "}, "
        + "#{" + dbColumnName(MESSAGE) + "}, "
        + metadata.insertClauseValueTemplates()
        + ")";
  }

  public Future<Integer> changeRunningToInterruptedByChannelId(EntityStorage db, String channelId) {
    String template = "UPDATE " + db.schema() + "." + table()
        + " SET "
        + dbColumnName(STATUS) + " = '"  + JobStatus.INTERRUPTED.name() + "', "
        + metadata.updateClauseColumnTemplates()
        + " WHERE " + dbColumnName(CHANNEL_ID) + " = '" + channelId + "'"
        + "  AND " + dbColumnName(STATUS) + " = '" + JobStatus.RUNNING.name() + "'";
    return db.updateEntitiesByStatement(this.withUpdatingUser(null), template)
        .map(SqlResult::rowCount);
  }

  public void logFinish(LocalDateTime finished, int recordCount, EntityStorage configStorage) {
    setFinished(finished, recordCount);
    configStorage.updateEntity(this.withUpdatingUser(null),
        "UPDATE " + configStorage.schema() + "." + table()
            + " SET "
            + dbColumnName(FINISHED)
            + " = TO_TIMESTAMP(#{" + dbColumnName(FINISHED) + "}, '" + DATE_FORMAT_TO_DB + "') "
            + ", "
            + dbColumnName(STATUS) + " = #{" + dbColumnName(STATUS) + "} "
            + ", "
            + dbColumnName(AMOUNT_IMPORTED) + " = #{" + dbColumnName(AMOUNT_IMPORTED) + "}"
            + ", "
            + metadata.updateClauseColumnTemplates()
            + " WHERE id = #{id}");
  }

  private void setFinished(LocalDateTime finished, int recordCount) {
    theRecord = new ImportJobRecord(theRecord.id, theRecord.channelId, theRecord.channelName, theRecord.importType,
        theRecord.transformation,
        JobStatus.DONE, theRecord.started, finished.toString(), recordCount, theRecord.message);
  }

  public void logStatus(JobStatus status, String message, int recordCount, EntityStorage configStorage) {
    theRecord = new ImportJobRecord(theRecord.id, theRecord.channelId, theRecord.channelName, theRecord.importType,
        theRecord.transformation,
        status, theRecord.started, theRecord.finished, recordCount, message);
    configStorage.updateEntity(this.withUpdatingUser(null),
        "UPDATE " + configStorage.schema() + "." + table()
            + " SET "
            + dbColumnName(STATUS) + " = #{" + dbColumnName(STATUS) + "} "
            + ", "
            + dbColumnName(MESSAGE) + " = #{" + dbColumnName(MESSAGE) + "} "
            + ", "
            + dbColumnName(AMOUNT_IMPORTED) + " = #{" + dbColumnName(AMOUNT_IMPORTED) + "}"
            + ", "
            + metadata.updateClauseColumnTemplates()
            + " WHERE id = #{id}");
  }

  @Override
  public Future<Void> createDatabase(TenantPgPool pool) {
    return executeSqlStatements(pool,
        "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + table()
            + "("
            + dbColumnNameAndType(ID) + " PRIMARY KEY, "
            + dbColumnNameAndType(CHANNEL_ID) + " NOT NULL CONSTRAINT import_job_channel_id_fkey "
            + "REFERENCES " + pool.getSchema() + "." + Tables.CHANNEL + " (" + new Channel().dbColumnName(ID) + ") "
            + " ON DELETE CASCADE, "
            + dbColumnNameAndType(CHANNEL_NAME) + ", "
            + dbColumnNameAndType(IMPORT_TYPE) + ", "
            + dbColumnNameAndType(TRANSFORMATION) + ", "
            + dbColumnNameAndType(STATUS) + ", "
            + dbColumnNameAndType(STARTED) + " NOT NULL, "
            + dbColumnNameAndType(FINISHED) + ", "
            + dbColumnNameAndType(AMOUNT_IMPORTED) + ", "
            + dbColumnNameAndType(MESSAGE) + ", "
            + metadata.columnsDdl() + ") ",
        "CREATE INDEX IF NOT EXISTS import_job_channel_id_idx "
            + " ON " + pool.getSchema() + "." + table() + "(" + dbColumnName(CHANNEL_ID) + ")",
        "ALTER TABLE " + pool.getSchema() +  "." + table() + " DROP CONSTRAINT import_job_channel_id_fkey",
        "ALTER TABLE " + pool.getSchema() +  "." + table()
            + " ADD CONSTRAINT import_job_channel_id_fkey FOREIGN KEY (" + dbColumnName(CHANNEL_ID) + ")"
            + " REFERENCES " + pool.getSchema() + "." + Tables.CHANNEL + " (" + new Channel().dbColumnName(ID) + ") "
            + " ON DELETE CASCADE"
    ).mapEmpty();
  }

  public Future<Integer> countImportJobsByChannelId(TenantPgPool pool, String channelId) {
    return SqlTemplate.forQuery(pool.getPool(),
        "SELECT COUNT(*) AS import_jobs_count FROM " + pool.getSchema() + "." + table()
        + " WHERE " + dbColumnName(CHANNEL_ID) + " = #{channelId}")
        .execute(Collections.singletonMap("channelId", channelId))
        .map(rows -> rows.iterator().next().getInteger("import_jobs_count"));
  }

  public record ImportJobRecord(UUID id,
                                UUID channelId,
                                String channelName,
                                String importType,
                                UUID transformation,
                                JobStatus status,
                                String started,
                                String finished,
                                Integer amountImported,
                                String message) {
  }
}
