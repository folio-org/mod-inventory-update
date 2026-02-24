package org.folio.inventoryupdate.importing.moduledata;

import static org.folio.inventoryupdate.importing.utils.DateTimeFormatter.formatDateTime;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.folio.inventoryupdate.importing.moduledata.database.Entity;
import org.folio.inventoryupdate.importing.moduledata.database.PgColumn;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.tlib.postgres.PgCqlDefinition;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldText;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldTimestamp;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldUuid;

public class RecordFailure extends Entity {

  public static final String ID = "ID";
  public static final String IMPORT_JOB_ID = "IMPORT_JOB_ID";
  public static final String VIEW_CHANNEL_ID = "CHANNEL_ID";
  public static final String VIEW_CHANNEL_NAME = "CHANNEL_NAME";
  public static final String RECORD_NUMBER = "RECORD_NUMBER";
  public static final String TIME_STAMP = "TIME_STAMP";
  public static final String ORIGINAL_RECORD = "ORIGINAL_RECORD";
  public static final String RECORD_ERRORS = "RECORD_ERRORS";
  public static final String TRANSFORMED_RECORD = "TRANSFORMED_RECORD";
  public static final String SOURCE_FILE_NAME = "SOURCE_FILE_NAME";
  private static final Map<String, Field> FIELDS = new HashMap<>();
  private static final String DATE_FORMAT = "YYYY-MM-DD''T''HH24:MI:SS,MS";
  FailedRecord theRecord;
  private boolean usingView = false;

  static {
    FIELDS.put(ID,
        new Field("id", "id", PgColumn.Type.UUID, false, true).isPrimaryKey());
    FIELDS.put(IMPORT_JOB_ID,
        new Field("importJobId", "import_job_id", PgColumn.Type.UUID, false, true));
    FIELDS.put(VIEW_CHANNEL_ID,
        new Field("channelId", "channel_id", PgColumn.Type.UUID, true, true));
    FIELDS.put(VIEW_CHANNEL_NAME,
        new Field("channelName", "channel_name", PgColumn.Type.TEXT, true, true));
    FIELDS.put(RECORD_NUMBER,
        new Field("recordNumber", "record_number", PgColumn.Type.TEXT, true, true));
    FIELDS.put(TIME_STAMP,
        new Field("timeStamp", "time_stamp", PgColumn.Type.TIMESTAMP, false, true));
    FIELDS.put(ORIGINAL_RECORD,
        new Field("originalRecord", "original_record", PgColumn.Type.TEXT, false, false));
    FIELDS.put(RECORD_ERRORS,
        new Field("recordErrors", "record_errors", PgColumn.Type.JSONB, false, false));
    FIELDS.put(TRANSFORMED_RECORD,
        new Field("transformedRecord", "transformed_record", PgColumn.Type.JSONB, false, false));
    FIELDS.put(SOURCE_FILE_NAME,
        new Field("sourceFileName", "source_file_name", PgColumn.Type.TEXT, true, true));
  }

  public RecordFailure() {
  }

  @SuppressWarnings("java:S107") // too many parameters, ignore for entity constructors
  public RecordFailure(UUID id, UUID importJobId, UUID channelId, String channelName, String recordNumber,
                       String timeStamp, String originalRecord, JsonArray recordErrors, JsonObject transformedRecord,
                       String sourceFileName) {
    theRecord = new FailedRecord(id, importJobId, channelId, channelName, recordNumber, timeStamp,
        originalRecord, recordErrors, transformedRecord, sourceFileName);
  }

  /**
   * Implement to return an enum identifier for the underlying database table for the implementing entity.
   *
   * @return a Tables enum value
   */
  @Override
  public Tables table() {
    if (usingView) {
      return Tables.RECORD_FAILURE_VIEW;
    } else {
      return Tables.RECORD_FAILURE;
    }
  }

  @Override
  public UUID getId() {
    return theRecord == null ? null : theRecord.id();
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

  /**
   * Use view record failure view instead of base table.
   */
  public RecordFailure usingView() {
    this.usingView = true;
    return this;
  }

  /**
   * Maps rows from RECORD_FAILURE_VIEW to RecordFailure object.
   */
  public RowMapper<Entity> fromRow() {
    return row -> new RecordFailure(
        row.getUUID(dbColumnName(ID)),
        row.getUUID(dbColumnName(IMPORT_JOB_ID)),
        row.getUUID(dbColumnName(VIEW_CHANNEL_ID)),
        row.getString(dbColumnName(VIEW_CHANNEL_NAME)),
        row.getString(dbColumnName(RECORD_NUMBER)),
        formatDateTime(row.getLocalDateTime(dbColumnName(TIME_STAMP))),
        row.getString(dbColumnName(ORIGINAL_RECORD)),
        row.getJsonArray(dbColumnName(RECORD_ERRORS)),
        row.getJsonObject(dbColumnName(TRANSFORMED_RECORD)),
        row.getString(dbColumnName(SOURCE_FILE_NAME)))
        .withMetadata(row);
  }

  @Override
  public String insertTemplate(String schema) {
    return "INSERT INTO " + schema + "." + Tables.RECORD_FAILURE
        + " ("
        + dbColumnName(ID) + ", "
        + dbColumnName(IMPORT_JOB_ID) + ", "
        + dbColumnName(RECORD_NUMBER) + ", "
        + dbColumnName(TIME_STAMP) + ", "
        + dbColumnName(RECORD_ERRORS) + ", "
        + dbColumnName(ORIGINAL_RECORD) + ", "
        + dbColumnName(TRANSFORMED_RECORD) + ", "
        + dbColumnName(SOURCE_FILE_NAME) + ", "
        + metadata.insertClauseColumns()
        + ")"
        + " VALUES ("
        + "#{" + dbColumnName(ID) + "}, "
        + "#{" + dbColumnName(IMPORT_JOB_ID) + "}, "
        + "#{" + dbColumnName(RECORD_NUMBER) + "}, "
        + "TO_TIMESTAMP(#{" + dbColumnName(TIME_STAMP) + "},'" + DATE_FORMAT + "'), "
        + "#{" + dbColumnName(RECORD_ERRORS) + "}, "
        + "#{" + dbColumnName(ORIGINAL_RECORD) + "}, "
        + "#{" + dbColumnName(TRANSFORMED_RECORD) + "}, "
        + "#{" + dbColumnName(SOURCE_FILE_NAME) + "}, "
        + metadata.insertClauseValueTemplates()
        + ")";
  }

  @Override
  public TupleMapper<Entity> toTemplateParameters() {
    return TupleMapper.mapper(
        entity -> {
          RecordFailure.FailedRecord rec = ((RecordFailure) entity).theRecord;
          Map<String, Object> parameters = new HashMap<>();
          parameters.put(dbColumnName(ID), rec.id);
          parameters.put(dbColumnName(IMPORT_JOB_ID), rec.importJobId);
          parameters.put(dbColumnName(RECORD_NUMBER), rec.recordNumber);
          parameters.put(dbColumnName(TIME_STAMP), rec.timeStamp);
          parameters.put(dbColumnName(ORIGINAL_RECORD), rec.originalRecord);
          parameters.put(dbColumnName(TRANSFORMED_RECORD), rec.transformedRecord);
          parameters.put(dbColumnName(RECORD_ERRORS), rec.recordErrors);
          parameters.put(dbColumnName(SOURCE_FILE_NAME), rec.sourceFileName);
          putMetadata(parameters);
          return parameters;
        });
  }

  @Override
  public PgCqlDefinition getQueryableFields() {
    PgCqlDefinition pgCqlDefinition = PgCqlDefinition.create();
    pgCqlDefinition.addField("cql.allRecords", new org.folio.tlib.postgres.cqlfield.PgCqlFieldAlwaysMatches());
    pgCqlDefinition.addField("recordNumber", new PgCqlFieldText().withExact().withLikeOps());
    pgCqlDefinition.addField("importJobId", new org.folio.tlib.postgres.cqlfield.PgCqlFieldUuid());
    pgCqlDefinition.addField("channelId", new PgCqlFieldUuid());
    pgCqlDefinition.addField(
        "channelName", new PgCqlFieldText().withExact().withLikeOps().withFullText());
    pgCqlDefinition.addField("timeStamp", new PgCqlFieldTimestamp());
    pgCqlDefinition.addField("sourceFileName", new PgCqlFieldText().withExact().withLikeOps().withFullText());
    return pgCqlDefinition;
  }

  /**
   * For building JSON collection response.
   *
   * @return the JSON property name for a collection of the entity
   */
  @Override
  public String jsonCollectionName() {
    return "failedRecords";
  }

  /**
   * For logging and response messages.
   *
   * @return label to display in messages.
   */
  @Override
  public String entityName() {
    return "RecordFailure";
  }

  /**
   * Implement to map from request body JSON to entity POJO.
   *
   * @param json incoming JSON body
   * @return Entity POJO
   */
  @Override
  public Entity fromJson(JsonObject json) {
    return new RecordFailure(
        getUuidOrGenerate(json.getString(jsonPropertyName(ID))),
        UUID.fromString(json.getString(jsonPropertyName(IMPORT_JOB_ID))),
        null,  // channelId, a column in view, thus read-only, ignore if in input JSON
        null, // channelName, a column in view, thus read-only, ignore if in input JSON
        json.getString(jsonPropertyName(RECORD_NUMBER)),
        json.getString(jsonPropertyName(TIME_STAMP)),
        json.getString(jsonPropertyName(ORIGINAL_RECORD)),
        json.getJsonArray(jsonPropertyName(RECORD_ERRORS)),
        json.getJsonObject(jsonPropertyName(TRANSFORMED_RECORD)),
        json.getString(jsonPropertyName(SOURCE_FILE_NAME))
    );
  }

  /**
   * Gets JSON representation.
   */
  public JsonObject asJson() {
    JsonObject json = new JsonObject();
    json.put(jsonPropertyName(ID), theRecord.id);
    json.put(jsonPropertyName(IMPORT_JOB_ID), theRecord.importJobId);
    json.put(jsonPropertyName(VIEW_CHANNEL_ID), theRecord.channelId);
    json.put(jsonPropertyName(VIEW_CHANNEL_NAME), theRecord.channelName);
    json.put(jsonPropertyName(RECORD_NUMBER), theRecord.recordNumber);
    json.put(jsonPropertyName(TIME_STAMP), theRecord.timeStamp);
    json.put(jsonPropertyName(SOURCE_FILE_NAME), theRecord.sourceFileName);
    json.put(jsonPropertyName(RECORD_ERRORS), theRecord.recordErrors);
    json.put(jsonPropertyName(ORIGINAL_RECORD), theRecord.originalRecord);
    json.put(jsonPropertyName(TRANSFORMED_RECORD), theRecord.transformedRecord);
    putMetadata(json);
    return json;
  }

  @Override
  public Future<Void> createDatabase(TenantPgPool pool) {
    return executeSqlStatements(pool,

        "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + table()
            + "("
            + dbColumnNameAndType(ID) + " PRIMARY KEY, "
            + dbColumnNameAndType(IMPORT_JOB_ID) + " NOT NULL CONSTRAINT record_failure_import_job_id_fkey "
            + "REFERENCES " + pool.getSchema() + "." + Tables.IMPORT_JOB
            + "(" + new ImportJob().dbColumnName(ID) + ") ON DELETE CASCADE, "
            + dbColumnNameAndType(RECORD_NUMBER) + ", "
            + dbColumnNameAndType(TIME_STAMP) + ", "
            + dbColumnNameAndType(SOURCE_FILE_NAME) + ", "
            + dbColumnNameAndType(RECORD_ERRORS) + " NOT NULL, "
            + dbColumnNameAndType(ORIGINAL_RECORD) + " NOT NULL, "
            + dbColumnNameAndType(TRANSFORMED_RECORD) + " NOT NULL, "
            + metadata.columnsDdl()
            + ")",
        "CREATE INDEX IF NOT EXISTS record_failure_import_job_id_idx "
            + " ON " + pool.getSchema() + "." + table() + "(" + dbColumnName(IMPORT_JOB_ID) + ")",
        "ALTER TABLE " + pool.getSchema() +  "." + table()
            + " DROP CONSTRAINT IF EXISTS record_failure_import_job_id_fkey",
        "ALTER TABLE " + pool.getSchema() +  "." + table()
            + " ADD CONSTRAINT record_failure_import_job_id_fkey FOREIGN KEY (" + dbColumnName(IMPORT_JOB_ID) + ")"
            + " REFERENCES " + pool.getSchema() + "." + Tables.IMPORT_JOB
            + " (" + new ImportJob().dbColumnName(ID) + ") "
            + " ON DELETE CASCADE"
    ).mapEmpty();
  }

  public record FailedRecord(UUID id, UUID importJobId, UUID channelId, String channelName, String recordNumber,
                             String timeStamp, String originalRecord, JsonArray recordErrors,
                             JsonObject transformedRecord,
                             String sourceFileName) {
  }
}
