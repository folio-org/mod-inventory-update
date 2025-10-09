package org.folio.inventoryimport.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import org.folio.inventoryimport.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryimport.moduledata.database.Tables;
import org.folio.inventoryimport.utils.SettableClock;
import org.folio.tlib.postgres.TenantPgPool;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ImportJob extends Entity {

    private static final String DATE_FORMAT = "YYYY-MM-DD''T''HH24:MI:SS,MS";

    public enum JobStatus {
        RUNNING,
        DONE,
        PAUSED
    }

    public ImportJob() {}

    public ImportJob(UUID id,
                     UUID importConfigId,
                     String importConfigName,
                     String importType,
                     String url,
                     Boolean allowErrors,
                     Integer recordLimit,
                     Integer batchSize,
                     UUID transformation,
                     UUID storage,
                     JobStatus status,
                     String started,
                     String finished,
                     Integer amountHarvested,
                     String message) {
        record = new ImportJob.ImportJobRecord(
                id, importConfigId, importConfigName, importType, url, allowErrors, recordLimit, batchSize, transformation, storage, status, started, finished, amountHarvested, message);
    }

    public ImportJobRecord record;
    public record ImportJobRecord(UUID id,
                                   UUID importConfigId,
                                   String importConfigName,
                                   String importType,
                                   String url,
                                   Boolean allowErrors,
                                   Integer recordLimit,
                                   Integer batchSize,
                                   UUID transformation,
                                   UUID storage,
                                   JobStatus status,
                                   String started,
                                   String finished,
                                   Integer amountHarvested,
                                   String message) {
    }

    private static final Map<String, Field> FIELDS = new HashMap<>();
    public static final String ID = "ID", IMPORT_CONFIG_ID = "IMPORT_CONFIG_ID", IMPORT_CONFIG_NAME = "HARVESTABLE_NAME",
            IMPORT_TYPE = "IMPORT_TYPE", URL = "URL", ALLOW_ERRORS = "ALLOW_ERRORS", RECORD_LIMIT = "RECORD_LIMIT",
            BATCH_SIZE = "BATCH_SIZE", TRANSFORMATION = "TRANSFORMATION", STORAGE = "STORAGE", STATUS = "STATUS", STARTED = "STARTED",
            FINISHED = "FINISHED", AMOUNT_HARVESTED = "AMOUNT_HARVESTED", MESSAGE = "MESSAGE";

    static {
        FIELDS.put(ID, new Field("id", "id", PgColumn.Type.UUID, false, true, true));
        FIELDS.put(IMPORT_CONFIG_ID, new Field("importConfigId", "import_config_id", PgColumn.Type.UUID, false, true));
        FIELDS.put(IMPORT_CONFIG_NAME, new Field("importConfigName", "import_config_name", PgColumn.Type.TEXT, false, true));
        FIELDS.put(IMPORT_TYPE, new Field("importType", "import_type", PgColumn.Type.TEXT, false, true));
        FIELDS.put(URL, new Field("url", "url", PgColumn.Type.TEXT, false, false));
        FIELDS.put(ALLOW_ERRORS, new Field("allowErrors", "allow_errors", PgColumn.Type.BOOLEAN, false, false));
        FIELDS.put(RECORD_LIMIT, new Field("recordLimit", "record_limit", PgColumn.Type.INTEGER, true, false));
        FIELDS.put(BATCH_SIZE, new Field("batchSize", "batch_size", PgColumn.Type.INTEGER, true, false));
        FIELDS.put(TRANSFORMATION, new Field("transformation", "transformation_id", PgColumn.Type.UUID, false, true));
        FIELDS.put(STORAGE, new Field("storage", "storage_id", PgColumn.Type.UUID, false, true));
        FIELDS.put(STATUS, new Field("status", "status", PgColumn.Type.TEXT, true, true));
        FIELDS.put(STARTED, new Field("started", "started", PgColumn.Type.TIMESTAMP, false, true));
        FIELDS.put(FINISHED, new Field("finished", "finished", PgColumn.Type.TIMESTAMP, true, true));
        FIELDS.put(AMOUNT_HARVESTED, new Field("amountHarvested", "amount_harvested", PgColumn.Type.INTEGER, true, true));
        FIELDS.put(MESSAGE, new Field("message", "message", PgColumn.Type.TEXT, true, true));
    }

    /**
     * Implement to return an enum identifier for the underlying database table for the implementing entity.
     *
     * @return a Tables enum value
     */
    @Override
    public Tables table() {
        return Tables.import_job;
    }

    /**
     * Implement to provide a map of the {@link Field} fields of the implementing entity
     *
     * @return Map fields by field keys to be used for finding queryable fields or, if possible, for creating the database table and more.
     */
    @Override
    public Map<String, Field> fields() {
        return FIELDS;
    }

    public ImportJob initiate(ImportConfig importConfig) {
        ImportConfig.ImportConfigRecord cfg = importConfig.record;
        return new ImportJob(UUID.randomUUID(), cfg.id(), cfg.name(), cfg.type(), cfg.URL(),
                cfg.allowErrors(), cfg.recordLimit(), cfg.batchSize(),
                cfg.transformationId(), cfg.storageId(),
                JobStatus.RUNNING, SettableClock.getLocalDateTime().toString(), "", 0, "");
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
                UUID.fromString(json.getString(jsonPropertyName(IMPORT_CONFIG_ID))),
                json.getString(jsonPropertyName(IMPORT_CONFIG_NAME)),
                json.getString(jsonPropertyName(IMPORT_TYPE)),
                json.getString(jsonPropertyName(URL)),
                json.getBoolean(jsonPropertyName(ALLOW_ERRORS)),
                json.getInteger(jsonPropertyName(RECORD_LIMIT)),
                json.getInteger(jsonPropertyName(BATCH_SIZE)),
                Util.getUUID(json, jsonPropertyName(TRANSFORMATION), null),
                Util.getUUID(json, jsonPropertyName(STORAGE), null),
                JobStatus.valueOf(json.getString(jsonPropertyName(STATUS))),
                json.getString(jsonPropertyName(STARTED)),
                finished != null && started != null && started.compareTo(finished) < 0 ?
                        json.getString(jsonPropertyName(FINISHED)) : null,
                json.getInteger(jsonPropertyName(AMOUNT_HARVESTED)),
                json.getString(jsonPropertyName(MESSAGE)));
    }

    /**
     * Implement to map for entity POJO to response JSON
     *
     * @return json representation of the entity
     */
    @Override
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        json.put(jsonPropertyName(ID), record.id);
        json.put(jsonPropertyName(IMPORT_CONFIG_ID), record.importConfigId);
        json.put(jsonPropertyName(IMPORT_CONFIG_NAME), record.importConfigName);
        json.put(jsonPropertyName(IMPORT_TYPE), record.importType);
        json.put(jsonPropertyName(URL), record.url);
        json.put(jsonPropertyName(ALLOW_ERRORS), record.allowErrors);
        json.put(jsonPropertyName(RECORD_LIMIT), record.recordLimit);
        json.put(jsonPropertyName(BATCH_SIZE), record.batchSize);
        json.put(jsonPropertyName(TRANSFORMATION), record.transformation);
        json.put(jsonPropertyName(STORAGE), record.storage);
        json.put(jsonPropertyName(STATUS), record.status);
        json.put(jsonPropertyName(STARTED), record.started);
        json.put(jsonPropertyName(FINISHED), record.finished);
        json.put(jsonPropertyName(AMOUNT_HARVESTED), record.amountHarvested);
        json.put(jsonPropertyName(MESSAGE), record.message);
        return json;
    }

    /**
     * Creates vert.x row mapper that maps a database select result row onto data object(s).
     */
    @Override
    public RowMapper<Entity> getRowMapper() {
        return row -> new ImportJob(
                row.getUUID(dbColumnName(ID)),
                row.getUUID(dbColumnName(IMPORT_CONFIG_ID)),
                row.getString(dbColumnName(IMPORT_CONFIG_NAME)),
                row.getString(dbColumnName(IMPORT_TYPE)),
                row.getString(dbColumnName(URL)),
                row.getBoolean(dbColumnName(ALLOW_ERRORS)),
                (row.getValue(dbColumnName(RECORD_LIMIT)) != null ? row.getInteger(dbColumnName(RECORD_LIMIT)) : null),
                row.getInteger(dbColumnName(BATCH_SIZE)),
                row.getUUID(dbColumnName(TRANSFORMATION)),
                row.getUUID(dbColumnName(STORAGE)),
                JobStatus.valueOf(row.getString(dbColumnName(STATUS))),
                row.getLocalDateTime(dbColumnName(STARTED)).toString(),
                (row.getValue(dbColumnName(FINISHED)) != null ? row.getLocalDateTime(dbColumnName(FINISHED)).toString() : null),
                (row.getValue(dbColumnName(AMOUNT_HARVESTED)) != null ? row.getInteger(dbColumnName(AMOUNT_HARVESTED)) : null),
                row.getString(dbColumnName(MESSAGE)));
    }

    /**
     * Creates vert.x tuple mapper that maps Postgres column names to field values.
     */
    @Override
    public TupleMapper<Entity> getTupleMapper() {
        return TupleMapper.mapper(
                entity -> {
                    ImportJob.ImportJobRecord rec = ((ImportJob) entity).record;
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(dbColumnName(ID), rec.id);
                    parameters.put(dbColumnName(IMPORT_CONFIG_ID), rec.importConfigId);
                    parameters.put(dbColumnName(IMPORT_CONFIG_NAME), rec.importConfigName);
                    parameters.put(dbColumnName(IMPORT_TYPE), rec.importType);
                    parameters.put(dbColumnName(URL), rec.url());
                    parameters.put(dbColumnName(ALLOW_ERRORS), rec.allowErrors);
                    if (rec.recordLimit() != null) {
                        parameters.put(dbColumnName(RECORD_LIMIT), rec.recordLimit);
                    }
                    if (rec.batchSize() != null) {
                        parameters.put(dbColumnName(BATCH_SIZE), rec.batchSize);
                    }
                    parameters.put(dbColumnName(TRANSFORMATION), rec.transformation);
                    parameters.put(dbColumnName(STORAGE), rec.storage);
                    parameters.put(dbColumnName(STATUS), rec.status);
                    if (rec.started != null) {
                        parameters.put(dbColumnName(STARTED), rec.started);
                    }
                    if (rec.finished != null) {
                        parameters.put(dbColumnName(FINISHED), rec.finished);
                    }
                    if (rec.amountHarvested != null) {
                        parameters.put(
                                dbColumnName(AMOUNT_HARVESTED), rec.amountHarvested);
                    }
                    parameters.put(dbColumnName(MESSAGE), rec.message);
                    return parameters;
                });
    }

    /**
     * For building JSON collection response
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

    public String makeInsertTemplate(String schema) {
        return "INSERT INTO " + schema + "." + table()
                + " ("
                + dbColumnName(ID) + ", "
                + dbColumnName(IMPORT_CONFIG_ID) + ", "
                + dbColumnName(IMPORT_CONFIG_NAME) + ", "
                + dbColumnName(IMPORT_TYPE) + ", "
                + dbColumnName(URL) + ", "
                + dbColumnName(ALLOW_ERRORS) + ", "
                + dbColumnName(RECORD_LIMIT) + ", "
                + dbColumnName(BATCH_SIZE) + ", "
                + dbColumnName(TRANSFORMATION) + ", "
                + dbColumnName(STORAGE) + ", "
                + dbColumnName(STATUS) + ", "
                + dbColumnName(STARTED) + ", "
                //+ dbColumnName(FINISHED) + ", "
                + dbColumnName(AMOUNT_HARVESTED) + ", "
                + dbColumnName(MESSAGE)
                + ")"
                + " VALUES ("
                + "#{" + dbColumnName(ID) + "}, "
                + "#{" + dbColumnName(IMPORT_CONFIG_ID) + "}, "
                + "#{" + dbColumnName(IMPORT_CONFIG_NAME) + "}, "
                + "#{" + dbColumnName(IMPORT_TYPE) + "}, "
                + "#{" + dbColumnName(URL) + "}, "
                + "#{" + dbColumnName(ALLOW_ERRORS) + "}, "
                + "#{" + dbColumnName(RECORD_LIMIT) + "}, "
                + "#{" + dbColumnName(BATCH_SIZE) + "}, "
                + "#{" + dbColumnName(TRANSFORMATION) + "}, "
                + "#{" + dbColumnName(STORAGE) + "}, "
                + "#{" + dbColumnName(STATUS) + "}, "
                + "TO_TIMESTAMP(#{" + dbColumnName(STARTED) + "},'" + DATE_FORMAT + "'), "
                //+ "TO_TIMESTAMP(#{" + dbColumnName(FINISHED) + "}, '" + DATE_FORMAT + "'), "
                + "#{" + dbColumnName(AMOUNT_HARVESTED) + "}, "
                + "#{" + dbColumnName(MESSAGE) + "}"
                + ")";
    }

    public void logFinish(LocalDateTime finished, int recordCount, ModuleStorageAccess configStorage) {
        setFinished(finished, recordCount);
        configStorage.updateEntity(this,
                "UPDATE " + configStorage.schema() + "." + table()
                        + " SET "
                        + dbColumnName(FINISHED) + " = TO_TIMESTAMP(#{" + dbColumnName(FINISHED) + "}, '" + DATE_FORMAT + "') "
                        + ", "
                        + dbColumnName(STATUS) + " = #{" + dbColumnName(STATUS) + "} "
                        + ", "
                        + dbColumnName(AMOUNT_HARVESTED) + " = #{" + dbColumnName(AMOUNT_HARVESTED) + "}"
                        + " WHERE id = #{id}");
    }

    private void setFinished(LocalDateTime finished, int recordCount) {
        record = new ImportJobRecord(record.id, record.importConfigId, record.importConfigName, record.importType,
                record.url, record.allowErrors, record.recordLimit, record.batchSize, record.transformation, record.storage,
                JobStatus.DONE, record.started, finished.toString(), recordCount, record.message);
    }

    public void logStatus(JobStatus status, int recordCount, ModuleStorageAccess configStorage) {
        record = new ImportJobRecord(record.id, record.importConfigId, record.importConfigName, record.importType,
                record.url, record.allowErrors, record.recordLimit, record.batchSize, record.transformation, record.storage,
                status, record.started, record.finished, recordCount, record.message);
        configStorage.updateEntity(this,
                "UPDATE " + configStorage.schema() + "." + table()
                        + " SET "
                        + dbColumnName(STATUS) + " = #{" + dbColumnName(STATUS) + "} "
                        + ", "
                        + dbColumnName(AMOUNT_HARVESTED) + " = #{" + dbColumnName(AMOUNT_HARVESTED) + "}"
                        + " WHERE id = #{id}");
    }


    @Override
    public Future<Void> createDatabase(TenantPgPool pool) {
        return executeSqlStatements(pool,
                "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + table()
                + "("
                + dbColumnNameAndType(ID) + " PRIMARY KEY, "
                + dbColumnNameAndType(IMPORT_CONFIG_ID) + " NOT NULL "
                + " REFERENCES " + pool.getSchema() + "." + Tables.import_config + " (" + new ImportConfig().dbColumnName(ID) + "), "
                + dbColumnNameAndType(IMPORT_CONFIG_NAME)  + ", "
                + dbColumnNameAndType(IMPORT_TYPE) + ", "
                + dbColumnNameAndType(URL) + ", "
                + dbColumnNameAndType(ALLOW_ERRORS) + ", "
                + dbColumnNameAndType(RECORD_LIMIT) + ", "
                + dbColumnNameAndType(BATCH_SIZE) + ", "
                + dbColumnNameAndType(TRANSFORMATION) + ", "
                + dbColumnNameAndType(STORAGE) + ", "
                + dbColumnNameAndType(STATUS) + ", "
                + dbColumnNameAndType(STARTED) + " NOT NULL, "
                + dbColumnNameAndType(FINISHED) + ", "
                + dbColumnNameAndType(AMOUNT_HARVESTED) + ", "
                + dbColumnNameAndType(MESSAGE) + ")",

                "CREATE INDEX IF NOT EXISTS import_job_import_config_id_idx "
                        + " ON " + pool.getSchema() + "." + table() + "(" + dbColumnName(IMPORT_CONFIG_ID) + ")"
        ).mapEmpty();
    }

}
