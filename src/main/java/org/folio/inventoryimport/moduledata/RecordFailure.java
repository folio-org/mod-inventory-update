package org.folio.inventoryimport.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import org.folio.inventoryimport.moduledata.database.Tables;
import org.folio.tlib.postgres.PgCqlDefinition;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.postgres.cqlfield.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RecordFailure extends Entity {

    public RecordFailure() {}

    public RecordFailure(UUID id, UUID importJobId, UUID importConfigId, String importConfigName, String recordNumber,
                         String timeStamp, String originalRecord, JsonArray recordErrors, JsonObject transformedRecord) {
        record = new FailedRecord(id, importJobId, importConfigId, importConfigName, recordNumber, timeStamp, originalRecord, recordErrors, transformedRecord);
    }
    public FailedRecord record;
    public record FailedRecord(UUID id, UUID importJobId, UUID importConfigId, String importConfigName, String recordNumber,
                               String timeStamp, String originalRecord, JsonArray recordErrors, JsonObject transformedRecord) {}

    private static final Map<String, Field> FIELDS = new HashMap<>();

    public static final String ID = "ID", IMPORT_JOB_ID = "IMPORT_JOB_ID", VIEW_IMPORT_CONFIG_ID = "IMPORT_CONFIG_ID", VIEW_IMPORT_CONFIG_NAME = "IMPORT_CONFIG_NAME",
    RECORD_NUMBER = "RECORD_NUMBER", TIME_STAMP = "TIME_STAMP", ORIGINAL_RECORD = "ORIGINAL_RECORD", RECORD_ERRORS = "RECORD_ERRORS",
    TRANSFORMED_RECORD = "TRANSFORMED_RECORD";
    static {
        FIELDS.put(ID, new Field("id", "id", PgColumn.Type.UUID, false, true, true));
        FIELDS.put(IMPORT_JOB_ID, new Field("importJobId", "import_job_id", PgColumn.Type.UUID, false, true));
        FIELDS.put(VIEW_IMPORT_CONFIG_ID, new Field("importConfigId", "import_config_id", PgColumn.Type.UUID, true, true));
        FIELDS.put(VIEW_IMPORT_CONFIG_NAME, new Field("importConfigName", "import_config_name", PgColumn.Type.TEXT, true, true));
        FIELDS.put(RECORD_NUMBER, new Field("recordNumber", "record_number", PgColumn.Type.TEXT, true, true));
        FIELDS.put(TIME_STAMP, new Field("timeStamp", "time_stamp", PgColumn.Type.TIMESTAMP, false, true));
        FIELDS.put(ORIGINAL_RECORD, new Field("originalRecord", "original_record", PgColumn.Type.TEXT, false, false));
        FIELDS.put(RECORD_ERRORS, new Field("recordErrors", "record_errors", PgColumn.Type.JSONB, false, false));
        FIELDS.put(TRANSFORMED_RECORD, new Field("transformedRecord", "transformed_record", PgColumn.Type.JSONB, false, false));
    }

    /**
     * Implement to return an enum identifier for the underlying database table for the implementing entity.
     *
     * @return a Tables enum value
     */
    @Override
    public Tables table() {
        return Tables.record_failure;
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


    private static final String DATE_FORMAT = "YYYY-MM-DD''T''HH24:MI:SS,MS";

    static Map<String,String> months = Stream.of(new String[][] {
            {"Jan", "01"},
            {"Feb", "02"},
            {"Mar", "03"},
            {"Apr", "04"},
            {"May", "05"},
            {"Jun", "06"},
            {"Jul", "07"},
            {"Aug", "08"},
            {"Sep", "09"},
            {"Oct", "10"},
            {"Nov", "11"},
            {"Dec", "12"}}).collect(Collectors.toMap(month -> month[0], month -> month[1]));

    /**
     * Maps rows from RECORD_FAILURE_VIEW to RecordFailure object.
     */
    public RowMapper<Entity> getRowMapper() {
        return row -> new RecordFailure(
                row.getUUID(dbColumnName(ID)),
                row.getUUID(dbColumnName(IMPORT_JOB_ID)),
                row.getUUID(dbColumnName(VIEW_IMPORT_CONFIG_ID)),
                row.getString(dbColumnName(VIEW_IMPORT_CONFIG_NAME)),
                row.getString(dbColumnName(RECORD_NUMBER)),
                row.getLocalDateTime(dbColumnName(TIME_STAMP)).toString(),
                row.getString(dbColumnName(ORIGINAL_RECORD)),
                row.getJsonArray(dbColumnName(RECORD_ERRORS)),
                row.getJsonObject(dbColumnName(TRANSFORMED_RECORD)));
    }

    @Override
    public String makeInsertTemplate(String schema) {
        return "INSERT INTO " + schema + "." + Tables.record_failure
                + " ("
                + dbColumnName(ID) + ", "
                + dbColumnName(IMPORT_JOB_ID) + ", "
                + dbColumnName(RECORD_NUMBER) + ", "
                + dbColumnName(TIME_STAMP) + ", "
                + dbColumnName(RECORD_ERRORS) + ", "
                + dbColumnName(ORIGINAL_RECORD) + ", "
                + dbColumnName(TRANSFORMED_RECORD)
                + ")"
                + " VALUES ("
                + "#{" + dbColumnName(ID) + "}, "
                + "#{" + dbColumnName(IMPORT_JOB_ID) + "}, "
                + "#{" + dbColumnName(RECORD_NUMBER) + "}, "
                + "TO_TIMESTAMP(#{" + dbColumnName(TIME_STAMP) + "},'" + DATE_FORMAT + "'), "
                + "#{" + dbColumnName(RECORD_ERRORS) + "}, "
                + "#{" + dbColumnName(ORIGINAL_RECORD) + "}, "
                + "#{" + dbColumnName(TRANSFORMED_RECORD) + "}"
                + ")";
    }

    @Override
    public TupleMapper<Entity> getTupleMapper() {
        return TupleMapper.mapper(
                entity -> {
                    RecordFailure.FailedRecord rec = ((RecordFailure) entity).record;
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(dbColumnName(ID), rec.id);
                    parameters.put(dbColumnName(IMPORT_JOB_ID), rec.importJobId);
                    parameters.put(dbColumnName(RECORD_NUMBER), rec.recordNumber);
                    parameters.put(dbColumnName(TIME_STAMP), rec.timeStamp);
                    parameters.put(dbColumnName(ORIGINAL_RECORD), rec.originalRecord);
                    parameters.put(dbColumnName(TRANSFORMED_RECORD), rec.transformedRecord);
                    parameters.put(dbColumnName(RECORD_ERRORS), rec.recordErrors);
                    return parameters;
                });
    }

    @Override
    public PgCqlDefinition getQueryableFields() {
        PgCqlDefinition pgCqlDefinition = PgCqlDefinition.create();
        pgCqlDefinition.addField("cql.allRecords", new PgCqlFieldAlwaysMatches());
        pgCqlDefinition.addField("recordNumber", new PgCqlFieldText().withExact().withLikeOps());
        pgCqlDefinition.addField("importConfigId", new PgCqlFieldUuid());
        pgCqlDefinition.addField(
                "importConfigName", new PgCqlFieldText().withExact().withLikeOps().withFullText());
        pgCqlDefinition.addField("timeStamp", new PgCqlFieldTimestamp());
        return pgCqlDefinition;
    }

    /**
     * For building JSON collection response
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
                null,  // importConfigId a column in view, thus read-only, ignore if in input JSON
                null, // importConfigName a column in view, thus read-only, ignore if in input JSON
                json.getString(jsonPropertyName(RECORD_NUMBER)),
                json.getString(jsonPropertyName(TIME_STAMP)),
                json.getString(jsonPropertyName(ORIGINAL_RECORD)),
                json.getJsonArray(jsonPropertyName(RECORD_ERRORS)),
                json.getJsonObject(jsonPropertyName(TRANSFORMED_RECORD))
        );
    }

    /**
     * Gets JSON representation.
     */
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        json.put(jsonPropertyName(ID), record.id);
        json.put(jsonPropertyName(IMPORT_JOB_ID), record.importJobId);
        json.put(jsonPropertyName(VIEW_IMPORT_CONFIG_ID), record.importConfigId);
        json.put(jsonPropertyName(VIEW_IMPORT_CONFIG_NAME), record.importConfigName);
        json.put(jsonPropertyName(RECORD_NUMBER), record.recordNumber);
        json.put(jsonPropertyName(TIME_STAMP), record.timeStamp);
        json.put(jsonPropertyName(RECORD_ERRORS), record.recordErrors);
        json.put(jsonPropertyName(ORIGINAL_RECORD), record.originalRecord);
        json.put(jsonPropertyName(TRANSFORMED_RECORD), record.transformedRecord);
        return json;
    }

    @Override
    public Future<Void> createDatabase(TenantPgPool pool) {
        return executeSqlStatements(pool,

                "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + table()
                + "("
                + dbColumnNameAndType(ID) + " PRIMARY KEY, "
                + dbColumnNameAndType(IMPORT_JOB_ID) + " NOT NULL REFERENCES "
                + pool.getSchema() + "." + Tables.import_job + "(" + new ImportJob().dbColumnName(ID) + "), "
                + dbColumnNameAndType(RECORD_NUMBER) + ", "
                + dbColumnNameAndType(TIME_STAMP) + ", "
                + dbColumnNameAndType(RECORD_ERRORS) + " NOT NULL, "
                + dbColumnNameAndType(ORIGINAL_RECORD) + " NOT NULL, "
                + dbColumnNameAndType(TRANSFORMED_RECORD) + " NOT NULL"
                + ")",

                "CREATE INDEX IF NOT EXISTS record_failure_import_job_id_idx "
                        + " ON " + pool.getSchema() + "." + table() + "(" + dbColumnName(IMPORT_JOB_ID) + ")"
        ).mapEmpty();

    }


}
