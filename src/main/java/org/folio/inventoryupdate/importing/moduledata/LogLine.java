package org.folio.inventoryupdate.importing.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import org.folio.inventoryupdate.importing.moduledata.database.SqlQuery;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.tlib.postgres.TenantPgPool;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LogLine extends Entity {
    LogLineRecord theRecord;
    public record LogLineRecord(UUID id, UUID importJobId, UUID importConfigId, String importConfigName, String timeStamp, String jobLabel, String line) {}

    public LogLine() {}

    public LogLine(UUID id, UUID importJobId, UUID importConfigId, String importConfigName, String timeStamp, String jobLabel, String line) {
        theRecord = new LogLineRecord(id, importJobId, importConfigId, importConfigName, timeStamp, jobLabel, line);
    }

    // Static map of Entity Fields.
    private static final Map<String, Field> FIELDS = new HashMap<>();
    public static final String ID = "ID";
    public static final String IMPORT_JOB_ID="IMPORT_JOB_ID";
    public static final String VIEW_IMPORT_CONFIG_ID = "IMPORT_CONFIG_ID";
    public static final String VIEW_IMPORT_CONFIG_NAME = "IMPORT_CONFIG_NAME";
    public static final String TIME_STAMP="TIME_STAMP";
    public static final String JOB_LABEL="JOB_LABEL";
    public static final String LOG_STATEMENT="LOG_STATEMENT";

    static {
        FIELDS.put(ID, new Field("id", "id", PgColumn.Type.UUID, false, false, true));
        FIELDS.put(IMPORT_JOB_ID, new Field("importJobId", "import_job_id", PgColumn.Type.UUID, false, true));
        FIELDS.put(VIEW_IMPORT_CONFIG_ID, new Field("importConfigId", "import_config_id", PgColumn.Type.UUID, true, true));
        FIELDS.put(VIEW_IMPORT_CONFIG_NAME, new Field("importConfigName", "import_config_name", PgColumn.Type.TEXT, true, true));
        FIELDS.put(TIME_STAMP, new Field("timeStamp", "time_stamp", PgColumn.Type.TIMESTAMP, false, false));
        FIELDS.put(JOB_LABEL, new Field("jobLabel", "job_label", PgColumn.Type.TEXT, false, true));
        FIELDS.put(LOG_STATEMENT, new Field("line", "statement", PgColumn.Type.TEXT, false, true));
    }
    @Override
    public Map<String, Field> fields() {
        return FIELDS;
    }

    @Override
    public String jsonCollectionName() {
        return "logLines";
    }

    @Override
    public String entityName() {
        return "Log line";
    }

    private static final String DATE_FORMAT = "YYYY-MM-DD''T''HH24:MI:SS,MS";

    @Override
    public Tables table() {
        return Tables.LOG_STATEMENT;
    }

    @Override
    public RowMapper<Entity> getRowMapper() {
        return row -> new LogLine(
                row.getUUID(dbColumnName(ID)),
                row.getUUID(dbColumnName(IMPORT_JOB_ID)),
                row.getUUID(dbColumnName(VIEW_IMPORT_CONFIG_ID)),
                row.getString(dbColumnName(VIEW_IMPORT_CONFIG_NAME)),
                row.getLocalDateTime(dbColumnName(TIME_STAMP)).toString(),
                row.getString(dbColumnName(JOB_LABEL)),
                row.getString(dbColumnName(LOG_STATEMENT)));
    }

    /**
     * INSERT INTO statement.
     */
    @Override
    public String makeInsertTemplate(String schema) {
        return "INSERT INTO " + schema + "." + table()
                + " ("
                + dbColumnName(ID) + ", "
                + dbColumnName(IMPORT_JOB_ID) + ", "
                + dbColumnName(TIME_STAMP) + ", "
                + dbColumnName(JOB_LABEL) + ", "
                + dbColumnName(LOG_STATEMENT)
                + ")"
                + " VALUES ("
                + "#{" + dbColumnName(ID) + "}, "
                + "#{" + dbColumnName(IMPORT_JOB_ID) + "}, "
                + "TO_TIMESTAMP(#{" + dbColumnName(TIME_STAMP) + "},'" + DATE_FORMAT + "'), "
                + "#{" + dbColumnName(JOB_LABEL) + "}, "
                + "#{" + dbColumnName(LOG_STATEMENT) + "}"
                + ")";
    }

    /**
     * Creates a TupleMapper for input mapping.
     */
    public TupleMapper<Entity> getTupleMapper() {
        return TupleMapper.mapper(
                entity -> {
                    LogLineRecord rec = ((LogLine) entity).theRecord;
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(dbColumnName(ID), rec.id);
                    parameters.put(dbColumnName(IMPORT_JOB_ID), rec.importJobId);
                    parameters.put(dbColumnName(TIME_STAMP), rec.timeStamp);
                    parameters.put(dbColumnName(JOB_LABEL), rec.jobLabel);
                    parameters.put(dbColumnName(LOG_STATEMENT), rec.line);
                    return parameters;
                });
    }

    @Override
    public SqlQuery makeSqlFromCqlQuery(ServiceRequest request, String schemaDotTable) {
        SqlQuery sql = super.makeSqlFromCqlQuery(request, schemaDotTable);
        sql.withAdditionalOrderByField(dbColumnName(TIME_STAMP));
        return sql;
    }

    public String toString() {
        return String.format("%s %s %s", theRecord.timeStamp, theRecord.jobLabel, theRecord.line);
    }

    @Override
    public LogLine fromJson(JsonObject json) {
        return new LogLine(
                getUuidOrGenerate(json.getString(jsonPropertyName(ID))),
                UUID.fromString(json.getString(jsonPropertyName(IMPORT_JOB_ID))),
                null,  // importConfigId a column in view, thus read-only, ignore if in input JSON
                null, // importConfigName a column in view, thus read-only, ignore if in input JSON
                json.getString(jsonPropertyName(TIME_STAMP)),
                json.getString(jsonPropertyName(JOB_LABEL)),
                json.getString(jsonPropertyName(LOG_STATEMENT))
        );
    }

    /**
     * Get log line as JSON.
     */
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        json.put(jsonPropertyName(ID), theRecord.id);
        json.put(jsonPropertyName(IMPORT_JOB_ID), theRecord.importJobId);
        json.put(jsonPropertyName(VIEW_IMPORT_CONFIG_ID), theRecord.importConfigId);
        json.put(jsonPropertyName(VIEW_IMPORT_CONFIG_NAME), theRecord.importConfigName);
        json.put(jsonPropertyName(TIME_STAMP), theRecord.timeStamp);
        json.put(jsonPropertyName(JOB_LABEL), theRecord.jobLabel);
        json.put(jsonPropertyName(LOG_STATEMENT), theRecord.line);
        return json;
    }

    @Override
    public Future<Void> createDatabase(TenantPgPool pool) {
        return executeSqlStatements(pool,

                "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + table()
                + "("
                + dbColumnName(ID) + " UUID PRIMARY KEY, "
                + dbColumnName(IMPORT_JOB_ID) + " UUID NOT NULL "
                + " REFERENCES " + pool.getSchema() + "." + Tables.IMPORT_JOB + " (" + new ImportJob().dbColumnName(ID) + "), "
                + dbColumnName(TIME_STAMP) + " TIMESTAMP NOT NULL, "
                + dbColumnName(JOB_LABEL) + " TEXT NOT NULL, "
                + dbColumnName(LOG_STATEMENT) + " TEXT NOT NULL"
                + ")",

                "CREATE INDEX IF NOT EXISTS log_statement_import_job_id_idx "
                                + " ON " + pool.getSchema() + "." + table() + "(" + dbColumnName(IMPORT_JOB_ID) + ")"
        ).mapEmpty();
    }

}
