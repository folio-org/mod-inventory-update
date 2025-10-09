package org.folio.inventoryimport.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import org.folio.inventoryimport.moduledata.database.SqlQuery;
import org.folio.inventoryimport.moduledata.database.Tables;
import org.folio.inventoryimport.service.ServiceRequest;
import org.folio.tlib.postgres.TenantPgPool;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LogLine extends Entity {
    public LogLine.LogLineRecord record;
    public record LogLineRecord(UUID id, UUID importJobId, String timeStamp, String jobLabel, String line) {}

    public LogLine() {}

    public LogLine(UUID id, UUID importJobId, String timeStamp, String jobLabel, String line) {
        record = new LogLineRecord(id, importJobId, timeStamp, jobLabel, line);
    }

    // Static map of Entity Fields.
    private static final Map<String, Field> FIELDS = new HashMap<>();
    public static final String ID="ID", IMPORT_JOB_ID="IMPORT_JOB_ID", TIME_STAMP="TIME_STAMP",
            JOB_LABEL="JOB_LABEL", LOG_STATEMENT="LOG_STATEMENT";
    static {
        FIELDS.put(ID, new Field("id", "id", PgColumn.Type.UUID, false, false, true));
        FIELDS.put(IMPORT_JOB_ID, new Field("importJobId", "import_job_id", PgColumn.Type.UUID, false, true));
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
        return Tables.log_statement;
    }

    @Override
    public RowMapper<Entity> getRowMapper() {
        return row -> new LogLine(
                row.getUUID(dbColumnName(ID)),
                row.getUUID(dbColumnName(IMPORT_JOB_ID)),
                row.getLocalDateTime(dbColumnName(TIME_STAMP)).toString(),
                row.getString(dbColumnName(JOB_LABEL)),
                row.getString(dbColumnName(LOG_STATEMENT)));
    }

    /**
     * INSERT INTO statement.
     */
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
                    LogLineRecord rec = ((LogLine) entity).record;
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(dbColumnName(ID), rec.id);
                    parameters.put(dbColumnName(IMPORT_JOB_ID), rec.importJobId);
                    parameters.put(dbColumnName(TIME_STAMP), rec.timeStamp);
                    parameters.put(dbColumnName(JOB_LABEL), rec.jobLabel);
                    parameters.put(dbColumnName(LOG_STATEMENT), rec.line);
                    return parameters;
                });
    }

    public SqlQuery makeSqlFromCqlQuery(ServiceRequest request, String schemaDotTable) {
        SqlQuery sql = super.makeSqlFromCqlQuery(request, schemaDotTable);
        sql.withAdditionalOrderByField(dbColumnName(TIME_STAMP));
        return sql;
    }

    public String toString() {
        return String.format("%s %s %s",record.timeStamp, record.jobLabel, record.line);
    }

    @Override
    public LogLine fromJson(JsonObject json) {
        return new LogLine(
                getUuidOrGenerate(json.getString(jsonPropertyName(ID))),
                UUID.fromString(json.getString(jsonPropertyName(IMPORT_JOB_ID))),
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
        json.put(jsonPropertyName(ID), record.id);
        json.put(jsonPropertyName(TIME_STAMP), record.timeStamp);
        json.put(jsonPropertyName(JOB_LABEL), record.jobLabel);
        json.put(jsonPropertyName(LOG_STATEMENT), record.line);
        return json;
    }

    @Override
    public Future<Void> createDatabase(TenantPgPool pool) {
        return executeSqlStatements(pool,

                "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + table()
                + "("
                + dbColumnName(ID) + " UUID PRIMARY KEY, "
                + dbColumnName(IMPORT_JOB_ID) + " UUID NOT NULL "
                + " REFERENCES " + pool.getSchema() + "." + Tables.import_job + " (" + new ImportJob().dbColumnName(ID) + "), "
                + dbColumnName(TIME_STAMP) + " TIMESTAMP NOT NULL, "
                + dbColumnName(JOB_LABEL) + " TEXT NOT NULL, "
                + dbColumnName(LOG_STATEMENT) + " TEXT NOT NULL"
                + ")",

                "CREATE INDEX IF NOT EXISTS log_statement_import_job_id_idx "
                                + " ON " + pool.getSchema() + "." + table() + "(" + dbColumnName(IMPORT_JOB_ID) + ")"
        ).mapEmpty();
    }

}
