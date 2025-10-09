package org.folio.inventoryimport.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import org.folio.inventoryimport.moduledata.database.Tables;
import org.folio.tlib.postgres.TenantPgPool;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ImportConfig extends Entity {

    public ImportConfig(){}

    public ImportConfig(UUID id, String name, String type, String URL, Boolean allowErrors,
                        Integer recordLimit, Integer batchSize, UUID transformationId, UUID storageId) {
        record = new ImportConfigRecord(id, name, type, URL, allowErrors, recordLimit, batchSize, transformationId, storageId);
    }

    // Import config record, the entity data.
    public record ImportConfigRecord(UUID id, String name, String type, String URL, Boolean allowErrors,
                                      Integer recordLimit, Integer batchSize, UUID transformationId, UUID storageId) {
    }
    public ImportConfigRecord record;

    // Static map of Entity Fields.
    private static final Map<String, Field> IMPORT_CONFIG_FIELDS = new HashMap<>();
    public static final String ID = "ID", NAME = "NAME", TYPE = "TYPE",
            URL = "URL", ALLOW_ERRORS = "ALLOW_ERRORS", RECORD_LIMIT = "RECORD_LIMIT", BATCH_SIZE = "BATCH_SIZE",
            TRANSFORMATION_ID = "TRANSFORMATION_ID", STORAGE_ID = "STORAGE_ID";

    static {
        IMPORT_CONFIG_FIELDS.put(ID, new Field("id", "id", PgColumn.Type.UUID, false, true, true));
        IMPORT_CONFIG_FIELDS.put(NAME, new Field("name", "name", PgColumn.Type.TEXT, false, true));
        IMPORT_CONFIG_FIELDS.put(TYPE, new Field("type", "type", PgColumn.Type.TEXT, false, true));
        IMPORT_CONFIG_FIELDS.put(URL, new Field("url", "url", PgColumn.Type.TEXT, false, false));
        IMPORT_CONFIG_FIELDS.put(ALLOW_ERRORS, new Field("allowErrors", "allow_errors", PgColumn.Type.BOOLEAN, false, false));
        IMPORT_CONFIG_FIELDS.put(RECORD_LIMIT, new Field("recordLimit", "record_limit", PgColumn.Type.INTEGER, true, false));
        IMPORT_CONFIG_FIELDS.put(BATCH_SIZE, new Field("batchSize", "batch_size", PgColumn.Type.INTEGER, true, false));
        IMPORT_CONFIG_FIELDS.put(TRANSFORMATION_ID, new Field("transformationId", "transformation_id", PgColumn.Type.UUID, false, true));
        IMPORT_CONFIG_FIELDS.put(STORAGE_ID, new Field("storageId", "storage_id", PgColumn.Type.UUID, true, true));
    }

    @Override
    public Map<String, Field> fields() {
        return IMPORT_CONFIG_FIELDS;
    }

    @Override
    public String jsonCollectionName() {
        return "importConfigs";
    }

    @Override
    public String entityName() {
        return "Import config";
    }

    public ImportConfig fromJson(JsonObject importConfigJson) {
        return new ImportConfig(
                getUuidOrGenerate(importConfigJson.getString(jsonPropertyName(ID))),
                importConfigJson.getString(jsonPropertyName(NAME)),
                importConfigJson.getString(jsonPropertyName(TYPE)),
                importConfigJson.getString(jsonPropertyName(URL)),
                Boolean.valueOf(importConfigJson.getString(jsonPropertyName(ALLOW_ERRORS))),
                importConfigJson.getInteger(jsonPropertyName(RECORD_LIMIT)),
                importConfigJson.getInteger(jsonPropertyName(BATCH_SIZE)),
                Util.getUUID(importConfigJson, jsonPropertyName(TRANSFORMATION_ID), null),
                Util.getUUID(importConfigJson, jsonPropertyName(STORAGE_ID), null));
    }

    @Override
    public RowMapper<Entity> getRowMapper() {
        return row -> new ImportConfig(
                row.getUUID(dbColumnName(ID)),
                row.getString(dbColumnName(NAME)),
                row.getString(dbColumnName(TYPE)),
                row.getString(dbColumnName(URL)),
                row.getBoolean(dbColumnName(ALLOW_ERRORS)),
                row.getInteger(dbColumnName(RECORD_LIMIT)),
                row.getInteger(dbColumnName(BATCH_SIZE)),
                row.getUUID(dbColumnName(TRANSFORMATION_ID)),
                row.getUUID(dbColumnName(STORAGE_ID)));
    }

    @Override
    public TupleMapper<Entity> getTupleMapper() {
        return TupleMapper.mapper(
                entity -> {
                    ImportConfigRecord rec = ((ImportConfig) entity).record;
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(dbColumnName(ID), rec.id);
                    parameters.put(dbColumnName(NAME), rec.name);
                    parameters.put(dbColumnName(TYPE), rec.type);
                    parameters.put(dbColumnName(URL), rec.URL);
                    parameters.put(dbColumnName(ALLOW_ERRORS), rec.allowErrors());
                    if (rec.recordLimit() != null) {
                        parameters.put(dbColumnName(RECORD_LIMIT), rec.recordLimit());
                    }
                    if (rec.batchSize() != null) {
                        parameters.put(dbColumnName(BATCH_SIZE), rec.batchSize());
                    }
                    parameters.put(dbColumnName(TRANSFORMATION_ID), rec.transformationId());
                    parameters.put(dbColumnName(STORAGE_ID), rec.storageId());
                    return parameters;
                });
    }

    /**
     * ImportConfig to JSON mapping.
     */
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        json.put(jsonPropertyName(ID), record.id());
        json.put(jsonPropertyName(NAME), record.name());
        json.put(jsonPropertyName(TYPE), record.type());
        json.put(jsonPropertyName(URL), record.URL());
        json.put(jsonPropertyName(ALLOW_ERRORS), record.allowErrors());
        json.put(jsonPropertyName(RECORD_LIMIT), record.recordLimit());
        json.put(jsonPropertyName(BATCH_SIZE), record.batchSize());
        json.put(jsonPropertyName(TRANSFORMATION_ID), record.transformationId());
        if (record.storageId() != null) {
            json.put(jsonPropertyName(STORAGE_ID), record.storageId().toString());
        }
        return json;
    }

    @Override
    public Tables table() {
        return Tables.import_config;
    }

    @Override
    public Future<Void> createDatabase(TenantPgPool pool) {
        return executeSqlStatements(pool,
                "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + table()
                + "("
                + field(ID).pgColumnDdl() + ", "
                + field(NAME).pgColumnDdl() + ", "
                + field(TYPE).pgColumnDdl() + ", "
                + field(URL).pgColumnDdl() + ", "
                + field(ALLOW_ERRORS).pgColumnDdl() + ", "
                + field(RECORD_LIMIT).pgColumnDdl() + ", "
                + field(BATCH_SIZE).pgColumnDdl() + ", "
                + field(TRANSFORMATION_ID).pgColumnDdl()
                + " REFERENCES " + pool.getSchema() + "." + Tables.transformation
                        + " (" + new Transformation().dbColumnName(Transformation.ID) + "), "
                + field(STORAGE_ID).pgColumnDdl()
                + ")"
        ).mapEmpty();
    }

}
