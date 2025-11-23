package org.folio.inventoryupdate.importing.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.tlib.postgres.TenantPgPool;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ImportConfig extends Entity {

    public ImportConfig(){}

    public ImportConfig(UUID id, String name, String type, UUID transformationId) {
        theRecord = new ImportConfigRecord(id, name, type, transformationId);
    }

    // Import config record, the entity data.
    public record ImportConfigRecord(UUID id, String name, String type, UUID transformationId) {
    }
    ImportConfigRecord theRecord;

    public ImportConfigRecord getRecord() {
      return theRecord;
    }

    // Static map of Entity Fields.
    private static final Map<String, Field> IMPORT_CONFIG_FIELDS = new HashMap<>();
    public static final String ID = "ID";
    public static final String NAME = "NAME";
    public static final String TYPE = "TYPE";
    public static final String TRANSFORMATION_ID = "TRANSFORMATION_ID";

    static {
        IMPORT_CONFIG_FIELDS.put(ID, new Field("id", "id", PgColumn.Type.UUID, false, true, true));
        IMPORT_CONFIG_FIELDS.put(NAME, new Field("name", "name", PgColumn.Type.TEXT, false, true));
        IMPORT_CONFIG_FIELDS.put(TYPE, new Field("type", "type", PgColumn.Type.TEXT, false, true));
        IMPORT_CONFIG_FIELDS.put(TRANSFORMATION_ID, new Field("transformationId", "transformation_id", PgColumn.Type.UUID, false, true));
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
                Util.getUUID(importConfigJson, jsonPropertyName(TRANSFORMATION_ID), null));
    }

    @Override
    public RowMapper<Entity> fromRow() {
        return row -> new ImportConfig(
            row.getUUID(dbColumnName(ID)),
            row.getString(dbColumnName(NAME)),
            row.getString(dbColumnName(TYPE)),
            row.getUUID(dbColumnName(TRANSFORMATION_ID)))
            .withMetadata(row);
    }

    @Override
    public TupleMapper<Entity> toTemplateParameters() {
        return TupleMapper.mapper(
                entity -> {
                    ImportConfigRecord rec = ((ImportConfig) entity).theRecord;
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(dbColumnName(ID), rec.id);
                    parameters.put(dbColumnName(NAME), rec.name);
                    parameters.put(dbColumnName(TYPE), rec.type);
                    parameters.put(dbColumnName(TRANSFORMATION_ID), rec.transformationId());
                    putMetadata(parameters);
                    return parameters;
                });
    }

    /**
     * ImportConfig to JSON mapping.
     */
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        json.put(jsonPropertyName(ID), theRecord.id());
        json.put(jsonPropertyName(NAME), theRecord.name());
        json.put(jsonPropertyName(TYPE), theRecord.type());
        json.put(jsonPropertyName(TRANSFORMATION_ID), theRecord.transformationId());
        putMetadata(json);
        return json;
    }

    @Override
    public Tables table() {
        return Tables.IMPORT_CONFIG;
    }

    @Override
    public Future<Void> createDatabase(TenantPgPool pool) {
        return executeSqlStatements(pool,
                "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + table()
                + "("
                + field(ID).pgColumnDdl() + ", "
                + field(NAME).pgColumnDdl() + ", "
                + field(TYPE).pgColumnDdl() + ", "
                + field(TRANSFORMATION_ID).pgColumnDdl()
                + " REFERENCES " + pool.getSchema() + "." + Tables.TRANSFORMATION
                        + " (" + new Transformation().dbColumnName(Transformation.ID) + "), "
                + metadata.columnsDdl()
                + ")"
        ).mapEmpty();
    }

}
