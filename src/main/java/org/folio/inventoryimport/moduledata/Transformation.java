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

public class Transformation extends Entity {

    public Transformation() {}

    public Transformation(UUID id, String name, boolean enabled, String description, String type) {
        record = new TransformationRecord(id, name, enabled, description, type);
    }

    public record TransformationRecord(UUID id, String name, boolean enabled, String description, String type) {}
    public TransformationRecord record;

    // Static map of Entity Fields.
    private static final Map<String, Field> FIELDS = new HashMap<>();
    public static final String ID = "ID", NAME = "NAME", TYPE = "TYPE", DESCRIPTION = "DESCRIPTION";
    static {
        FIELDS.put(ID,new Field("id", "id", PgColumn.Type.UUID, false, true, true));
        FIELDS.put(NAME,new Field("name", "name", PgColumn.Type.TEXT, false, true));
        FIELDS.put(DESCRIPTION, new Field("description", "description", PgColumn.Type.TEXT, true, true));
        FIELDS.put(TYPE, new Field("type", "type", PgColumn.Type.TEXT, true, true));
    }
    @Override
    public Map<String, Field> fields() {
        return FIELDS;
    }

    @Override
    public Tables table() {
        return Tables.transformation;
    }

    @Override
    public String jsonCollectionName() {
        return "transformations";
    }

    @Override
    public String entityName() {
        return "Transformation pipeline";
    }

    public Transformation fromJson(JsonObject json) {
        return new Transformation(
                getUuidOrGenerate(json.getString(jsonPropertyName(ID))),
                json.getString(jsonPropertyName(NAME)),
                true,
                json.getString(jsonPropertyName(TYPE)),
                json.getString(jsonPropertyName(DESCRIPTION)));
    }

    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        json.put(jsonPropertyName(ID), record.id);
        json.put(jsonPropertyName(NAME), record.name);
        json.put(jsonPropertyName(TYPE), record.type);
        json.put("enabled", record.enabled);
        json.put(jsonPropertyName(DESCRIPTION), record.description);
        return json;
    }

    @Override
    public RowMapper<Entity> getRowMapper() {
            return row -> new Transformation(
                    row.getUUID(dbColumnName(ID)),
                    row.getString(dbColumnName(NAME)),
                    true,
                    row.getString(dbColumnName(TYPE)),
                    row.getString(dbColumnName(DESCRIPTION)));
    }

    @Override
    public TupleMapper<Entity> getTupleMapper() {
        return TupleMapper.mapper(
                entity -> {
                    TransformationRecord rec = ((Transformation) entity).record;
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(dbColumnName(ID), rec.id);
                    parameters.put(dbColumnName(NAME), rec.name);
                    parameters.put(dbColumnName(TYPE), rec.type);
                    parameters.put(dbColumnName(DESCRIPTION), rec.description);
                    return parameters;
                });
    }

    @Override
    public Future<Void> createDatabase(TenantPgPool pool) {
        // table without indexes or foreign keys.
        return super.createDatabase(pool);
    }


}
