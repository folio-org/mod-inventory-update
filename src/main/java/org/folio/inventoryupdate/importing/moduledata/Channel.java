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

public class Channel extends Entity {

    public Channel(){}

    public Channel(UUID id, String name, String type, UUID transformationId, boolean enabled) {
        theRecord = new ChannelRecord(id, name, type, transformationId, enabled);
    }

    // Import config record, the entity data.
    public record ChannelRecord(UUID id, String name, String type, UUID transformationId, boolean enabled) {
    }
    ChannelRecord theRecord;

    public ChannelRecord getRecord() {
      return theRecord;
    }

    // Static map of Entity Fields.
    private static final Map<String, Field> CHANNEL_FIELDS = new HashMap<>();
    public static final String ID = "ID";
    public static final String NAME = "NAME";
    public static final String TYPE = "TYPE";
    public static final String TRANSFORMATION_ID = "TRANSFORMATION_ID";
    public static final String ENABLED = "ENABLED";

    static {
        CHANNEL_FIELDS.put(ID, new Field("id", "id", PgColumn.Type.UUID, false, true, true));
        CHANNEL_FIELDS.put(NAME, new Field("name", "name", PgColumn.Type.TEXT, false, true));
        CHANNEL_FIELDS.put(TYPE, new Field("type", "type", PgColumn.Type.TEXT, false, true));
        CHANNEL_FIELDS.put(TRANSFORMATION_ID, new Field("transformationId", "transformation_id", PgColumn.Type.UUID, false, true));
        CHANNEL_FIELDS.put(ENABLED, new Field("enabled", "enabled", PgColumn.Type.BOOLEAN, false, true));
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

    public Channel fromJson(JsonObject channelJson) {
        return new Channel(
                getUuidOrGenerate(channelJson.getString(jsonPropertyName(ID))),
                channelJson.getString(jsonPropertyName(NAME)),
                channelJson.getString(jsonPropertyName(TYPE)),
                Util.getUUID(channelJson, jsonPropertyName(TRANSFORMATION_ID), null),
                "TRUE".equalsIgnoreCase(channelJson.getString(jsonPropertyName(ENABLED))));
    }

    @Override
    public RowMapper<Entity> fromRow() {
        return row -> new Channel(
            row.getUUID(dbColumnName(ID)),
            row.getString(dbColumnName(NAME)),
            row.getString(dbColumnName(TYPE)),
            row.getUUID(dbColumnName(TRANSFORMATION_ID)),
            row.getBoolean(dbColumnName(ENABLED)))
            .withMetadata(row);
    }

    @Override
    public TupleMapper<Entity> toTemplateParameters() {
        return TupleMapper.mapper(
                entity -> {
                    ChannelRecord rec = ((Channel) entity).theRecord;
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(dbColumnName(ID), rec.id);
                    parameters.put(dbColumnName(NAME), rec.name);
                    parameters.put(dbColumnName(TYPE), rec.type);
                    parameters.put(dbColumnName(TRANSFORMATION_ID), rec.transformationId());
                    parameters.put(dbColumnName(ENABLED), rec.enabled);
                    putMetadata(parameters);
                    return parameters;
                });
    }

    /**
     * Channel pojo to JSON mapping.
     */
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        json.put(jsonPropertyName(ID), theRecord.id());
        json.put(jsonPropertyName(NAME), theRecord.name());
        json.put(jsonPropertyName(TYPE), theRecord.type());
        json.put(jsonPropertyName(TRANSFORMATION_ID), theRecord.transformationId());
        json.put(jsonPropertyName(ENABLED), theRecord.enabled());
        putMetadata(json);
        return json;
    }

    @Override
    public Tables table() {
        return Tables.CHANNEL;
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
                + field(ENABLED).pgColumnDdl() + ", "
                + metadata.columnsDdl()
                + ")"
        ).mapEmpty();
    }

}
