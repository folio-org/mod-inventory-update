package org.folio.inventoryimport.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import org.folio.inventoryimport.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryimport.moduledata.database.Tables;
import org.folio.tlib.postgres.TenantPgPool;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Step extends Entity {

    public Step() {}

    public Step(UUID id, String name, boolean enabled, String description, String type, String inputFormat,
                String outputFormat, String script) {
        record = new StepRecord(
                id, name, enabled, description, type, inputFormat, outputFormat, script);
    }
    // Step record, the entity data.
    public record StepRecord(UUID id, String name, boolean enabled, String description, String type, String inputFormat,
                              String outputFormat, String script) {
    }
    public StepRecord record;

    // Static map of Entity Fields.
    private static final Map<String, Field> FIELDS = new HashMap<>();
    public static final String ID="ID", NAME="NAME", ENABLED="ENABLED", TYPE="TYPE", DESCRIPTION="DESCRIPTION", INPUT_FORMAT="INPUT_FORMAT",
            OUTPUT_FORMAT="OUTPUT_FORMAT", SCRIPT = "SCRIPT";
    static {
        FIELDS.put(ID,new Field("id", "id", PgColumn.Type.UUID, false, true, true));
        FIELDS.put(NAME,new Field("name", "name", PgColumn.Type.TEXT, false, true));
        FIELDS.put(ENABLED, new Field("enabled", "enabled", PgColumn.Type.BOOLEAN, true, true));
        FIELDS.put(DESCRIPTION, new Field("description", "description", PgColumn.Type.TEXT, true, true));
        FIELDS.put(TYPE, new Field("type", "type", PgColumn.Type.TEXT, true, true));
        FIELDS.put(INPUT_FORMAT, new Field("inputFormat", "input_format", PgColumn.Type.TEXT, true, true));
        FIELDS.put(OUTPUT_FORMAT, new Field("outputFormat", "output_format", PgColumn.Type.TEXT, true, true));
        FIELDS.put(SCRIPT, new Field("script", "script", PgColumn.Type.TEXT, true, false));
    }
    @Override
    public Map<String, Field> fields() {
        return FIELDS;
    }

    @Override
    public String jsonCollectionName() {
        return "steps";
    }

    @Override
    public String entityName() {
        return "Step";
    }

    public String getLineSeparatedXslt() {
        return record.script.replaceAll("\\r[\\n]?", System.lineSeparator());
    }

    @Override
    public Tables table() {
        return Tables.step;
    }

    /**
     * Creates record from JSON.
     * @param stepJson Step JSON
     * @return Data object
     */
    public Step fromJson(JsonObject stepJson) {
        return new Step(
                getUuidOrGenerate(stepJson.getString(jsonPropertyName(ID))),
                stepJson.getString(jsonPropertyName(NAME)),
                stepJson.getBoolean(jsonPropertyName(ENABLED)),
                stepJson.getString(jsonPropertyName(TYPE)),
                stepJson.getString(jsonPropertyName(DESCRIPTION)),
                stepJson.getString(jsonPropertyName(INPUT_FORMAT)),
                stepJson.getString(jsonPropertyName(OUTPUT_FORMAT)),
                stepJson.getString(jsonPropertyName(SCRIPT)));
    }

    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        json.put(jsonPropertyName(ID), record.id());
        json.put(jsonPropertyName(NAME), record.name());
        json.put(jsonPropertyName(TYPE), record.type());
        json.put(jsonPropertyName(ENABLED), record.enabled());
        json.put(jsonPropertyName(DESCRIPTION), record.description());
        json.put(jsonPropertyName(INPUT_FORMAT), record.inputFormat());
        json.put(jsonPropertyName(OUTPUT_FORMAT), record.outputFormat());
        json.put(jsonPropertyName(SCRIPT), record.script());
        return json;
    }

    /**
     * Maps from PG row to POJO
     * @return Step data object
     */
    @Override
    public RowMapper<Entity> getRowMapper() {
        return row -> new Step(
                row.getUUID(dbColumnName(ID)),
                row.getString(dbColumnName(NAME)),
                row.getBoolean(dbColumnName(ENABLED)),
                row.getString(dbColumnName(TYPE)),
                row.getString(dbColumnName(DESCRIPTION)),
                row.getString(dbColumnName(INPUT_FORMAT)),
                row.getString(dbColumnName(OUTPUT_FORMAT)),
                row.getString(dbColumnName(SCRIPT)));
    }

    /**
     * Maps from entity data object to PG columns
     * @return a mapper to be used by PG insert statement
     */
    @Override
    public TupleMapper<Entity> getTupleMapper() {
        return TupleMapper.mapper(
                entity -> {
                    StepRecord rec = ((Step) entity).record;
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(dbColumnName(ID), rec.id);
                    parameters.put(dbColumnName(NAME), rec.name);
                    parameters.put(dbColumnName(ENABLED), rec.enabled);
                    parameters.put(dbColumnName(TYPE), rec.type);
                    parameters.put(dbColumnName(DESCRIPTION), rec.description);
                    parameters.put(dbColumnName(INPUT_FORMAT), rec.inputFormat);
                    parameters.put(dbColumnName(OUTPUT_FORMAT), rec.outputFormat);
                    parameters.put(dbColumnName(SCRIPT), rec.script);
                    return parameters;
                });
    }

    public String validateScriptAsXml() {
        return validateScriptAsXml(getLineSeparatedXslt());
    }

    public static String validateScriptAsXml(String xslt) {
        try {
            DocumentBuilderFactory builder = DocumentBuilderFactory.newInstance();
            builder.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder parser = builder.newDocumentBuilder();
            parser.parse(new ByteArrayInputStream(xslt.replaceAll("\\r[\\n]?", System.lineSeparator()).getBytes(StandardCharsets.UTF_8)));
        } catch (ParserConfigurationException | IOException | SAXException pe) {
            return "Could not parse [ " + xslt + "] as XML: " + pe.getMessage();
        }
        return "OK";
    }

    private void setScript(String xslt) {
        record = new Step.StepRecord(record.id, record.name, record.enabled, record.description, record.type, record.inputFormat,
                record.outputFormat, xslt);
    }

    public Future<Void> updateScript(String xslt, ModuleStorageAccess storage) {
        setScript(xslt);
        return storage.updateEntity(this,
                "UPDATE " + storage.schema() + "." + table()
                        + " SET " + dbColumnName(SCRIPT)
                        + " = #{" + dbColumnName(SCRIPT) + "}".replaceAll(System.lineSeparator(), "\n")
                        + " WHERE id = #{id}")
                .mapEmpty();
    }

    @Override
    public Future<Void> createDatabase(TenantPgPool pool) {
        // table without indexes or foreign keys.
        return super.createDatabase(pool);
    }

}
