package org.folio.inventoryupdate.importing.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.moduledata.database.ModuleStorageAccess;
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
import java.util.regex.Pattern;

public class Step extends Entity {

    public Step() {}

    public Step(UUID id, String name, boolean enabled, String description, String type, String inputFormat,
                String outputFormat, String script) {
        theRecord = new StepRecord(
                id, name, enabled, description, type, inputFormat, outputFormat, script);
    }
    // Step record, the entity data.
    public record StepRecord(UUID id, String name, boolean enabled, String description, String type, String inputFormat,
                              String outputFormat, String script) {
    }
    StepRecord theRecord;

    // Static map of Entity Fields.
    private static final Map<String, Field> FIELDS = new HashMap<>();
    public static final String ID="ID";
    public static final String NAME="NAME";
    public static final String ENABLED="ENABLED";
    public static final String TYPE="TYPE";
    public static final String DESCRIPTION="DESCRIPTION";
    public static final String INPUT_FORMAT="INPUT_FORMAT";
    public static final String OUTPUT_FORMAT="OUTPUT_FORMAT";
    public static final String SCRIPT = "SCRIPT";

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

    private static final Pattern regex = Pattern.compile("\\r[\\n]?");
    public String getLineSeparatedXslt() {
        return regex.matcher(theRecord.script).replaceAll(System.lineSeparator());
    }

    @Override
    public Tables table() {
        return Tables.STEP;
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
        json.put(jsonPropertyName(ID), theRecord.id());
        json.put(jsonPropertyName(NAME), theRecord.name());
        json.put(jsonPropertyName(TYPE), theRecord.type());
        json.put(jsonPropertyName(ENABLED), theRecord.enabled());
        json.put(jsonPropertyName(DESCRIPTION), theRecord.description());
        json.put(jsonPropertyName(INPUT_FORMAT), theRecord.inputFormat());
        json.put(jsonPropertyName(OUTPUT_FORMAT), theRecord.outputFormat());
        json.put(jsonPropertyName(SCRIPT), theRecord.script());
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
                    StepRecord rec = ((Step) entity).theRecord;
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
            parser.parse(new ByteArrayInputStream(regex.matcher(xslt).replaceAll(System.lineSeparator()).getBytes(StandardCharsets.UTF_8)));
        } catch (ParserConfigurationException | IOException | SAXException pe) {
            return "Could not parse [ " + xslt + "] as XML: " + pe.getMessage();
        }
        return "OK";
    }

    private void setScript(String xslt) {
        theRecord = new Step.StepRecord(theRecord.id, theRecord.name, theRecord.enabled, theRecord.description, theRecord.type, theRecord.inputFormat,
                theRecord.outputFormat, xslt);
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

}
