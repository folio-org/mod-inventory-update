package org.folio.inventoryupdate.importing.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;
import org.folio.inventoryupdate.importing.moduledata.database.Entity;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.moduledata.database.PgColumn;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.xml.sax.SAXException;

public class Step extends Entity {
  // Static map of Entity Fields.
  public static final String ID = "ID";
  public static final String NAME = "NAME";
  public static final String ENABLED = "ENABLED";
  public static final String TYPE = "TYPE";
  public static final String DESCRIPTION = "DESCRIPTION";
  public static final String INPUT_FORMAT = "INPUT_FORMAT";
  public static final String OUTPUT_FORMAT = "OUTPUT_FORMAT";
  public static final String SCRIPT = "SCRIPT";
  private static final Map<String, Field> FIELDS = new HashMap<>();

  static {
    FIELDS.put(ID,
        new Field("id", "id", PgColumn.Type.UUID, false, true).isPrimaryKey());
    FIELDS.put(NAME,
        new Field("name", "name", PgColumn.Type.TEXT, false, true).isUnique());
    FIELDS.put(ENABLED,
        new Field("enabled", "enabled", PgColumn.Type.BOOLEAN, true, true));
    FIELDS.put(DESCRIPTION,
        new Field("description", "description", PgColumn.Type.TEXT, true, true));
    FIELDS.put(TYPE,
        new Field("type", "type", PgColumn.Type.TEXT, true, true));
    FIELDS.put(INPUT_FORMAT,
        new Field("inputFormat", "input_format", PgColumn.Type.TEXT, true, true));
    FIELDS.put(OUTPUT_FORMAT,
        new Field("outputFormat", "output_format", PgColumn.Type.TEXT, true, true));
    FIELDS.put(SCRIPT,
        new Field("script", "script", PgColumn.Type.TEXT, true, false));
  }

  private static final Pattern LINEBREAK_REGEX = Pattern.compile("\\r\\n?");

  private static final ErrorListener XSLT_PARSING_ERRORS = new ErrorListener() {
    final List<String> issues = new ArrayList<>();
    @Override
    public void warning(TransformerException e) throws TransformerException {
      issues.add("Line " + e.getLocator().getLineNumber() + ": " + e.getMessage());
      throw e;
    }

    @Override
    public void error(TransformerException e) throws TransformerException {
      System.out.println(e.getMessage());
      issues.add("Line " + e.getLocator().getLineNumber() + ": " + e.getMessage());
      throw e;
    }

    @Override
    public void fatalError(TransformerException e) throws TransformerException {
      issues.add("Line " + e.getLocator().getLineNumber() + ": " + e.getMessage());
      throw e;
    }

    public String toString() {
      StringBuilder errors = new StringBuilder();
      for (String issue : issues) {
        errors.append(System.lineSeparator()).append(issue);
      }
      return errors.toString();
    }
  };

  StepRecord theRecord;

  public Step() {
  }

  public Step(UUID id, String name, String description, String type, String script) {
    theRecord = new StepRecord(id, name, description, type, script);
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
    return LINEBREAK_REGEX.matcher(theRecord.script).replaceAll(System.lineSeparator());
  }

  @Override
  public Tables table() {
    return Tables.STEP;
  }

  public String name() {
    return asJson().getString(jsonPropertyName(NAME));
  }

  @Override
  public UUID getId() {
    return theRecord == null ? null : theRecord.id();
  }

  /**
   * Creates record from JSON.
   *
   * @param stepJson Step JSON
   * @return Data object
   */
  public Step fromJson(JsonObject stepJson) {
    return new Step(
        getUuidOrGenerate(stepJson.getString(jsonPropertyName(ID))),
        stepJson.getString(jsonPropertyName(NAME)),
        stepJson.getString(jsonPropertyName(TYPE)),
        stepJson.getString(jsonPropertyName(DESCRIPTION)),
        stepJson.getString(jsonPropertyName(SCRIPT)));
  }

  public JsonObject asJson() {
    JsonObject json = new JsonObject();
    json.put(jsonPropertyName(ID), theRecord.id());
    json.put(jsonPropertyName(NAME), theRecord.name());
    json.put(jsonPropertyName(TYPE), theRecord.type());
    json.put(jsonPropertyName(DESCRIPTION), theRecord.description());
    json.put(jsonPropertyName(SCRIPT), theRecord.script());
    putMetadata(json);
    return json;
  }

  /**
   * Maps from PG row to POJO.
   *
   * @return Step data object
   */
  @Override
  public RowMapper<Entity> fromRow() {
    return row -> new Step(
        row.getUUID(dbColumnName(ID)),
        row.getString(dbColumnName(NAME)),
        row.getString(dbColumnName(TYPE)),
        row.getString(dbColumnName(DESCRIPTION)),
        row.getString(dbColumnName(SCRIPT)))
        .withMetadata(row);
  }

  /**
   * Maps from entity data object to PG columns.
   *
   * @return a mapper to be used by PG insert statement
   */
  @Override
  public TupleMapper<Entity> toTemplateParameters() {
    return TupleMapper.mapper(
        entity -> {
          StepRecord rec = ((Step) entity).theRecord;
          Map<String, Object> parameters = new HashMap<>();
          parameters.put(dbColumnName(ID), rec.id);
          parameters.put(dbColumnName(NAME), rec.name);
          parameters.put(dbColumnName(TYPE), rec.type);
          parameters.put(dbColumnName(DESCRIPTION), rec.description);
          parameters.put(dbColumnName(SCRIPT), rec.script);
          putMetadata(parameters);
          return parameters;
        });
  }

  public String validateStyleSheet() {
    return validateStyleSheet(getLineSeparatedXslt());
  }

  public static String validateStyleSheet(String xslt) {
    try {
      DocumentBuilderFactory builder = DocumentBuilderFactory.newInstance();
      builder.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      DocumentBuilder parser = builder.newDocumentBuilder();
      parser.parse(new ByteArrayInputStream(xslt.getBytes(StandardCharsets.UTF_8)));
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      transformerFactory.setErrorListener(XSLT_PARSING_ERRORS);
      transformerFactory.newTransformer();
      Source source = new StreamSource(new StringReader(xslt));
      transformerFactory.newTemplates(source);
    } catch (ParserConfigurationException | IOException | SAXException pe) {
      return "Could not parse [ " + xslt + "] as XML: " + pe.getMessage();
    } catch (TransformerException tce) {
      return tce.getMessage() + XSLT_PARSING_ERRORS;
    }
    return "OK";
  }

  private void setScript(String xslt) {
    theRecord = new Step.StepRecord(theRecord.id, theRecord.name, theRecord.description, theRecord.type, xslt);
  }

  public Future<Void> updateScript(String xslt, EntityStorage storage) {
    setScript(xslt);
    return storage.updateEntity(this,
            "UPDATE " + storage.schema() + "." + table()
                + " SET "
                + dbColumnName(SCRIPT)
                + " = #{" + dbColumnName(SCRIPT) + "}".replaceAll(System.lineSeparator(), "\n") + ", "
                + metadata.updateClauseColumnTemplates()
                + " WHERE id = #{id}")
        .mapEmpty();
  }

  // Step record, the entity data.
  public record StepRecord(UUID id, String name, String description, String type, String script) {
  }
}
