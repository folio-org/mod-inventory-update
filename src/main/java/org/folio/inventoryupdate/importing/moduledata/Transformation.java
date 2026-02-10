package org.folio.inventoryupdate.importing.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.folio.inventoryupdate.importing.moduledata.database.Entity;
import org.folio.inventoryupdate.importing.moduledata.database.PgColumn;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.tlib.postgres.TenantPgPool;

public class Transformation extends Entity {

  public static final String ID = "ID";
  public static final String NAME = "NAME";
  public static final String DESCRIPTION = "DESCRIPTION";
  private static final Map<String, Field> FIELDS = new HashMap<>();

  static {
    FIELDS.put(ID,
        new Field("id", "id", PgColumn.Type.UUID, false, true).isPrimaryKey());
    FIELDS.put(NAME,
        new Field("name", "name", PgColumn.Type.TEXT, false, true).isUnique());
    FIELDS.put(DESCRIPTION,
        new Field("description", "description", PgColumn.Type.TEXT, true, true));
  }

  TransformationRecord theRecord;
  private JsonArray stepsArray = new JsonArray();

  public Transformation() {
  }

  public Transformation(UUID id, String name, String description) {
    theRecord = new TransformationRecord(id, name, description);
  }

  public TransformationRecord getRecord() {
    return theRecord;
  }

  @Override
  public Map<String, Field> fields() {
    return FIELDS;
  }

  @Override
  public Tables table() {
    return Tables.TRANSFORMATION;
  }

  @Override
  public UUID getId() {
    return theRecord == null ? null : theRecord.id();
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
        json.getString(jsonPropertyName(DESCRIPTION)))
        .setStepsArray(json.getJsonArray("steps", new JsonArray()));
  }

  public Transformation setStepsArray(JsonArray steps) {
    stepsArray = steps;
    return this;
  }

  public boolean containsListOfSteps() {
    return !stepsArray.isEmpty();
  }

  public List<Entity> getListOfTransformationSteps() {
    if (containsListOfSteps()) {
      List<Entity> tsas = new ArrayList<>();
      for (int i = 0; i < stepsArray.size(); i++) {
        JsonObject step = stepsArray.getJsonObject(i);
        tsas.add(new TransformationStep(
            UUID.randomUUID(),
            this.theRecord.id,
            UUID.fromString(step.getString("id")),
            i + 1));
      }
      return tsas;
    } else {
      return new ArrayList<>();
    }
  }

  public JsonObject asJson() {
    JsonObject json = new JsonObject();
    json.put(jsonPropertyName(ID), theRecord.id);
    json.put(jsonPropertyName(NAME), theRecord.name);
    json.put(jsonPropertyName(DESCRIPTION), theRecord.description);
    json.put("steps", stepsArray);
    putMetadata(json);
    return json;
  }

  @Override
  public RowMapper<Entity> fromRow() {
    return row -> new Transformation(
        row.getUUID(dbColumnName(ID)),
        row.getString(dbColumnName(NAME)),
        row.getString(dbColumnName(DESCRIPTION)))
        .withMetadata(row);
  }

  @Override
  public TupleMapper<Entity> toTemplateParameters() {
    return TupleMapper.mapper(
        entity -> {
          TransformationRecord rec = ((Transformation) entity).theRecord;
          Map<String, Object> parameters = new HashMap<>();
          parameters.put(dbColumnName(ID), rec.id);
          parameters.put(dbColumnName(NAME), rec.name);
          parameters.put(dbColumnName(DESCRIPTION), rec.description);
          putMetadata(parameters);
          return parameters;
        });
  }

  public Future<Transformation> fetchTransformationSteps(TenantPgPool pool) {
    return new TransformationStep().fetchTransformationStepsByTransformationId(pool, this.getId())
        .map(steps -> {
          for (JsonObject step : steps) {
            stepsArray.add(step);
          }
          return this;
        });
  }

  public record TransformationRecord(UUID id, String name, String description) {
  }
}
