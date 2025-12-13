package org.folio.inventoryupdate.importing.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.SqlTemplate;
import io.vertx.sqlclient.templates.TupleMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.folio.inventoryupdate.importing.moduledata.database.Entity;
import org.folio.inventoryupdate.importing.moduledata.database.PgColumn;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.tlib.postgres.TenantPgPool;

public class TransformationStep extends Entity {

  public static final String ID = "ID";
  public static final String TRANSFORMATION_ID = "TRANSFORMATION_ID";
  public static final String STEP_ID = "STEP_ID";
  public static final String POSITION = "POSITION";
  private static final Map<String, Field> FIELDS = new HashMap<>();

  static {
    FIELDS.put(ID,
        new Field("id", "id", PgColumn.Type.UUID, false, true).isPrimaryKey());
    FIELDS.put(TRANSFORMATION_ID,
        new Field("transformationId", "transformation_id", PgColumn.Type.UUID, false, true));
    FIELDS.put(STEP_ID,
        new Field("stepId", "step_id", PgColumn.Type.UUID, true, true));
    FIELDS.put(POSITION,
        new Field("position", "position", PgColumn.Type.INTEGER, false, true));
  }

  TransformationStepRecord theRecord;

  private Integer positionOfLastStepOfTransformation = null;
  private Integer positionOfTheExistingStep = null;
  private Integer newPosition = null;

  public TransformationStep() {
  }

  public TransformationStep(UUID id, UUID transformationId, UUID stepId, Integer position) {
    theRecord = new TransformationStepRecord(id, transformationId, stepId, position);
  }

  public TransformationStepRecord getRecord() {
    return theRecord;
  }

  // Static map of Entity Fields.

  @Override
  public Map<String, Field> fields() {
    return FIELDS;
  }

  @Override
  public String jsonCollectionName() {
    return "transformationStepAssociations";
  }

  @Override
  public String entityName() {
    return "Transformation-step association";
  }

  @Override
  public TransformationStep fromJson(JsonObject json) {
    return new TransformationStep(
        getUuidOrGenerate(json.getString(jsonPropertyName(ID))),
        json.containsKey(jsonPropertyName(TRANSFORMATION_ID))
            ? UUID.fromString(json.getString(jsonPropertyName(TRANSFORMATION_ID)))
            : UUID.fromString(json.getString("transformation")), // legacy Harvester schema
        json.containsKey(jsonPropertyName(STEP_ID))
            ? UUID.fromString(json.getString(jsonPropertyName(STEP_ID)))
            : UUID.fromString(json.getJsonObject("step").getString("id")), // legacy Harvester schema
        Integer.parseInt(json.getString(jsonPropertyName(POSITION))));
  }

  @Override
  public JsonObject asJson() {
    JsonObject json = new JsonObject();
    json.put(jsonPropertyName(ID), theRecord.id);
    json.put(jsonPropertyName(TRANSFORMATION_ID), theRecord.transformationId);
    json.put(jsonPropertyName(STEP_ID), theRecord.stepId);
    json.put(jsonPropertyName(POSITION), theRecord.position);
    putMetadata(json);
    return json;
  }

  @Override
  public RowMapper<Entity> fromRow() {
    return row -> new TransformationStep(
        row.getUUID(dbColumnName(ID)),
        row.getUUID(dbColumnName(TRANSFORMATION_ID)),
        row.getUUID(dbColumnName(STEP_ID)),
        row.getInteger(dbColumnName(POSITION))).withMetadata(row);
  }

  @Override
  public TupleMapper<Entity> toTemplateParameters() {
    return TupleMapper.mapper(
        entity -> {
          TransformationStepRecord rec = ((TransformationStep) entity).theRecord;
          Map<String, Object> parameters = new HashMap<>();
          parameters.put(dbColumnName(ID), rec.id);
          parameters.put(dbColumnName(TRANSFORMATION_ID), rec.transformationId);
          parameters.put(dbColumnName(STEP_ID), rec.stepId);
          parameters.put(dbColumnName(POSITION), rec.position);
          putMetadata(parameters);
          return parameters;
        });
  }

  @Override
  public Tables table() {
    return Tables.TRANSFORMATION_STEP;
  }

  @Override
  public UUID getId() {
    return theRecord == null ? null : theRecord.id();
  }

  public Future<Void> createTsaRepositionSteps(ServiceRequest request) {
    return request.entityStorage().storeEntity(this.withCreatingUser(request.currentUser()))
        .onSuccess(ignore -> executeSqlStatements(request.entityStorage().getTenantPool(),
            // Potentially adjust the positions of other steps
            "UPDATE " + this.schemaTable(request.dbSchema())
                + " SET position = position + 1 "
                + " WHERE transformation_id = '" + theRecord.transformationId + "'"
                + "   AND position >= " + this.theRecord.position
                + "   AND id != '" + this.theRecord.id + "'"
        )).mapEmpty();
  }

  public Future<Void> updateTsaRepositionSteps(ServiceRequest request, int positionOfExistingTsa) {
    return findPositionOfLastStepOfTransformation(request.entityStorage().getTenantPool())
        .compose(maxPosition -> {
          this.positionOfLastStepOfTransformation = maxPosition;
          this.positionOfTheExistingStep = positionOfExistingTsa;
          if (this.theRecord.position != positionOfExistingTsa) { // new position requested
            // Cannot get new position beyond the last step, assuming this step should just become the last
            this.newPosition = Math.min(positionOfLastStepOfTransformation, this.theRecord.position);
          } else {
            this.newPosition = positionOfExistingTsa;
          }
          return Future.succeededFuture(this);
        }).compose(rec -> executeUpdateAndAdjustPositions(request));
  }

  public Future<Integer> findPositionOfLastStepOfTransformation(TenantPgPool tenantPool) {
    return SqlTemplate.forQuery(tenantPool.getPool(),
            "SELECT MAX(position) AS last_position "
                + "FROM " + schemaTable(tenantPool.getSchema()) + " "
                + "WHERE transformation_id = #{transformationId}")
        .execute(Collections.singletonMap("transformationId", theRecord.transformationId))
        .map(rows -> rows.iterator().next().getInteger("last_position"));
  }

  public Future<Void> executeUpdateAndAdjustPositions(ServiceRequest request) {

    return request.entityStorage().updateEntity(this.theRecord.id, this.withUpdatingUser(request.currentUser()))
        .onSuccess(ignore ->
            executeSqlStatements(request.entityStorage().getTenantPool(),
                // Update the one property that can change besides position.
                "UPDATE " + schemaTable(request.dbSchema())
                    + " SET step_id = '" + theRecord.stepId + "'"
                    + " WHERE id = '" + theRecord.id + "'",
                // Update position while potentially adjusting the positions of other steps
                "UPDATE " + schemaTable(request.dbSchema())
                    + " SET position = "
                    + "     CASE "
                    // set the new position of the step
                    + "       WHEN id = '" + this.theRecord.id + "' THEN " + this.newPosition + " "
                    // the step is moving towards end of pipeline, move affected steps back
                    + "       WHEN " + this.newPosition + " > " + this.positionOfTheExistingStep + " THEN position - 1 "
                    // the step is moving towards beginning of pipeline, move affected steps forward
                    + "       WHEN " + this.newPosition + " < " + this.positionOfTheExistingStep + " THEN position + 1 "
                    + "       ELSE position  "  // not a move (though we shouldn't get here due to the first WHEN
                    + "     END "
                    + " WHERE transformation_id = '" + theRecord.transformationId + "'"
                    + "   AND position BETWEEN SYMMETRIC " + this.positionOfTheExistingStep + " AND " + this.newPosition
            )).mapEmpty();
  }

  public Future<Void> deleteTsaRepositionSteps(TenantPgPool tenantPool, int positionOfExistingTsa) {
    return findPositionOfLastStepOfTransformation(tenantPool).compose(maxPosition -> {
      this.positionOfLastStepOfTransformation = maxPosition;
      this.positionOfTheExistingStep = positionOfExistingTsa;
      return Future.succeededFuture(this);
    }).compose(rec -> executeDeleteAndAdjustPositions(tenantPool, rec));
  }

  public Future<Void> executeDeleteAndAdjustPositions(TenantPgPool tenantPool, TransformationStep updatingTsa) {
    return executeSqlStatements(tenantPool,
        "DELETE FROM " + this.schemaTable(tenantPool.getSchema())
            + " WHERE id = '" + theRecord.id + "'",
        // Delete position while potentially adjusting the positions of other steps
        "UPDATE " + this.schemaTable(tenantPool.getSchema())
            + " SET position = position - 1 "
            + " WHERE transformation_id = '" + theRecord.transformationId + "'"
            + "   AND position > " + updatingTsa.positionOfTheExistingStep
    ).mapEmpty();
  }

  public Future<Void> deleteStepsOfTransformation(ServiceRequest request, UUID transformationId) {
    TenantPgPool pool = request.entityStorage().getTenantPool();
    return executeSqlStatements(pool,
        "DELETE FROM " + this.schemaTable(pool.getSchema())
            + " WHERE transformation_id = '" + transformationId.toString() + "'").mapEmpty();
  }

  @Override
  public Future<Void> createDatabase(TenantPgPool pool) {
    return executeSqlStatements(pool,
        "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + table()
            + " ("
            + dbColumnName(ID) + " UUID PRIMARY KEY, "
            + dbColumnName(TRANSFORMATION_ID) + " UUID NOT NULL "
            + " REFERENCES " + pool.getSchema() + "." + Tables.TRANSFORMATION
            + "(" + new Transformation().dbColumnName(Transformation.ID) + "), "
            + dbColumnName(STEP_ID) + " UUID NOT NULL "
            + " REFERENCES " + pool.getSchema() + "." + Tables.STEP + "(" + new Step().dbColumnName(Step.ID) + "), "
            + dbColumnName(POSITION) + " INTEGER NOT NULL, "
            + metadata.columnsDdl()
            + ") "

    ).mapEmpty();
  }

  // Transformation/Step association record, the entity data.
  public record TransformationStepRecord(UUID id, UUID transformationId, UUID stepId, Integer position) {
  }
}
