package org.folio.inventoryupdate.importing.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.database.SqlQuery;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.inventoryupdate.importing.utils.SettableClock;
import org.folio.tlib.postgres.PgCqlDefinition;
import org.folio.tlib.postgres.PgCqlQuery;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldAlwaysMatches;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import static org.folio.inventoryupdate.importing.moduledata.Metadata.METADATA_PROPERTY;

public abstract class Entity {

  public static final Logger logger = LogManager.getLogger("inventory-import");
  public static final String DATE_FORMAT_TO_DB = "YYYY-MM-DD''T''HH24:MI:SS,MS";

  protected String tenant;

  /**
   * Implement to return an enum identifier for the underlying database table for the implementing entity.
   *
   * @return a Tables enum value
   */
  public abstract Tables table();

  public abstract UUID getId();

  protected Metadata metadata = new Metadata();

  /**
   * Build and execute the DDL statements for creating the database objects for persisting the entity.
   * The default implementation is for the simplest possible table with no foreign key constraints, indexes
   * or special column validations etc. Commonly overridden.
   *
   * @param pool the TenantPgPool for executing the DDL
   * @return Empty future when done
   */
  public Future<Void> createDatabase(TenantPgPool pool) {
    StringBuilder columnsDdl = new StringBuilder();
    fields().keySet()
        .forEach(field -> columnsDdl.append(field(field).pgColumn().getColumnDdl()).append(","));
    columnsDdl.append(metadata.columnsDdl());
    return executeSqlStatements(pool,
        "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + table()
            + "("
            + columnsDdl
            + ")")
        .mapEmpty();
  }

  protected Future<RowSet<Row>> executeSqlStatements(TenantPgPool pool, String... statements) {
    Future<RowSet<Row>> future = Future.succeededFuture();
    for (var sql : statements) {
      future = future.compose(x -> pool.query(sql).execute()
          .onFailure(e -> logger.error("Failed to execute [{}]: {}", sql, e.getMessage())));
    }
    return future;
  }

  public String table(String schema) {
    return schema + "." + table().toString();
  }


  /**
   * Represents a field of an entity, containing JSON property name, database column name and other features of the field.
   *
   */
  public static class Field {
    public String jsonPropertyName() {
      return jsonPropertyName;
    }

    public String columnName() {
      return columnName;
    }

    public PgColumn.Type pgType() {
      return pgType;
    }

    String jsonPropertyName;
    String columnName;
    PgColumn.Type pgType;
    boolean nullable;
    boolean queryable;
    boolean primaryKey;
    boolean unique;

    public Field(String jsonPropertyName, String columnName, PgColumn.Type pgType, boolean nullable, boolean queryable) {
      this.jsonPropertyName = jsonPropertyName;
      this.columnName = columnName;
      this.pgType = pgType;
      this.nullable = nullable;
      this.queryable = queryable;
    }

    public Field isPrimaryKey() {
      this.primaryKey = true;
      return this;
    }

    public Field isUnique() {
      this.unique = true;
      return this;
    }

    public PgColumn pgColumn() {
      return new PgColumn(columnName, pgType, nullable, primaryKey, unique);
    }

    public String pgColumnDdl() {
      return pgColumn().getColumnDdl().strip();
    }
  }

  /**
   * Implement to provide a map of the {@link Field} fields of the implementing entity
   *
   * @return Map fields by field keys to be used for finding queryable fields or, if possible, for creating the database table and more.
   */
  public abstract Map<String, Field> fields();

  /**
   * Implement to map from request body JSON to entity POJO.
   *
   * @param json incoming JSON body
   * @return Entity POJO
   */
  public abstract Entity fromJson(JsonObject json);

  /**
   * Implement to map for entity POJO to response JSON
   *
   * @return json representation of the entity
   */
  public abstract JsonObject asJson();

  /**
   * Mapping of metadata to JSON
   *
   * @param json target JSON
   */
  public void putMetadata(JsonObject json) {
    json.put(METADATA_PROPERTY, metadata.asJson());
  }

  /**
   * Mapping of metadata to SQL insert/update templates
   *
   * @param parameters template parameters for metadata properties
   */
  public void putMetadata(Map<String, Object> parameters) {
    parameters.putAll(metadata.asTemplateParameters());
  }

  /**
   * Vert.x / Postgres template for table insert, using a tuple mapper.
   * This base implementation assumes a simple one-to-one mapping of values to columns. It should be
   * overridden if some entity fields should not be included in the insert statement (virtual fields for example)
   * or additional hardcoded insert values should be applied or some transformations need to happen to the values
   * on the fly, like date or time formatting.
   */
  public String insertTemplate(String schema) {
    return "INSERT INTO " + schema + "." + table()
        + " (" + insertClauseColumns() + ")"
        + " VALUES (" + insertClauseValueTemplates() + ")";
  }

  public String updateByIdTemplate(UUID entityId, String schema) {
    return "UPDATE " + schema + "." + table()
        + " SET "
        + updateClauseColumnTemplates()
        + " WHERE id = '" + entityId.toString() + "'";
  }

  protected String insertClauseColumns() {
    StringBuilder columnListAsString = new StringBuilder();
    fields().keySet().forEach(field -> columnListAsString.append(dbColumnName(field)).append(","));
    return columnListAsString.append(metadata.insertClauseColumns()).toString();
  }

  protected String insertClauseValueTemplates() {
    StringBuilder valueListAsString = new StringBuilder();
    fields().keySet().forEach(field -> valueListAsString.append("#{").append(dbColumnName(field)).append("},"));
    return valueListAsString.append(metadata.insertClauseValueTemplates()).toString();
  }

  protected String updateClauseColumnTemplates() {
    StringBuilder listOfColumnsValues = new StringBuilder();
    fields().keySet().forEach(field ->
        listOfColumnsValues.append(dbColumnName(field)).append(" = #{").append(dbColumnName(field)).append("},"));
    return listOfColumnsValues.append(metadata.updateClauseColumnTemplates()).toString();
  }


  /**
   * Creates Vert.X row mapper that maps a database select result row onto data object(s).
   */
  public abstract RowMapper<Entity> fromRow();

  /**
   * Adds the record metadata to the Vert.x row mapper.
   *
   * @param row The database record to map from
   */
  public Entity withMetadata(Row row) {
    metadata = new Metadata().fromRow(row);
    return this;
  }

  /**
   * Creates vert.x tuple mapper that maps Postgres column names to field values.
   */
  public abstract TupleMapper<Entity> toTemplateParameters();


  /**
   * Gets Postgres/CQL definition, containing listing of queryable fields.
   */
  public PgCqlDefinition getQueryableFields() {
    PgCqlDefinition pgCqlDefinition = PgCqlDefinition.create();
    pgCqlDefinition.addField("cql.allRecords", new PgCqlFieldAlwaysMatches());
    for (Field entityField : fields().values()) {
      if (entityField.queryable) {
        pgCqlDefinition.addField(entityField.jsonPropertyName, entityField.pgColumn().pgCqlField());
      }
    }
    return pgCqlDefinition;
  }

  /**
   * Map of JSON property names to Postgres table column definitions (PgColumns).
   */
  public Map<String, PgColumn> getPropertyToColumnMap() {
    return fields().values().stream().collect(Collectors.toMap(Field::jsonPropertyName, Field::pgColumn));
  }

  /**
   * Gets a SQL query string from CQL.
   */
  public SqlQuery cqlToSql(ServiceRequest request, String schemaDotTable) {
    PgCqlDefinition definition = getQueryableFields();

    String query = request.requestParam("query");
    String offset = request.requestParam("offset");
    String limit = request.requestParam("limit");
    return cqlToSql(query, offset, limit, schemaDotTable, definition);
  }

  public SqlQuery cqlToSql(String query, String offset, String limit, String table, PgCqlDefinition definition) {
    String select = "SELECT * ";
    String from = "FROM " + table;
    String whereClause = "";
    String orderByClause = "";
    if (query != null && !query.isEmpty()) {
      PgCqlQuery pgCqlQuery = definition.parse(query);
      if (pgCqlQuery.getWhereClause() != null) {
        whereClause = jsonPropertiesToColumnNames(pgCqlQuery.getWhereClause());
      }
      if (pgCqlQuery.getOrderByClause() != null) {
        orderByClause = jsonPropertiesToColumnNames(pgCqlQuery.getOrderByClause());
      }
    }
    return new SqlQuery(select, from, whereClause, orderByClause, offset, limit);

  }
  /**
   * Crosswalk JSON property names to table column names.
   *
   * @param clause string containing names to translate
   * @return translated string
   */
  private String jsonPropertiesToColumnNames(String clause) {
    if (clause != null) {
      Map<String, PgColumn> prop2col = getPropertyToColumnMap();
      for (Map.Entry<String, PgColumn> property : prop2col.entrySet()) {
        clause = clause.replaceAll(property.getKey().toLowerCase(), prop2col.get(property.getKey()).name);
      }
    }
    return clause;
  }

  public Field field(String fieldCode) {
    return fields().get(fieldCode);
  }

  public String jsonPropertyName(String key) {
    return field(key).jsonPropertyName();
  }

  public String dbColumnName(String key) {
    return field(key).columnName();
  }

  public String dbColumnType(String key) {
    return field(key).pgType().name();
  }

  public String dbColumnNameAndType(String key) {
    return dbColumnName(key) + " " + dbColumnType(key);
  }

  /**
   * For naming the property of a JSON collection response
   *
   * @return the JSON property name for a collection of the entity
   */
  public abstract String jsonCollectionName();

  /**
   * For logging statements and response messages.
   *
   * @return label to display in messages.
   */
  public abstract String entityName();

  public UUID getUuidOrGenerate(String uuidAsString) {
    try {
      return UUID.fromString(uuidAsString);
    } catch (Exception e) {
      return UUID.randomUUID();
    }
  }

  /**
   * Setting current user (if any) as the creating user in the record's metadata
   *
   * @param currentUser UUID of current user
   */
  public Entity withCreatingUser(UUID currentUser) {
    metadata = new Metadata().withCreatedByUserId(currentUser).withCreatedDate(SettableClock.getLocalDateTime().toString());
    return this;
  }

  /**
   * Setting current user (if any) as the updating user in the record's metadata
   *
   * @param currentUser UUID of current user
   */
  public Entity withUpdatingUser(UUID currentUser) {
    metadata = new Metadata().withUpdatedByUserId(currentUser).withUpdatedDate(SettableClock.getLocalDateTime().toString());
    return this;
  }

  public Entity withTenant (String tenant) {
    this.tenant = tenant;
    return this;
  }

}
