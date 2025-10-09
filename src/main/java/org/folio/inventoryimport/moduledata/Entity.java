package org.folio.inventoryimport.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryimport.moduledata.database.SqlQuery;
import org.folio.inventoryimport.moduledata.database.Tables;
import org.folio.inventoryimport.service.ServiceRequest;
import org.folio.tlib.postgres.PgCqlDefinition;
import org.folio.tlib.postgres.PgCqlQuery;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldAlwaysMatches;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public abstract class Entity {

    public static final Logger logger = LogManager.getLogger("inventory-import");

    /**
     * Implement to return an enum identifier for the underlying database table for the implementing entity.
     * @return a Tables enum value
     */
    public abstract Tables table();


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
        columnsDdl.deleteCharAt(columnsDdl.length() - 1); // remove ending comma

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
                        .onFailure(e -> logger.error("Failed to execute [" + sql + "]: " + e.getMessage() )));
            }
            return future;
    }

    public String table(String schema) {
        return schema + "." + table().toString();
    }


    /**
     * Represents a field of an entity, containing JSON property name, database column name and other features of the field.
     * @param jsonPropertyName
     * @param columnName
     * @param pgType
     * @param nullable
     * @param queryable
     * @param primaryKey
     */
    public record Field(String jsonPropertyName, String columnName, PgColumn.Type pgType, boolean nullable, boolean queryable, boolean primaryKey) {
        public Field(String jsonPropertyName, String columnName, PgColumn.Type pgType, boolean nullable, boolean queryable) {
            this(jsonPropertyName, columnName, pgType, nullable, queryable, false);
        }
        public PgColumn pgColumn() {
            return new PgColumn(columnName, pgType, nullable, primaryKey);
        }

        public String pgColumnDdl() {
            return pgColumn().getColumnDdl();
        }
    }

    /**
     * Implement to provide a map of the {@link Field} fields of the implementing entity
     * @return Map fields by field keys to be used for finding queryable fields or, if possible, for creating the database table and more.
     */
    public abstract Map<String, Field> fields();

    /**
     * Implement to map from request body JSON to entity POJO.
     * @param json incoming JSON body
     * @return Entity POJO
     */
    public abstract Entity fromJson(JsonObject json);

    /**
     * Implement to map for entity POJO to response JSON
     * @return json representation of the entity
     */
    public abstract JsonObject asJson();

    /**
     * Vert.x / Postgres template for table insert, using a tuple mapper.
     * This base implementation assumes a simple one-to-one mapping of values to columns. It should be
     * overridden if some entity fields should not be included in the insert statement (virtual fields for example)
     * or additional hardcoded insert values should be applied or some transformations need to happen to the values
     * on the fly, like date or time formatting.
     */
    public String makeInsertTemplate(String schema) {
        StringBuilder listOfColumns = new StringBuilder();
        StringBuilder listOfValues = new StringBuilder();
        fields().keySet().forEach(field -> {
            listOfColumns.append(dbColumnName(field)).append(",");
            listOfValues.append("#{").append(dbColumnName(field)).append("},");
        });
        listOfColumns.deleteCharAt(listOfColumns.length()-1);
        listOfValues.deleteCharAt(listOfValues.length()-1);
        return "INSERT INTO " + schema + "." + table()
                + " (" + listOfColumns + ")"
                + " VALUES (" + listOfValues + ")";
    }

    public String makeUpdateByIdTemplate(UUID entityId, String schema) {
        StringBuilder listOfColumnsValues = new StringBuilder();
        fields().keySet().forEach(field ->
                listOfColumnsValues.append(dbColumnName(field)).append(" = #{").append(dbColumnName(field)).append("},"));
        listOfColumnsValues.deleteCharAt(listOfColumnsValues.length()-1);
        return "UPDATE " + schema + "." + table()
                + " SET "
                + listOfColumnsValues
                + " WHERE id = '" + entityId.toString() + "'";
    }

    /**
     * Creates vert.x row mapper that maps a database select result row onto data object(s).
     */
    public abstract RowMapper<Entity> getRowMapper();

    /**
     * Creates vert.x tuple mapper that maps Postgres column names to field values.
     */
    public abstract TupleMapper<Entity> getTupleMapper();


    /**
     * Gets Postgres/CQL definition, containing listing of queryable fields.
     */
    public PgCqlDefinition getQueryableFields(){
        PgCqlDefinition pgCqlDefinition = PgCqlDefinition.create();
        pgCqlDefinition.addField("cql.allRecords", new PgCqlFieldAlwaysMatches());
        for (Field entityField : fields().values()) {
            if (entityField.queryable()) {
                pgCqlDefinition.addField(entityField.jsonPropertyName(), entityField.pgColumn().pgCqlField());
            }
        }
        return pgCqlDefinition;
    }

    /**
     * Map of JSON property names to Postgres table column definitions (PgColumns).
     */
    public Map<String, PgColumn> getPropertyToColumnMap() {
        return fields().values().stream().collect(Collectors.toMap(Field::jsonPropertyName,Field::pgColumn ));
    }

    /**
     * Gets a SQL query string.
     */
    public SqlQuery makeSqlFromCqlQuery(ServiceRequest request, String schemaDotTable) {
        PgCqlDefinition definition = getQueryableFields();

        String query = request.requestParam("query");
        String offset =  request.requestParam("offset");
        String limit = request.requestParam("limit");

        String select = "SELECT * ";
        String from = "FROM " + schemaDotTable;
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
     * @param clause string containing names to translate
     * @return translated string
     */
    private String jsonPropertiesToColumnNames(String clause) {
        if (clause != null) {
            Map<String, PgColumn> prop2col = getPropertyToColumnMap();
            for (String property : prop2col.keySet()) {
                clause = clause.replaceAll(property.toLowerCase(), prop2col.get(property).name);
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
     * For building JSON collection response
     * @return the JSON property name for a collection of the entity
     */
    public abstract String jsonCollectionName();

    /**
     * For logging and response messages.
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

}
