package org.folio.inventoryimport.moduledata.database;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryimport.moduledata.*;
import org.folio.inventoryimport.service.ServiceRequest;
import org.folio.tlib.postgres.TenantPgPool;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.folio.inventoryimport.moduledata.ImportJob.ID;
import static org.folio.inventoryimport.moduledata.ImportJob.STARTED;
import static org.folio.inventoryimport.moduledata.LogLine.IMPORT_JOB_ID;


public class ModuleStorageAccess {
    final TenantPgPool pool;
    private static final Logger logger = LogManager.getLogger(ModuleStorageAccess.class);


    /**
     * Constructor.
     */
    public ModuleStorageAccess(Vertx vertx, String tenant) {
        pool = TenantPgPool.pool(vertx, tenant);
    }

    public String schemaDotTable(Tables table) {
        return pool.getSchema() + "." + table.name();
    }

    public String schema() {
        return pool.getSchema();
    }

    /**
     * tenant init handling (including disable).
     *
     * @param tenantAttributes as passed in tenant init
     * @return async result.
     */
    public Future<Void> init(JsonObject tenantAttributes) {
        if (!tenantAttributes.containsKey("module_to")) {
            return Future.succeededFuture(); // doing nothing for disable
        } else {
            return DatabaseInit.createDatabase(pool);
        }
    }

    public TenantPgPool getTenantPool() {
        return pool;
    }

    public Future<String> getScript(ServiceRequest request) {
        String id = request.requestParam("id");
        Promise<String> promise = Promise.promise();
        getEntity(UUID.fromString(id), new Step()).onComplete(step -> {
            if (step.result() != null) {
                promise.complete(((Step)step.result()).getLineSeparatedXslt());
            } else {
                promise.fail("Could not find step with ID " + id + " to GET script from");
            }
        });
        return promise.future();
    }

    public Future<Void> putScript(ServiceRequest request) {
        String id = request.requestParam("id");
        String script = request.bodyAsString();
        Promise<Void> promise = Promise.promise();
        getEntity(UUID.fromString(id), new Step()).onComplete(getStep -> {
            if (getStep.result() != null) {
                Step step = (Step) getStep.result();
                step.updateScript(script, this).onSuccess(ignore->promise.complete());
            } else {
                promise.fail("Could not find step with ID " + id + " to GET script from");
            }
        });
        return promise.future();
    }

    public Future<UUID> storeEntity(Entity entity) {
        return SqlTemplate.forUpdate(pool.getPool(),
                        entity.makeInsertTemplate(pool.getSchema()))
                .mapFrom(entity.getTupleMapper())
                .execute(entity)
                .onSuccess(res -> logger.info("Created " + entity.entityName().toLowerCase() + ". ID [" + entity.asJson().getString("id") + "]"))
                .onFailure(res -> logger.error("Couldn't save " + entity.entityName().toLowerCase() + ": " + res.getMessage() + " " + entity.asJson()))
                .map(UUID.fromString(entity.asJson().getString("id")));
    }

    public Future<SqlResult<Void>> updateEntity (Entity entity, String updateTemplate) {
        return SqlTemplate.forUpdate(pool.getPool(), updateTemplate)
                .mapFrom(entity.getTupleMapper())
                .execute(entity);
    }

    public Future<SqlResult<Void>> updateEntity (UUID entityId, Entity entity) {
        String updateTemplate = entity.makeUpdateByIdTemplate(entityId, pool.getSchema());
        return updateEntity(entity, updateTemplate);
    }

    public Future<Void> storeEntities(Entity definition, List<Entity> entities) {
        if (entities!=null && !entities.isEmpty()) {
            return SqlTemplate.forUpdate(pool.getPool(),
                            definition.makeInsertTemplate(pool.getSchema()))
                    .mapFrom(definition.getTupleMapper())
                    .executeBatch(entities)
                    .onSuccess(res -> logger.info("Saved batch of " + definition.entityName().toLowerCase() + ", row count: " + res.rowCount()))
                    .onFailure(res -> logger.error("Couldn't save batch of " + definition.entityName().toLowerCase() + ": " + res.getMessage()))
                    .mapEmpty();
        } else {
            return Future.succeededFuture();
        }
    }

    public Future<List<Entity>> getEntities(String query, Entity definition) {
        List<Entity> records = new ArrayList<>();
        return SqlTemplate.forQuery(pool.getPool(), query)
                .mapTo(definition.getRowMapper())
                .execute(null)
                .onSuccess(rows -> {
                    for (Entity record : rows) {
                        records.add(record);
                    }
                }).map(records);
    }

    public Future<Entity> getEntity(UUID id, Entity definition) {
        return SqlTemplate.forQuery(pool.getPool(),
                        "SELECT * "
                                + "FROM " + schemaDotTable(definition.table()) + " "
                                + "WHERE id = #{id}")
                .mapTo(definition.getRowMapper())
                .execute(Collections.singletonMap("id", id))
                .map(rows -> {
                    RowIterator<Entity> iterator = rows.iterator();
                    return iterator.hasNext() ? iterator.next() : null;
                });
    }

    public Future<Integer> deleteEntity(UUID id, Entity definition) {
        return SqlTemplate.forUpdate(pool.getPool(),
                        "DELETE FROM " + schemaDotTable(definition.table()) + " "
                                + "WHERE id = #{id}")
                .execute(Collections.singletonMap("id", id))
                .map(SqlResult::rowCount);
    }

    /**
     * Gets record count.
     */
    public Future<Long> getCount(String sql) {
        return SqlTemplate.forQuery(pool.getPool(), sql)
                .execute(null)
                .map(rows -> rows.iterator().next().getLong("total_records"));
    }

    public Future<SqlResult<Void>> purgePreviousJobsByAge (LocalDateTime untilDate) {
        Promise<Void> promise = Promise.promise();
        return SqlTemplate.forUpdate(pool.getPool(),
                        "DELETE FROM " + schemaDotTable(Tables.log_statement)
                                + " WHERE " +  new LogLine().field(IMPORT_JOB_ID).columnName() +
                                "    IN (SELECT " + new ImportJob().field(ID).columnName() +
                                "        FROM " + schemaDotTable(Tables.import_job) +
                                "        WHERE " + new ImportJob().field(STARTED).columnName() + " < #{untilDate} )")
                .execute(Collections.singletonMap("untilDate", untilDate))
                .onComplete(deletedLogs -> {
                    if (deletedLogs.succeeded()) {
                        SqlTemplate.forUpdate(pool.getPool(),
                                        "DELETE FROM " + schemaDotTable(Tables.record_failure)
                                                + " WHERE " + new RecordFailure().field(IMPORT_JOB_ID).columnName() +
                                                "    IN (SELECT " + new ImportJob().field(ID).columnName() +
                                                "        FROM " + schemaDotTable(Tables.import_job) +
                                                "        WHERE " + new ImportJob().field(STARTED).columnName() + " < #{untilDate} )")
                                .execute(Collections.singletonMap("untilDate", untilDate))
                                .onComplete(deletedFailedRecords -> {
                                    if (deletedFailedRecords.succeeded()) {
                                        SqlTemplate.forUpdate(pool.getPool(),
                                                        "DELETE FROM " + schemaDotTable(Tables.import_job) +
                                                                "        WHERE " + new ImportJob().field(STARTED).columnName() + " < #{untilDate} ")
                                                .execute(Collections.singletonMap("untilDate", untilDate))
                                                .onSuccess( result -> {
                                                    logger.info("Timer process purged " + result.rowCount() + " harvest job runs from before " + untilDate);
                                                    promise.complete();
                                                })
                                                .onFailure( result -> {
                                                    logger.error("Timer process: Purge of previous jobs failed." + result.getCause().getMessage());
                                                    promise.fail("Could not delete job runs with finish dates before  " + untilDate
                                                            + result.getCause().getMessage());
                                                });
                                    } else {
                                        logger.error("Purge of failed records failed." + deletedFailedRecords.cause().getMessage());
                                        promise.fail("Could not delete job runs with finish dates before  " + untilDate
                                                + " because deletion of its failed records failed: "
                                                + deletedFailedRecords.cause().getMessage());
                                    }
                                });
                    } else {
                        logger.error("Purge of log statements failed." + deletedLogs.cause().getMessage());
                        promise.fail("Could not delete job runs with finish dates before  " + untilDate
                                + " because deletion of its logs failed: "
                                + deletedLogs.cause().getMessage());
                    }
                });
    }

}
