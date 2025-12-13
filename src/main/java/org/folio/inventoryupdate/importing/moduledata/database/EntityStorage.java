package org.folio.inventoryupdate.importing.moduledata.database;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.moduledata.LogLine;
import org.folio.inventoryupdate.importing.moduledata.RecordFailure;
import org.folio.tlib.postgres.TenantPgPool;

public class EntityStorage {
  private static final Logger logger = LogManager.getLogger(EntityStorage.class);
  final TenantPgPool pool;
  String tenant;

  /**
   * Constructor.
   */
  public EntityStorage(Vertx vertx, String tenant) {
    this.tenant = tenant;
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

  public Future<UUID> storeEntity(Entity entity) {
    return SqlTemplate.forUpdate(pool.getPool(),
            entity.insertTemplate(pool.getSchema()))
        .mapFrom(entity.toTemplateParameters())
        .execute(entity)
        .onSuccess(res -> logger.info("Created {}. ID [{}]", entity.entityName().toLowerCase(), entity.getId()))
        .onFailure(res -> logger.error("Couldn't save {}: {} {}",
            entity.entityName().toLowerCase(), res.getMessage(), entity.asJson()))
        .map(entity.getId());
  }

  public Future<SqlResult<Void>> updateEntity(Entity entity, String updateTemplate) {
    return SqlTemplate.forUpdate(pool.getPool(), updateTemplate)
        .mapFrom(entity.toTemplateParameters())
        .execute(entity);
  }

  public Future<SqlResult<Void>> updateEntity(UUID entityId, Entity entity) {
    String updateTemplate = entity.updateByIdTemplate(entityId, pool.getSchema());
    return updateEntity(entity, updateTemplate);
  }

  public Future<SqlResult<Void>> updateEntitiesByStatement(Entity entity, String updateTemplate) {
    return SqlTemplate.forUpdate(pool.getPool(), updateTemplate)
        .mapFrom(entity.toUpdateStatementTemplateParameters())
        .execute(entity);
  }

  public Future<Void> storeEntities(List<Entity> entities) {
    if (entities != null && !entities.isEmpty()) {
      // The insert templates are the same for all entities in a batch and some data (the metadata) live outside
      // the entity Record and is the same for all entities of the batch as well. Those templates and data are
      // picked from a single entity instance, though; namely from the first, and possibly only, entity instance,
      // named the "modelEntity"
      Entity modelEntity = entities.getFirst();
      return SqlTemplate.forUpdate(pool.getPool(),
              modelEntity.insertTemplate(pool.getSchema()))
          .mapFrom(modelEntity.toTemplateParameters())
          .executeBatch(entities)
          .onFailure(res -> logger.error("Couldn't save batch of {} with {}: {}",
              modelEntity.entityName().toLowerCase(), modelEntity.insertTemplate(pool.getSchema()), res.getMessage()))
          .mapEmpty();
    } else {
      return Future.succeededFuture();
    }
  }

  public Future<List<Entity>> getEntities(String query, Entity definition) {
    List<Entity> records = new ArrayList<>();
    return SqlTemplate.forQuery(pool.getPool(), query)
        .mapTo(definition.fromRow())
        .execute(null)
        .onSuccess(rows -> {
          for (Entity entity : rows) {
            records.add(entity.withTenant(tenant));
          }
        }).map(records);
  }

  public Future<Entity> getEntity(UUID id, Entity definition) {
    return SqlTemplate.forQuery(pool.getPool(),
            "SELECT * "
                + "FROM " + schemaDotTable(definition.table()) + " "
                + "WHERE id = #{id}")
        .mapTo(definition.fromRow())
        .execute(Collections.singletonMap("id", id))
        .map(rows -> {
          RowIterator<Entity> iterator = rows.iterator();
          return iterator.hasNext() ? iterator.next().withTenant(tenant) : null;
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

  public Future<SqlResult<Void>> purgePreviousJobsByAge(LocalDateTime untilDate) {
    Promise<Void> promise = Promise.promise();
    return SqlTemplate.forUpdate(pool.getPool(),
            "DELETE FROM " + schemaDotTable(Tables.LOG_STATEMENT)
                + " WHERE " + new LogLine().field(LogLine.IMPORT_JOB_ID).columnName()
                + "    IN (SELECT " + new ImportJob().field(ImportJob.ID).columnName()
                + "        FROM " + schemaDotTable(Tables.IMPORT_JOB)
                + "        WHERE " + new ImportJob().field(ImportJob.STARTED).columnName() + " < #{untilDate} )")
        .execute(Collections.singletonMap("untilDate", untilDate))
        .onComplete(deletedLogs -> {
          if (deletedLogs.succeeded()) {
            SqlTemplate.forUpdate(pool.getPool(),
                    "DELETE FROM " + schemaDotTable(Tables.RECORD_FAILURE)
                        + " WHERE " + new RecordFailure().field(LogLine.IMPORT_JOB_ID).columnName()
                        + "    IN (SELECT " + new ImportJob().field(ImportJob.ID).columnName()
                        + "        FROM " + schemaDotTable(Tables.IMPORT_JOB)
                        + "       WHERE " + new ImportJob().field(ImportJob.STARTED).columnName() + " < #{untilDate} )")
                .execute(Collections.singletonMap("untilDate", untilDate))
                .onComplete(deletedFailedRecords -> {
                  if (deletedFailedRecords.succeeded()) {
                    SqlTemplate.forUpdate(pool.getPool(),
                            "DELETE FROM " + schemaDotTable(Tables.IMPORT_JOB)
                                + " WHERE " + new ImportJob().field(ImportJob.STARTED).columnName() + "<#{untilDate} ")
                        .execute(Collections.singletonMap("untilDate", untilDate))
                        .onSuccess(result -> {
                          logger.info("Timer process purged {} import jobs from before {}",
                              result.rowCount(), untilDate);
                          promise.complete();
                        })
                        .onFailure(result -> {
                          logger.error("Timer process: Purge of previous jobs failed. {}",
                              result.getCause().getMessage());
                          promise.fail(String.format("Could not delete job runs with finish dates before %s %s",
                              untilDate, result.getCause().getMessage()));
                        });
                  } else {
                    logger.error("Purge of failed records failed. {}",
                        deletedFailedRecords.cause().getMessage());
                    promise.fail(String.format("Could not delete job runs with finish dates before  %s "
                            + " because deletion of its failed records failed: %s",
                        untilDate, deletedFailedRecords.cause().getMessage()));
                  }
                });
          } else {
            logger.error("Purge of log statements failed. {}", deletedLogs.cause().getMessage());
            promise.fail("Could not delete job runs with finish dates before  " + untilDate
                + " because deletion of its logs failed: "
                + deletedLogs.cause().getMessage());
          }
        });
  }
}
