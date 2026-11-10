package org.folio.inventoryupdate.importing.moduledata;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.templates.SqlTemplate;
import io.vertx.sqlclient.templates.TupleMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.database.TableCreator;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.reporting.InventoryMetrics;
import org.folio.tlib.postgres.TenantPgPool;

public class OutcomeReport extends TableCreator {

  static final Logger logger = LogManager.getLogger(OutcomeReport.class);
  private TenantPgPool pool;
  private UUID reportId;

  public OutcomeReport() {}

  public OutcomeReport(TenantPgPool pool) {
    this.pool = pool;
  }

  @Override
  public Tables table() {
    return Tables.OUTCOME_TABLES;
  }

  public Future<Void> init(String filename, UUID channelId) {
    reportId = UUID.randomUUID();
    logger.info("init with {}", reportId);
    String stmt = "INSERT INTO " + pool.getSchema() + ".processed_file "
        + " (id, filename, channel_id, started) "
        + " VALUES ( $1, $2, $3, statement_timestamp()) ";
    return pool.preparedQuery(stmt).execute(Tuple.of(reportId, filename, channelId))
        .onFailure(f ->
            logger.error("Error initiating outcome counts for channel ID {} and filename {}: {}",
                channelId, filename, f.getMessage())).mapEmpty();
  }

  public void fileProcessed(int sourceRecordCount) {
    String stmt = "UPDATE " + pool.getSchema() + ".processed_file "
        + " SET finished = statement_timestamp(), "
        + "     source_records = $2 "
        + " WHERE id = $1 ";
    pool.preparedQuery(stmt).execute(Tuple.of(reportId, sourceRecordCount))
        .onFailure(f ->
            logger.error("Error finalising record count for repot ID {}", reportId)).mapEmpty();
  }

  public void updateRecordCount(int sourceRecordCount) {
    String stmt = "UPDATE " + pool.getSchema() + ".processed_file "
        + " SET source_records = $2 "
        + " WHERE id = $1 ";
    pool.preparedQuery(stmt).execute(Tuple.of(reportId, sourceRecordCount))
        .onFailure(f ->
            logger.error("Error updating record count for report ID {}", reportId)).mapEmpty();

  }

  public void storeOutcomeCounts(List<InventoryMetrics.Count> counts) {
    SqlTemplate.forUpdate(pool.getPool(),
            "INSERT INTO " + pool.getSchema() + ".outcome "
                + " (id, processed_file_id, entity, transaction, outcome, count) "
                + " VALUES ( #{id}, #{processed_file_id}, "
                + " #{entity}, #{transaction}, #{outcome}, #{count})")
        .mapFrom(TupleMapper.<InventoryMetrics.Count>mapper(cnt -> {
          Map<String, Object> parameters = new HashMap<>();
          parameters.put("id", UUID.randomUUID());
          parameters.put("processed_file_id", reportId);
          parameters.put("entity", cnt.entity());
          parameters.put("transaction", cnt.transaction());
          parameters.put("outcome", cnt.outcome());
          parameters.put("count", cnt.count());
          return parameters;
        }))
        .executeBatch(counts)
        .onFailure(res -> logger.error("Couldn't save batch of outcome metrics with: {}",
            res.getMessage()))
        .mapEmpty();
  }

  public Future<JsonObject> getOutcomeCounts(String from, String until, String channelId) {
    JsonObject metrics = new JsonObject();
    JsonObject outcomes = new JsonObject()
        .put("COMPLETED", 0).put("FAILED", 0).put("SKIPPED", 0).put("PENDING", 0);
    metrics
        .put("INSTANCE", new JsonObject()
            .put("CREATE", outcomes.copy()).put("UPDATE", outcomes.copy()).put("DELETE", outcomes.copy()))
        .put("HOLDINGS_RECORD", new JsonObject()
            .put("CREATE", outcomes.copy()).put("UPDATE", outcomes.copy()).put("DELETE", outcomes.copy()))
        .put("ITEM", new JsonObject()
            .put("CREATE", outcomes.copy()).put("UPDATE", outcomes.copy()).put("DELETE", outcomes.copy()))
        .put("INSTANCE_RELATIONSHIP", new JsonObject()
            .put("CREATE", outcomes.copy()).put("DELETE", outcomes.copy())
            .put("PROVISIONAL_INSTANCE", outcomes.copy()))
        .put("INSTANCE_TITLE_SUCCESSION", new JsonObject()
            .put("CREATE", outcomes.copy()).put("DELETE", outcomes.copy())
            .put("PROVISIONAL_INSTANCE", outcomes.copy()));

    return pool.preparedQuery(
        "SELECT entity, transaction, outcome, SUM(count) count "
            + " FROM " + pool.getSchema() + ".outcome, "
            +            pool.getSchema() + ".processed_file "
            + " WHERE outcome.processed_file_id = processed_file.id "
            + "   AND ($1 = 'undefined' OR started > TO_TIMESTAMP($1,'YYYY-MM-DD''T''HH24:MI:SS,MS')) "
            + "   AND ($2 = 'undefined' OR started <= TO_TIMESTAMP($2,'YYYY-MM-DD''T''HH24:MI:SS,MS')) "
            + "   AND ($3 = 'undefined' OR channel_id = $3::UUID) "
            + " GROUP BY entity, transaction, outcome ")
        .execute(Tuple.of(from, until, channelId))
        .onFailure(f -> {
          String msg = String.format("Failed to retrieve summary of outcomes: %s", f.getMessage());
          logger.error(msg);
          Future.failedFuture(msg);
        }).compose(rowSet -> {
          for (Row row : rowSet) {
            metrics.getJsonObject(row.getString("entity")).getJsonObject(row.getString("transaction"))
                .put(row.getString("outcome"), row.getInteger("count"));
          }
          return Future.succeededFuture(metrics);
        }).compose(populatedMetrics -> getSourceRecordsCount(from, until, channelId).compose(sourceRecordsCount -> {
          JsonObject results = new JsonObject();
          results.put("fromDate", from.equalsIgnoreCase("undefined") ? "Unbounded" : from);
          results.put("untilDate", until.equalsIgnoreCase("undefined") ? "Unbounded" : until);
          results.put("channelId", channelId.equalsIgnoreCase("undefined") ? "Any channel" : channelId);
          results.put("sourceRecords", sourceRecordsCount);
          results.put("metrics", populatedMetrics);
          return Future.succeededFuture(results);
        }));
  }

  private Future<Integer> getSourceRecordsCount(String from, String until, String channelId) {
    return pool.preparedQuery(
            "SELECT SUM(source_records) count "
                + " FROM " + pool.getSchema() + ".processed_file "
                + " WHERE ($1 = 'undefined' OR started > TO_TIMESTAMP($1,'YYYY-MM-DD''T''HH24:MI:SS,MS')) "
                + "   AND ($2 = 'undefined' OR started <= TO_TIMESTAMP($2,'YYYY-MM-DD''T''HH24:MI:SS,MS')) "
                + "   AND ($3 = 'undefined' OR channel_id = $3::UUID) ")
        .execute(Tuple.of(from, until, channelId))
        .onFailure(f -> {
          String msg = String.format("Failed to retrieve summary of outcomes: %s", f.getMessage());
          logger.error(msg);
          Future.failedFuture(msg);
        }).compose(rowSet -> {
          Integer count = rowSet.iterator().next().getInteger("count");
          return Future.succeededFuture(count == null ? Integer.valueOf(0) : count);
        });
  }

  public Future<Void> createDatabase(TenantPgPool pool) {
    String[] statements = {
      "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + "processed_file "
          + " (id UUID PRIMARY KEY, "
          + "  filename TEXT, "
          + "  channel_id UUID REFERENCES " + pool.getSchema() + "." + Tables.CHANNEL + "(id) ON DELETE CASCADE, "
          + "  started TIMESTAMP, "
          + "  finished TIMESTAMP, "
          + "  source_records INTEGER )",
      "CREATE INDEX IF NOT EXISTS processed_file_channel_id_idx ON "
          + pool.getSchema() + ".processed_file (channel_id)",
      "CREATE TABLE IF NOT EXISTS " + pool.getSchema() + "." + "outcome "
          + " (id UUID PRIMARY KEY, "
          + "  processed_file_id UUID REFERENCES " + pool.getSchema() + ".processed_file (id) ON DELETE CASCADE, "
          + "  entity TEXT, "
          + "  transaction TEXT, "
          + "  outcome TEXT, "
          + "  count INTEGER, "
          + "CONSTRAINT supported_entities CHECK (entity in "
          + "   ('INSTANCE', 'HOLDINGS_RECORD', 'ITEM', 'INSTANCE_RELATIONSHIP', 'INSTANCE_TITLE_SUCCESSION')), "
          + "CONSTRAINT supported_transactions CHECK (transaction in "
          + "   ('CREATE', 'UPDATE', 'DELETE', 'PROVISIONAL_INSTANCE')), "
          + "CONSTRAINT supported_outcomes CHECK (outcome in "
          + "   ('COMPLETED', 'FAILED', 'SKIPPED', 'PENDING')))",
      "CREATE INDEX IF NOT EXISTS outcome_processed_file_id_idx ON "
          + pool.getSchema() + ".outcome (processed_file_id)"};
    logger.info("Create outcome tables: {}", () -> Arrays.toString(statements));
    return executeSqlStatements(pool, statements).mapEmpty();
  }
}
