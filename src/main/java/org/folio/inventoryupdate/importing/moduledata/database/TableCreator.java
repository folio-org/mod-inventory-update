package org.folio.inventoryupdate.importing.moduledata.database;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tlib.postgres.TenantPgPool;

public abstract class TableCreator {

  public static final Logger logger = LogManager.getLogger(TableCreator.class);

  public abstract Future<Void> createDatabase(TenantPgPool pool);

  protected abstract Tables table();

  protected Future<RowSet<Row>> executeSqlStatements(TenantPgPool pool, String... statements) {
    Future<RowSet<Row>> future = Future.succeededFuture();
    for (var sql : statements) {
      future = future.compose(x -> pool.query(sql).execute()
          .onFailure(e -> logger.error("Failed to execute [{}]: {}", sql, e.getMessage())));
    }
    return future;
  }
}
