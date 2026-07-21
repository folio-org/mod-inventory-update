package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import io.vertx.sqlclient.templates.SqlTemplate;
import java.util.Map;
import java.util.UUID;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.tlib.postgres.TenantPgPool;

public class SourceFileDb implements SourceFile {

  final String name;
  final String payload;
  final TenantPgPool pool;
  final UUID channelId;

  public SourceFileDb(String name, String payload, UUID channelId, TenantPgPool pool) {
    this.name = name;
    this.payload = payload;
    this.channelId = channelId;
    this.pool = pool;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getPayload() {
    return payload;
  }

  @Override
  public Future<Void> discard() {
    return SqlTemplate.forUpdate(pool.getPool(),
            "DELETE FROM " + pool.getSchema() + "." + Tables.SOURCE_FILE
                + " WHERE file_name = #{fileName} "
                + " AND channel_id = #{channelId} "
                + " AND processing = 1 ")
        .execute(Map.of("channelId", channelId, "fileName", name))
        .mapEmpty();
  }
}
