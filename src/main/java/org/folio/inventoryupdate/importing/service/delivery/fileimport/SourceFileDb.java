package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import io.vertx.sqlclient.templates.SqlTemplate;
import java.util.HashMap;
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
    Map<String, Object> params = new HashMap<>();
    params.put("channelId", channelId);
    params.put("fileName", name);
    return SqlTemplate.forUpdate(pool.getPool(),
            "UPDATE " + pool.getSchema() + "." + Tables.SOURCE_FILE
                + " SET done = true, "
                + "     processing = NULL "
                + " WHERE file_name = #{fileName} "
                + " AND channel_id = #{channelId} ")
        .execute(params)
        .mapEmpty();
  }
}
