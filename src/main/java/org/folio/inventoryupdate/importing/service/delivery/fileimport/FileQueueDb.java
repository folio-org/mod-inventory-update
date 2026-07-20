package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.templates.SqlTemplate;
import io.vertx.sqlclient.templates.TupleMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.inventoryupdate.importing.utils.SettableClock;
import org.folio.tlib.postgres.TenantPgPool;

public final class FileQueueDb implements FileQueue {

  private static final String DATE_FORMAT = "YYYY-MM-DD''T''HH24:MI:SS,MS";
  private final UUID channelId;
  private final TenantPgPool pool;

  private FileQueueDb(ServiceRequest request, UUID channelId) {
    pool = request.entityStorage().getTenantPool();
    this.channelId = channelId;
  }

  public static FileQueueDb get(ServiceRequest request, UUID channelId) {
    return new FileQueueDb(request, channelId);
  }

  @Override
  public Future<String> initialize(boolean retainFilesIfAny) {
    if (retainFilesIfAny) {
      return Future.succeededFuture("File queue ready.");
    } else {
      return SqlTemplate.forUpdate(pool.getPool(),
          "DELETE FROM " + pool.getSchema() + "." + Tables.SOURCE_FILE
              + " WHERE channel_id = #{channelId}")
          .execute(Collections.singletonMap("channelId", channelId))
          .compose(x -> Future.succeededFuture("Cleared file queue"))
          .map("Cleared file queue");
    }
  }

  @Override
  public Future<Void> push(String fileName, String payload) {
    return SqlTemplate.forUpdate(pool.getPool(),
            "INSERT INTO " + pool.getSchema() + "." + Tables.SOURCE_FILE
                + " ( id, file_name, channel_id, uploaded_date, payload ) "
                + " VALUES ( #{id}, #{fileName}, #{channelId}, "
                + "  TO_TIMESTAMP(#{timeStamp},'" + DATE_FORMAT + "'), #{payload} ) "
                + " ON CONFLICT (file_name, channel_id) DO UPDATE "
                + " SET uploaded_date = TO_TIMESTAMP(#{timeStamp},'" + DATE_FORMAT + "'), "
                + "     payload = #{payload} ")
        .mapFrom(TupleMapper.mapper(
            fileData -> {
              Map<String, Object> parameters = new HashMap<>();
              parameters.put("id", UUID.randomUUID().toString());
              parameters.put("fileName", fileName);
              parameters.put("timeStamp", SettableClock.getLocalDateTime().toString());
              parameters.put("channelId", this.channelId);
              parameters.put("payload", payload);
              return parameters;
            }))
        .execute(payload)
        .mapEmpty();
  }

  @Override
  public Future<Boolean> hasFileInProcess() {
    Map<String, Object> params = new HashMap<>();
    params.put("channelId", channelId);
    return SqlTemplate.forQuery(pool.getPool(),
            "SELECT 1 "
                + " FROM " + pool.getSchema() + "." + Tables.SOURCE_FILE
                + " WHERE channel_id = #{channelId} "
                + "   AND processing = 1 ")
        .execute(params)
        .map(rows -> rows.rowCount() == 1);
  }

  @Override
  public Future<Boolean> isEmpty() {
    Map<String, Object> params = new HashMap<>();
    params.put("channelId", channelId);
    return SqlTemplate.forQuery(pool.getPool(),
            "SELECT 1 "
                + " FROM " + pool.getSchema() + "." + Tables.SOURCE_FILE
                + " WHERE channel_id = #{channelId} "
                + "   AND processing IS NULL ")
        .execute(params)
        .map(rows -> rows.rowCount() == 0);
  }

  @Override
  public Future<Integer> size() {
    Map<String, Object> params = new HashMap<>();
    params.put("channelId", channelId);
    return SqlTemplate.forQuery(pool.getPool(),
            "SELECT COUNT(id) AS queue_size "
                + " FROM " + pool.getSchema() + "." + Tables.SOURCE_FILE
                + " WHERE channel_id = #{channelId} ")
        .execute(params)
        .map(rows -> rows.iterator().next().getInteger("queue_size"));
  }

  @Override
  public Future<String> nameOfFileInProcess() {
    Map<String, Object> params = new HashMap<>();
    params.put("channelId", channelId);
    return SqlTemplate.forQuery(pool.getPool(),
        "SELECT file_name "
            + " FROM " + pool.getSchema() + "." + Tables.SOURCE_FILE
            + " WHERE channel_id = #{channelId} "
            + "   AND processing = 1")
        .execute(params)
        .compose(res -> {
          if (res.iterator().hasNext()) {
            return Future.succeededFuture(res.iterator().next().getString("file_name"));
          } else {
            return Future.succeededFuture("no file in process");
          }
        });
  }

  @Override
  public Future<SourceFile> currentlyPromotedFile() {
    Map<String, Object> params = new HashMap<>();
    params.put("channelId", channelId);
    return SqlTemplate.forQuery(pool.getPool(),
            "SELECT file_name, payload "
                + " FROM " + pool.getSchema() + "." + Tables.SOURCE_FILE
                + " WHERE channel_id = #{channelId} "
                + "   AND processing = 1 ")
        .execute(params)
        .compose(res -> {
          if (res.iterator().hasNext()) {
            Row row = res.stream().iterator().next();
            SourceFile sf = new SourceFileDb(row.getString("file_name"), row.getString("payload"),
                channelId, pool);
            return Future.succeededFuture(sf);
          } else {
            return Future.succeededFuture(null);
          }
        });
  }

  @Override
  public Future<SourceFile> promoteAndGetNextFileIfPossible() {
    return hasFileInProcess()
        .compose(alreadyHasFileInProcess -> {
          if (alreadyHasFileInProcess) {
            return Future.succeededFuture(null);
          } else {
            Map<String, Object> params = new HashMap<>();
            params.put("channelId", channelId);
            return SqlTemplate.forUpdate(pool.getPool(),
                "UPDATE " + pool.getSchema() + "." + Tables.SOURCE_FILE
                    + " SET processing = 1 "
                    + " WHERE id IN (SELECT id "
                    + "              FROM " + pool.getSchema() + "." + Tables.SOURCE_FILE
                    + "              WHERE channel_id = #{channelId}"
                    + "                AND processing IS NULL "
                    + "              ORDER BY uploaded_date LIMIT 1 )")
                .execute(params)
                .compose(na -> SqlTemplate.forQuery(pool.getPool(),
                    "SELECT file_name, payload "
                        + " FROM " + pool.getSchema() + "." + Tables.SOURCE_FILE
                        + " WHERE channel_id = #{channelId} "
                        + "   AND processing = 1 ")
                    .execute(params)
                    .compose(res -> {
                      Row row = res.iterator().next();
                      return Future.succeededFuture(new SourceFileDb(
                          row.getString("file_name"),
                          row.getString("payload"),
                          channelId,
                          pool));
                    }));
          }
        });
  }
}
