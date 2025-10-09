package org.folio.inventoryimport.moduledata.database;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryimport.moduledata.*;
import org.folio.tlib.postgres.TenantPgPool;

public class DatabaseInit {

    static final Logger logger = LogManager.getLogger(DatabaseInit.class);


    /**
     * Creates tables and views.
     */
    public static Future<Void> createDatabase(TenantPgPool pool) {
        return create(new Step(), pool)
                .compose(na -> create(new Transformation(), pool))
                .compose(na -> create(new ImportConfig(), pool))
                .compose(na -> create(new ImportJob(), pool))
                .compose(na -> create(new RecordFailure(), pool))
                .compose(na -> create(new LogLine(), pool))
                .compose(na -> create(new TransformationStep(), pool))
                .compose(na -> pool.query(createRecordFailureView(pool.getSchema())).execute())
                .mapEmpty();


        /* Template for processing parameters in init.
          JsonArray parameters = tenantAttributes.getJsonArray("parameters");
          if (parameters != null) {
            for (int i = 0; i < parameters.size(); i++) {
              JsonObject parameter = parameters.getJsonObject(i);
              if ("loadSample".equals(parameter.getString("key"))
                  && "true".equals(parameter.getString("value"))) {
              }
            }
          }
        */
    }

    /**
     * Creates database objects for an entity
     * @param entity the domain object to persist
     * @param pool the tenant specific Postgres pool.
     */
    public static Future<Void> create(Entity entity, TenantPgPool pool) {
        return entity.createDatabase(pool)
                .onFailure(e -> logger.error("Error creating table [" + entity.table() + "] or related objects: " +  e.getMessage()));
    }

    /**
     * Creates specific view.
     */
    public static String createRecordFailureView(String schema) {
        String ddl;
        ddl = "CREATE OR REPLACE VIEW " + schema + "." + Tables.record_failure_view
                + " AS SELECT rf.id AS id, "
                + "          rf.import_job_Id AS import_job_id, "
                + "          ij.import_config_id AS import_config_id, "
                + "          ij.import_config_name AS import_config_name, "
                + "          rf.record_number AS record_number, "
                + "          rf.time_stamp AS time_stamp, "
                + "          rf.record_errors AS record_errors, "
                + "          rf.original_record AS original_record, "
                + "          rf.transformed_record AS transformed_record "
                + "  FROM " + schema + ".record_failure AS rf, "
                + "       " + schema + ".import_job as ij "
                + "  WHERE rf.import_job_id = ij.id";
        return ddl;
    }

}
