package org.folio.inventoryupdate.importing.moduledata.database;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.Entity;
import org.folio.inventoryupdate.importing.moduledata.ImportConfig;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.moduledata.LogLine;
import org.folio.inventoryupdate.importing.moduledata.RecordFailure;
import org.folio.inventoryupdate.importing.moduledata.Step;
import org.folio.inventoryupdate.importing.moduledata.Transformation;
import org.folio.inventoryupdate.importing.moduledata.TransformationStep;
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
            .compose(na -> pool.query(createJobLogsView(pool.getSchema())).execute()).mapEmpty();


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
     * Creates custom views.
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
                + "  FROM " + schema + "." + Tables.record_failure + " AS rf, "
                + "       " + schema + "." + Tables.import_job + " as ij "
                + "  WHERE rf.import_job_id = ij.id";
        return ddl;
    }

    public static String createJobLogsView(String schema) {
        String ddl;
        ddl = "CREATE OR REPLACE VIEW " + schema + "." + Tables.job_log_view
            + " AS SELECT ls.id AS id, "
            + "          ls.import_job_Id AS import_job_id, "
            + "          ij.import_config_id AS import_config_id, "
            + "          ij.import_config_name AS import_config_name, "
            + "          ls.time_stamp AS time_stamp, "
            + "          ls.job_label AS job_label, "
            + "          ls.statement AS statement "
            + "  FROM " + schema + "." + Tables.log_statement + " AS ls, "
            + "       " + schema + "." + Tables.import_job + " as ij "
            + "  WHERE ls.import_job_id = ij.id";
        return ddl;
    }

}
