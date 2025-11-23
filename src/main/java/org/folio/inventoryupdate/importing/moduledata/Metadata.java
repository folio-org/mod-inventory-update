package org.folio.inventoryupdate.importing.moduledata;

import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.folio.inventoryupdate.importing.moduledata.Entity.DATE_FORMAT;

public class Metadata {
    String createdDate;
    UUID createdByUser;
    String updatedDate;
    UUID updatedByUser;

    public Metadata () {

    }

    public Metadata withUpdatedDate(String date) {
      this.updatedDate = date;
      return this;
    }

    public Metadata withUpdatedByUser (UUID user) {
      this.updatedByUser = user;
      return this;
    }

    public Metadata withCreatedDate (String date) {
      this.createdDate = date;
      return this;
    }

    public Metadata withCreatedByUser (UUID user) {
      this.createdByUser = user;
      return this;
    }

    public Metadata fromRow(Row row) {
      return new Metadata()
          .withCreatedDate(ifDateSet(row.getLocalDateTime("created_date")))
          .withCreatedByUser(row.getUUID("created_by_user"))
          .withUpdatedDate(ifDateSet(row.getLocalDateTime("updated_date")))
          .withUpdatedByUser(row.getUUID("updated_by_user"));
    }

    private String ifDateSet(LocalDateTime date) {
    return date == null ? null : date.toString();
  }

    public JsonObject asJson () {
      JsonObject metadata = new JsonObject();
        if (createdDate != null) metadata.put("createdDate", createdDate);
        if (createdByUser != null) metadata.put("createdByUser", createdByUser);
        if (updatedDate != null) metadata.put("updatedDate", updatedDate);
        if (updatedByUser != null) metadata.put("updatedByUser", updatedByUser);
      return metadata;
    }

    public Map<String, Object> asTemplateParameters() {
      Map<String, Object> parameters = new HashMap<>();
      parameters.put("created_date", createdDate);
      parameters.put("created_by_user", createdByUser);
      parameters.put("updated_date", updatedDate);
      parameters.put("updated_by_user", updatedByUser);
      return parameters;
    }

    public String columnsDdl() {
      return
          "created_date TIMESTAMP, " +
          "created_by_user UUID, " +
          "updated_date TIMESTAMP, " +
          "updated_by_user UUID ";
    }


  public String insertClauseColumns() {
    return "created_date" + "," + "created_by_user";
  }

  public String insertClauseValueTemplates() {
    return "TO_TIMESTAMP(#{created_date},'" + DATE_FORMAT + "')," + "#{created_by_user} ";
  }

  public String updateClauseColumnTemplates() {
     return "updated_date = TO_TIMESTAMP(#{updated_date}, '" + DATE_FORMAT + "')," + "updated_by_user = #{updated_by_user} ";
  }
}
