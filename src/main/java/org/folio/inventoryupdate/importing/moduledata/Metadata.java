package org.folio.inventoryupdate.importing.moduledata;

import static org.folio.inventoryupdate.importing.moduledata.Entity.DATE_FORMAT_TO_DB;
import static org.folio.inventoryupdate.importing.utils.DateTimeFormatter.formatDateTime;

import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Metadata {

  public static final String METADATA_PROPERTY = "metadata";
  private static final String CREATED_BY_USER_ID_PROP = "createdByUserId";
  private static final String UPDATED_BY_USER_ID_PROP = "updatedByUserId";
  private static final String CREATED_BY_USER_ID_COL = "created_by_user_id";
  private static final String UPDATED_BY_USER_ID_COL = "updated_by_user_id";
  private static final String CREATED_DATE_PROP = "createdDate";
  private static final String CREATED_DATE_COL = "created_date";
  private static final String UPDATED_DATE_PROP = "updatedDate";
  private static final String UPDATED_DATE_COL = "updated_date";
  String createdDate;
  UUID createdByUserId;
  String updatedDate;
  UUID updatedByUserId;

  public Metadata() {

  }

  public Metadata withUpdatedDate(String date) {
    this.updatedDate = date;
    return this;
  }

  public Metadata withUpdatedByUserId(UUID user) {
    this.updatedByUserId = user;
    return this;
  }

  public Metadata withCreatedDate(String date) {
    this.createdDate = date;
    return this;
  }

  public Metadata withCreatedByUserId(UUID user) {
    this.createdByUserId = user;
    return this;
  }

  public Metadata fromRow(Row row) {
    return new Metadata()
        .withCreatedDate(ifDateSet(row.getLocalDateTime(CREATED_DATE_COL)))
        .withCreatedByUserId(row.getUUID(CREATED_BY_USER_ID_COL))
        .withUpdatedDate(ifDateSet(row.getLocalDateTime(UPDATED_DATE_COL)))
        .withUpdatedByUserId(row.getUUID(UPDATED_BY_USER_ID_COL));
  }

  private String ifDateSet(LocalDateTime date) {
    return date == null ? null : formatDateTime(date);
  }

  public JsonObject asJson() {
    JsonObject metadata = new JsonObject();
    if (createdDate != null) {
      metadata.put(CREATED_DATE_PROP, createdDate);
    }
    if (createdByUserId != null) {
      metadata.put(CREATED_BY_USER_ID_PROP, createdByUserId);
    }
    if (updatedDate != null) {
      metadata.put(UPDATED_DATE_PROP, updatedDate);
    }
    if (updatedByUserId != null) {
      metadata.put(UPDATED_BY_USER_ID_PROP, updatedByUserId);
    }
    return metadata;
  }

  public Map<String, Object> asTemplateParameters() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(CREATED_DATE_COL, createdDate);
    parameters.put(CREATED_BY_USER_ID_COL, createdByUserId);
    parameters.put(UPDATED_DATE_COL, updatedDate);
    parameters.put(UPDATED_BY_USER_ID_COL, updatedByUserId);
    return parameters;
  }

  public String columnsDdl() {
    return
        CREATED_DATE_COL + " TIMESTAMP, "
            + CREATED_BY_USER_ID_COL + " UUID, "
            + UPDATED_DATE_COL + " TIMESTAMP, "
            + UPDATED_BY_USER_ID_COL + " UUID ";
  }

  public String insertClauseColumns() {
    return CREATED_DATE_COL + ","
        + CREATED_BY_USER_ID_COL;
  }

  public String insertClauseValueTemplates() {
    return "TO_TIMESTAMP(#{" + CREATED_DATE_COL + "},'" + DATE_FORMAT_TO_DB + "'),"
        + "#{" + CREATED_BY_USER_ID_COL + "} ";
  }

  public String updateClauseColumnTemplates() {
    return UPDATED_DATE_COL + " = TO_TIMESTAMP(#{" + UPDATED_DATE_COL + "}, '" + DATE_FORMAT_TO_DB + "'),"
        + UPDATED_BY_USER_ID_COL + " = #{" + UPDATED_BY_USER_ID_COL + "} ";
  }
}
