package org.folio.inventoryimport.moduledata.database;


import org.apache.commons.lang3.StringUtils;

public class SqlQuery {

  private final String select;
  private final String from;
  private String where;
  private String orderBy;
  private final String offset;
  private final String limit;
  private String defaultLimit = null;

  /**
   * Constructor.
   */
  public SqlQuery(String select, String from, String where, String orderBy,
                  String offset, String limit) {
    this.select = select;
    this.from = from;
    this.where = where != null ? where : "";
    this.orderBy = orderBy != null ? orderBy : "";
    this.offset = offset;
    this.limit = limit;
  }

  /**
   * Gets count query.
   */
  public String getCountingSql() {
    return "SELECT COUNT(*) as total_records "
        + from
        + (where.isEmpty() ? "" : " WHERE " + where);
  }

  /**
   * Gets SQL with limits applied.
   */
  public String getQueryWithLimits() {
    return select
        + from
        + (where.isEmpty() ? "" : " WHERE " + where)
        + (orderBy.isEmpty() ? "" : " ORDER BY " + orderBy)
        + keywordLong(" offset ", offset)
        + keywordLong(" limit ", (limit == null ? defaultLimit : limit));
  }

  /**
   * Adds ANDed where clause to query.
   */
  public SqlQuery withAdditionalWhereClause(String clause) {
    if (clause != null && !clause.isEmpty()) {
      if (where.isEmpty()) {
        where = " (" + clause + ")";
      } else {
        where += " AND " + "(" + clause + ") ";
      }
    }
    return this;
  }

  public SqlQuery withAdditionalOrderByField(String clause) {
    if (clause != null && !clause.isEmpty()) {
      if (orderBy.isEmpty()) {
        orderBy = clause;
      } else {
        orderBy += ", " + clause;
      }
    }
    return this;
  }

  public SqlQuery withDefaultLimit(String defaultLimit) {
    this.defaultLimit = defaultLimit;
    return this;
  }

  private static String keywordLong(String keyword, String number) {
    var stripped = StringUtils.stripToEmpty(number);
    if (stripped.isEmpty()) {
      return "";
    }
    // prevent SQL injection by parsing the number
    return keyword + Long.parseLong(stripped);
  }

  public String toString() {
    return getQueryWithLimits();
  }
}
