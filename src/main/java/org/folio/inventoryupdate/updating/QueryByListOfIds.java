package org.folio.inventoryupdate.updating;

import java.util.Iterator;
import java.util.List;

public class QueryByListOfIds extends InventoryQuery {
  public final String queryField;
  public final List<String> ids;

  public QueryByListOfIds (String queryField, List<String> ids) {
    this.queryField = queryField;
    this.ids = ids;
    queryString = buildQueryByListOfIDs();
  }

  private String buildQueryByListOfIDs() {
    // Get match properties from request
    StringBuilder query = new StringBuilder();
    query.append("(")
            .append(queryField)
            .append("==(");
    Iterator<String> items = ids.iterator();
    while (items.hasNext()) {
      query.append("\"")
              .append(items.next())
              .append("\"")
              .append(items.hasNext() ? " OR " : "");
    }
    query.append("))");
    return query.toString();

  }
}
