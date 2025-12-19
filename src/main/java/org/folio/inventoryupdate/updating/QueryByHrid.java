package org.folio.inventoryupdate.updating;

public class QueryByHrid extends InventoryQuery {
    public final String hrid;

    public QueryByHrid(String hrid) {
        this.hrid = hrid;
        queryString = buildHridQuery();
    }

  private String buildHridQuery() {
      // Get match properties from request
      return "(hrid==\"" + hrid + "\")";
  }

}
