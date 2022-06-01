package org.folio.inventoryupdate;

/**
 * MatchQuery takes an Instance and constructs a Inventory query needed for
 * determining if a similar Instance already exists in Inventory
 *
 */
public class QueryMatchKey extends InventoryQuery {

  private final String matchKey;

  public QueryMatchKey(String matchKey) {
    this.matchKey = matchKey;
    queryString = buildMatchQuery();
  }

  /**
   * Builds a match query from properties of incoming Instance
   * @return match query string
   */
  private String buildMatchQuery() {
    return "(matchKey==\"" + matchKey + "\")";
  }

}
