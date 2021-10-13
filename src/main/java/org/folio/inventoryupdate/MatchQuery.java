package org.folio.inventoryupdate;

/**
 * MatchQuery takes an Instance and constructs a Inventory query needed for
 * determining if a similar Instance already exists in Inventory
 *
 */
public class MatchQuery extends InventoryQuery {

  private final String matchKey;

  public MatchQuery(String matchKey) {
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
