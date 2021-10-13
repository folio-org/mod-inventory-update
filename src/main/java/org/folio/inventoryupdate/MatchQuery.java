package org.folio.inventoryupdate;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

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
    StringBuilder query = new StringBuilder();
    // Get match properties from request
    query.append("(matchKey==\"").append(matchKey).append("\")");
    return query.toString();
  }

}
