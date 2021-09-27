package org.folio.inventoryupdate;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;


/**
 *
 * @author ne
 */
public class SharedInstanceByLocalIdentifierQuery implements InventoryQuery {

  private final String localIdentifier;
  private final String identifierTypeId;
  public final String queryString;

  public SharedInstanceByLocalIdentifierQuery(String localIdentifier, String identifierTypeId) {
    this.localIdentifier = localIdentifier;
    this.identifierTypeId = identifierTypeId;
    queryString = buildQuery();
  }

  private String buildQuery() {
    StringBuilder query = new StringBuilder();
    query.append("(identifiers =/@value/@identifierTypeId=")
          .append("\"").append(identifierTypeId).append("\"")
          .append(" \"").append(localIdentifier).append("\"")
          .append(")");
    return query.toString();
  }

  @Override
  public String getQueryString() {
    return queryString;
  }

  @Override
  public String getURLEncodedQueryString() {
    return URLEncoder.encode(queryString, StandardCharsets.UTF_8);
  }

}
