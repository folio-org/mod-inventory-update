package org.folio.inventoryupdate;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;


/**
 *
 * @author ne
 */
public class SharedInstanceByLocalIdentifierQuery extends InventoryQuery {

  private final String localIdentifier;
  private final String identifierTypeId;

  public SharedInstanceByLocalIdentifierQuery(String localIdentifier, String identifierTypeId) {
    this.localIdentifier = localIdentifier;
    this.identifierTypeId = identifierTypeId;
    queryString = buildQuery();
  }

  protected String buildQuery() {
    String query = "(identifiers =/@value/@identifierTypeId=" + "\"" + identifierTypeId + "\"" + " \"" + localIdentifier + "\"" + ")";
    return query;
  }
}
