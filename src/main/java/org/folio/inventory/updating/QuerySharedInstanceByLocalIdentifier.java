package org.folio.inventory.updating;

public class QuerySharedInstanceByLocalIdentifier extends InventoryQuery {

  private final String localIdentifier;
  private final String identifierTypeId;

  public QuerySharedInstanceByLocalIdentifier(String localIdentifier, String identifierTypeId) {
    this.localIdentifier = localIdentifier;
    this.identifierTypeId = identifierTypeId;
    queryString = buildQuery();
  }

  protected String buildQuery() {
    return "(identifiers =/@value/@identifierTypeId=" + "\"" + identifierTypeId + "\"" + " \"" + localIdentifier + "\"" + ")";
  }
}
