package org.folio.inventoryupdate;

public class SharedInstanceByLocalIdentifierQuery extends InventoryQuery {

  private final String localIdentifier;
  private final String identifierTypeId;

  public SharedInstanceByLocalIdentifierQuery(String localIdentifier, String identifierTypeId) {
    this.localIdentifier = localIdentifier;
    this.identifierTypeId = identifierTypeId;
    queryString = buildQuery();
  }

  protected String buildQuery() {
    return "(identifiers =/@value/@identifierTypeId=" + "\"" + identifierTypeId + "\"" + " \"" + localIdentifier + "\"" + ")";
  }
}
