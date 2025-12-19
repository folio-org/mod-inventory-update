package org.folio.inventoryupdate.updating;


public class QueryShiftingMatchKey extends InventoryQuery
{
    String matchKey;
    String identifierTypeId;
    String localIdentifier;
    public String localIdentifier () {
      return localIdentifier;
    }

    public QueryShiftingMatchKey(String localIdentifier, String identifierTypeId, String matchKey) {
        this.identifierTypeId = identifierTypeId;
        this.localIdentifier = localIdentifier;
        this.matchKey = matchKey;
        this.queryString = buildQuery();
    }

    public String buildQuery () {
        return "(identifiers =/@value/@identifierTypeId="
                + "\"" + identifierTypeId + "\""
                + " \"" + localIdentifier + "\""
                + " not matchKey==\"" + matchKey + "\")";
    }

}
