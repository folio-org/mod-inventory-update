package org.folio.inventoryupdate;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class ShiftingMatchKeyQuery extends InventoryQuery
{

    String matchKey;
    String identifierTypeId;
    String localIdentifier;

    public ShiftingMatchKeyQuery( String localIdentifier, String identifierTypeId, String matchKey) {
        this.identifierTypeId = identifierTypeId;
        this.localIdentifier = localIdentifier;
        this.matchKey = matchKey;
        this.queryString = buildQuery();
    }

    public String buildQuery () {
        String query = "(identifiers =/@value/@identifierTypeId=" + "\"" + identifierTypeId + "\"" + " \"" + localIdentifier + "\"" + " not matchKey==" + matchKey + ")";
        return query;
    }

}
