package org.folio.inventoryupdate;

import java.util.UUID;

public class QueryByUUID extends InventoryQuery
{
    public QueryByUUID ( UUID uuid) {
        queryString = "(id==\"" + uuid.toString() + "\")";
    }

    public QueryByUUID ( String uuid) {
        queryString = "(id==\"" + uuid + "\")";
    }
}
