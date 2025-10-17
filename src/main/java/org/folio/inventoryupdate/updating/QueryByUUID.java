package org.folio.inventoryupdate.updating;

import java.util.UUID;

public class QueryByUUID extends InventoryQuery
{
    private final String uuid;

    public QueryByUUID (UUID uuid) {
        this(uuid.toString());
    }

    public QueryByUUID (String uuid) {
        this.uuid = uuid;
        queryString = "(id==\"" + uuid + "\")";
    }

    public String getUuid() {
      return uuid;
    }
}
