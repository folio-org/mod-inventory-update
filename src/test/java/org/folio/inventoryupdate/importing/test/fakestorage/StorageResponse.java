package org.folio.inventoryupdate.importing.test.fakestorage;

public class StorageResponse
{
    public final int statusCode;
    public final String responseBody;

    public StorageResponse (int statusCode, String responseBody) {
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }
}
