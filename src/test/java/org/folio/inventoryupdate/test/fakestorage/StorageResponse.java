package org.folio.inventoryupdate.test.fakestorage;

public class StorageResponse
{
    public int statusCode;
    public String responseBody;

    public StorageResponse (int statusCode, String responseBody) {
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }
}
