package org.folio.inventoryupdate.updating.entities;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public abstract class JsonRepresentation {

    protected JsonObject sourceJson = null;

    public abstract JsonObject asJson();
    public abstract JsonArray getErrors ();

    /**
     * Creates a deep clone of a JSONArray from a JSONObject, removes the array from the source object and returns the clone
     * @param jsonObject Source object containing the array to extract
     * @param arrayName Property name of the array to extract
     * @return  The extracted JsonArray or an empty JsonArray if none found to extract.
     */
    protected static JsonArray extractJsonArrayFromObject(JsonObject jsonObject, String arrayName)  {
        JsonArray array = new JsonArray();
        if (jsonObject.containsKey(arrayName)) {
            array = new JsonArray((jsonObject.getJsonArray(arrayName)).encode());
            jsonObject.remove(arrayName);
        }
        return array;
    }

}
