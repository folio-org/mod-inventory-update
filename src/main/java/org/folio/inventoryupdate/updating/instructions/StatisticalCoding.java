package org.folio.inventoryupdate.updating.instructions;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.updating.entities.InventoryRecord;

public class StatisticalCoding {

    public static final String STATISTICAL_CODING = "statisticalCoding";
    private static final String DELETE_SKIPPED_BECAUSE_OF = "becauseOf";
    private static final String SET_CODE_UUID = "setCode";

    public JsonArray codings = new JsonArray();
    StatisticalCoding(JsonObject processing) {
      if (processing != null) {
        codings = processing.getJsonArray(STATISTICAL_CODING, new JsonArray());
      }
    }

    public String getStatisticalCodeId(InventoryRecord.DeletionConstraint constraint) {
      for (Object entityEventCode : codings) {
        JsonObject i = (JsonObject) entityEventCode;
        if (i.getString("if").equalsIgnoreCase("deleteSkipped") && i.getString(DELETE_SKIPPED_BECAUSE_OF).equalsIgnoreCase(constraint.toString())) {
          return  i.getString(SET_CODE_UUID);
        }
      }
      return "";
    }

}

