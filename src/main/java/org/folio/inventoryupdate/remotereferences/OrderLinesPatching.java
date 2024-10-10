package org.folio.inventoryupdate.remotereferences;

import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.Item;
import org.folio.inventoryupdate.entities.Repository;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.okapi.common.OkapiClient;

import java.util.ArrayList;
import java.util.List;


public class OrderLinesPatching {

  private static final Logger logger = LoggerFactory.getLogger("inventory-update");

  public OrderLinesPatching() {
    // Noop
  }

  public static Future<Void> processPoLineReferences (OkapiClient okapiClient, Repository repository) {
    List<Future<Void>> orderLinePatchingFutures = new ArrayList<>();
    for (Item item : repository.getItemsToUpdate()) {
      if (item.isSwitchingInstance() && item.getPurchaseOrderLineIdentifier() != null) {
        logger.info("Switching PO line '" + item.getPurchaseOrderLineIdentifier() + "' to instance '" + item.getNewInstanceId() + "'");
        JsonObject patchBody = new JsonObject()
            .put("operation", "Replace Instance Ref")
            .put("replaceInstanceRef",
                new JsonObject()
                    .put("holdingsOperation", "Move")
                    .put("newInstanceId", item.getNewInstanceId()));
        orderLinePatchingFutures.add(Orders.patchOrderLine(okapiClient, item.getPurchaseOrderLineIdentifier(), patchBody));
      }
    }
    return GenericCompositeFuture.join(orderLinePatchingFutures).mapEmpty();
  }
}
